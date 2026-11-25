pub mod block;
pub mod bloom;
pub mod merge;
pub mod sstable;

use block::{BlockCache, BlockCacheStats, DEFAULT_BLOCK_CACHE_CAPACITY};
use common::DbError;
use merge::KWayMergeIterator;
use sstable::{EntryValue, SsTableReader};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use wal::{DEFAULT_WAL_SEGMENT_BYTES, WalManager};

pub type KvPair = (Vec<u8>, Vec<u8>);

pub const DEFAULT_MEMTABLE_MAX_BYTES: usize = 256 * 1024;
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;
const MAX_LEVELS: usize = 5;
const L1_MAX_BYTES: u64 = 1024 * 1024; // 1 MB base for L1, 10x per subsequent level
const TARGET_SST_BYTES: usize = 256 * 1024; // Split L1+ runs into ~256KB non-overlapping SSTables

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsmStats {
    pub memtable_bytes: usize,
    pub level_file_counts: Vec<usize>,
    pub cache_stats: BlockCacheStats,
}

struct LsmInner {
    memtable: BTreeMap<Vec<u8>, EntryValue>,
    memtable_bytes: usize,
    /// `levels[0]` holds L0 SSTables (may overlap, ordered oldest -> newest).
    /// `levels[1..]` hold L1+ SSTables (strictly disjoint key ranges, sorted by `min_key`).
    levels: Vec<Vec<SsTableReader>>,
    next_sst_id: u64,
}

/// Production-style LSM-Tree storage engine featuring:
/// - Prefix-compressed 4KB Data Blocks
/// - LRU Block Cache
/// - K-way Merge Iterator
/// - Multi-level Compaction (L0 -> L1 -> L2 ...)
pub struct LsmEngine {
    dir: PathBuf,
    memtable_max_bytes: usize,
    l0_trigger: usize,
    l1_max_bytes: u64,
    inner: RwLock<LsmInner>,
    block_cache: Mutex<BlockCache>,
    wal: Mutex<WalManager>,
    writer: Mutex<()>,
}

impl LsmEngine {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(
            dir,
            DEFAULT_MEMTABLE_MAX_BYTES,
            DEFAULT_L0_COMPACTION_TRIGGER,
            DEFAULT_WAL_SEGMENT_BYTES,
        )
    }

    pub fn open_with_options(
        dir: impl AsRef<Path>,
        memtable_max_bytes: usize,
        l0_trigger: usize,
        wal_segment_bytes: u64,
    ) -> Result<Self, DbError> {
        Self::open_with_tuning(
            dir,
            memtable_max_bytes,
            l0_trigger,
            L1_MAX_BYTES,
            DEFAULT_BLOCK_CACHE_CAPACITY,
            wal_segment_bytes,
        )
    }

    pub fn open_with_tuning(
        dir: impl AsRef<Path>,
        memtable_max_bytes: usize,
        l0_trigger: usize,
        l1_max_bytes: u64,
        block_cache_capacity: usize,
        wal_segment_bytes: u64,
    ) -> Result<Self, DbError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut levels: Vec<Vec<SsTableReader>> = (0..MAX_LEVELS).map(|_| Vec::new()).collect();
        let mut max_id = 0u64;

        let mut found = list_sst_files(&dir)?;
        found.sort_by_key(|(level, id)| (*level, *id));

        for (level, id) in found {
            if level >= MAX_LEVELS {
                continue;
            }
            let reader = SsTableReader::open(sst_path(&dir, level, id), id, level)?;
            max_id = max_id.max(id);
            levels[level].push(reader);
        }

        // Ensure L1+ tables are sorted by min_key ascending
        for lvl in levels.iter_mut().skip(1) {
            lvl.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        let (wal_mgr, records) = WalManager::open(dir.join("wal"), wal_segment_bytes)?;
        let mut memtable = BTreeMap::new();
        let mut memtable_bytes = 0usize;

        for rec in records {
            let (key, val) = decode_wal_mutation(&rec.payload)?;
            memtable_bytes += key.len() + entry_size(&val);
            memtable.insert(key, val);
        }

        Ok(Self {
            dir,
            memtable_max_bytes: memtable_max_bytes.max(512),
            l0_trigger: l0_trigger.max(2),
            l1_max_bytes: l1_max_bytes.max(2048),
            inner: RwLock::new(LsmInner {
                memtable,
                memtable_bytes,
                levels,
                next_sst_id: max_id + 1,
            }),
            block_cache: Mutex::new(BlockCache::new(block_cache_capacity)),
            wal: Mutex::new(wal_mgr),
            writer: Mutex::new(()),
        })
    }

    pub fn stats(&self) -> Result<LsmStats, DbError> {
        let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
        let cache_stats = self
            .block_cache
            .lock()
            .map_err(|_| DbError::LockPoisoned)?
            .stats();
        Ok(LsmStats {
            memtable_bytes: guard.memtable_bytes,
            level_file_counts: guard.levels.iter().map(Vec::len).collect(),
            cache_stats,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let val = EntryValue::Put(value.to_vec());
        let payload = encode_wal_mutation(key, &val);

        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            wal.append(payload)?;
        }

        let should_flush = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.memtable_bytes += key.len() + entry_size(&val);
            guard.memtable.insert(key.to_vec(), val);
            guard.memtable_bytes >= self.memtable_max_bytes
        };

        if should_flush {
            self.flush_memtable_locked()?;
        }
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let val = EntryValue::Delete;
        let payload = encode_wal_mutation(key, &val);

        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            wal.append(payload)?;
        }

        let should_flush = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.memtable_bytes += key.len() + 1;
            guard.memtable.insert(key.to_vec(), val);
            guard.memtable_bytes >= self.memtable_max_bytes
        };

        if should_flush {
            self.flush_memtable_locked()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;

        // 1. MemTable (newest)
        if let Some(entry) = guard.memtable.get(key) {
            return Ok(match entry {
                EntryValue::Put(v) => Some(v.clone()),
                EntryValue::Delete => None,
            });
        }

        // 2. L0 SSTables (newest -> oldest, overlapping ranges allowed)
        for sst in guard.levels[0].iter().rev() {
            if let Some(entry) = sst.get(key, &self.block_cache)? {
                return Ok(match entry {
                    EntryValue::Put(v) => Some(v),
                    EntryValue::Delete => None,
                });
            }
        }

        // 3. L1+ SSTables (disjoint ranges, binary search by key range)
        for level in guard.levels.iter().skip(1) {
            if level.is_empty() {
                continue;
            }
            let candidate_idx = match level.binary_search_by(|s| s.min_key.as_slice().cmp(key)) {
                Ok(i) => Some(i),
                Err(0) => None,
                Err(i) => Some(i - 1),
            };
            if let Some(idx) = candidate_idx
                && let Some(entry) = level[idx].get(key, &self.block_cache)?
            {
                return Ok(match entry {
                    EntryValue::Put(v) => Some(v),
                    EntryValue::Delete => None,
                });
            }
        }

        Ok(None)
    }

    /// Scans all live key-value pairs starting with `prefix` via K-way Merge Iterator.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>, DbError> {
        let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;

        // Build ranked sources from oldest -> newest: L(max)..L1 -> L0 (oldest..newest) -> MemTable
        let mut ranked_sources: Vec<Vec<(Vec<u8>, EntryValue)>> = Vec::new();

        for level in guard.levels.iter().skip(1).rev() {
            let mut level_entries = Vec::new();
            for sst in level {
                if !prefix.is_empty() && sst.max_key.as_slice() < prefix {
                    continue;
                }
                for (k, v) in sst.read_all()? {
                    if k.starts_with(prefix) {
                        level_entries.push((k, v));
                    }
                }
            }
            if !level_entries.is_empty() {
                ranked_sources.push(level_entries);
            }
        }

        for sst in &guard.levels[0] {
            let mut entries = Vec::new();
            for (k, v) in sst.read_all()? {
                if k.starts_with(prefix) {
                    entries.push((k, v));
                }
            }
            if !entries.is_empty() {
                ranked_sources.push(entries);
            }
        }

        let mem_entries: Vec<(Vec<u8>, EntryValue)> = guard
            .memtable
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !mem_entries.is_empty() {
            ranked_sources.push(mem_entries);
        }

        let merge_iter = KWayMergeIterator::new(ranked_sources);
        let mut out = Vec::new();
        for (k, v) in merge_iter {
            if let EntryValue::Put(bytes) = v {
                out.push((k, bytes));
            }
        }
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        self.flush_memtable_locked()
    }

    /// Triggers Leveled Compaction across all levels until every level satisfies size/count budgets.
    pub fn compact(&self) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        self.run_leveled_compaction_locked()
    }

    fn flush_memtable_locked(&self) -> Result<(), DbError> {
        let (entries, sst_id) = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            if guard.memtable.is_empty() {
                return Ok(());
            }
            let id = guard.next_sst_id;
            guard.next_sst_id += 1;
            let entries = std::mem::take(&mut guard.memtable);
            guard.memtable_bytes = 0;
            (entries, id)
        };

        if let Some(reader) =
            SsTableReader::write_from_sorted(sst_path(&self.dir, 0, sst_id), sst_id, 0, entries)?
        {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.levels[0].push(reader);
        }

        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            let lsn = wal.current_lsn();
            wal.purge_all_and_reset(lsn)?;
        }

        self.run_leveled_compaction_locked()
    }

    fn run_leveled_compaction_locked(&self) -> Result<(), DbError> {
        loop {
            // 1. Check L0 file count threshold -> compact all L0 into overlapping L1 files
            let l0_exceeded = {
                let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
                guard.levels[0].len() >= self.l0_trigger
            };
            if l0_exceeded {
                self.compact_l0_to_l1()?;
                continue;
            }

            // 2. Check L1..(MAX_LEVELS-1) byte size budget -> pick oldest/first file and merge into L(k+1)
            let mut compacted_higher = false;
            for lvl in 1..(MAX_LEVELS - 1) {
                let budget = self.level_max_bytes(lvl);
                let total_bytes: u64 = {
                    let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;
                    guard.levels[lvl].iter().map(|s| s.file_size).sum()
                };
                if total_bytes > budget {
                    self.compact_level_to_next(lvl)?;
                    compacted_higher = true;
                    break;
                }
            }

            if !compacted_higher {
                break;
            }
        }
        Ok(())
    }

    fn level_max_bytes(&self, level: usize) -> u64 {
        let exp = (level.saturating_sub(1)) as u32;
        self.l1_max_bytes.saturating_mul(10u64.saturating_pow(exp))
    }

    fn compact_l0_to_l1(&self) -> Result<(), DbError> {
        let (l0_sources, l1_overlapping, is_bottommost, start_id) = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            if guard.levels[0].is_empty() {
                return Ok(());
            }

            let min_k = guard.levels[0]
                .iter()
                .map(|s| s.min_key.clone())
                .min()
                .unwrap();
            let max_k = guard.levels[0]
                .iter()
                .map(|s| s.max_key.clone())
                .max()
                .unwrap();

            let mut l1_overlap = Vec::new();
            let mut l1_keep = Vec::new();
            for sst in guard.levels[1].drain(..) {
                if sst.overlaps_range(&min_k, &max_k) {
                    l1_overlap.push(sst);
                } else {
                    l1_keep.push(sst);
                }
            }
            guard.levels[1] = l1_keep;

            let l0_drain: Vec<SsTableReader> = std::mem::take(&mut guard.levels[0]);
            let is_bottommost = guard.levels.iter().skip(2).all(Vec::is_empty);
            let start_id = guard.next_sst_id;
            guard.next_sst_id += 64;
            (l0_drain, l1_overlap, is_bottommost, start_id)
        };

        // Ranked sources: L1 (older) -> L0 oldest..newest
        let mut ranked = Vec::new();
        let mut l1_entries = Vec::new();
        for sst in &l1_overlapping {
            l1_entries.extend(sst.read_all()?);
        }
        if !l1_entries.is_empty() {
            ranked.push(l1_entries);
        }
        for sst in &l0_sources {
            ranked.push(sst.read_all()?);
        }

        let new_tables = self.write_merged_run(1, start_id, ranked, is_bottommost)?;

        {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.levels[1].extend(new_tables);
            guard.levels[1].sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        self.cleanup_old_ssts(l0_sources.into_iter().chain(l1_overlapping))?;
        Ok(())
    }

    fn compact_level_to_next(&self, level: usize) -> Result<(), DbError> {
        let next_level = level + 1;
        let (picked_src, next_overlapping, is_bottommost, start_id) = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            if guard.levels[level].is_empty() {
                return Ok(());
            }
            // Pick the file with lowest ID (oldest) from `level`
            let pick_idx = guard.levels[level]
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.id)
                .map(|(i, _)| i)
                .unwrap();
            let picked = guard.levels[level].remove(pick_idx);

            let mut next_overlap = Vec::new();
            let mut next_keep = Vec::new();
            for sst in guard.levels[next_level].drain(..) {
                if sst.overlaps_range(&picked.min_key, &picked.max_key) {
                    next_overlap.push(sst);
                } else {
                    next_keep.push(sst);
                }
            }
            guard.levels[next_level] = next_keep;

            let is_bottommost = guard.levels.iter().skip(next_level + 1).all(Vec::is_empty);
            let start_id = guard.next_sst_id;
            guard.next_sst_id += 64;
            (picked, next_overlap, is_bottommost, start_id)
        };

        let mut ranked = Vec::new();
        let mut next_entries = Vec::new();
        for sst in &next_overlapping {
            next_entries.extend(sst.read_all()?);
        }
        if !next_entries.is_empty() {
            ranked.push(next_entries);
        }
        ranked.push(picked_src.read_all()?);

        let new_tables = self.write_merged_run(next_level, start_id, ranked, is_bottommost)?;

        {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.levels[next_level].extend(new_tables);
            guard.levels[next_level].sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }

        self.cleanup_old_ssts(std::iter::once(picked_src).chain(next_overlapping))?;
        Ok(())
    }

    fn write_merged_run(
        &self,
        target_level: usize,
        mut next_id: u64,
        ranked_sources: Vec<Vec<(Vec<u8>, EntryValue)>>,
        drop_tombstones: bool,
    ) -> Result<Vec<SsTableReader>, DbError> {
        let merge_iter = KWayMergeIterator::new(ranked_sources);
        let mut out_tables = Vec::new();
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0usize;

        for (k, v) in merge_iter {
            if drop_tombstones && matches!(v, EntryValue::Delete) {
                continue;
            }
            chunk_bytes += k.len() + entry_size(&v);
            chunk.push((k, v));

            if chunk_bytes >= TARGET_SST_BYTES {
                let id = next_id;
                next_id += 1;
                if let Some(sst) = SsTableReader::write_from_sorted(
                    sst_path(&self.dir, target_level, id),
                    id,
                    target_level,
                    std::mem::take(&mut chunk),
                )? {
                    out_tables.push(sst);
                }
                chunk_bytes = 0;
            }
        }

        if !chunk.is_empty() {
            let id = next_id;
            if let Some(sst) = SsTableReader::write_from_sorted(
                sst_path(&self.dir, target_level, id),
                id,
                target_level,
                chunk,
            )? {
                out_tables.push(sst);
            }
        }

        Ok(out_tables)
    }

    fn cleanup_old_ssts<I>(&self, obsolete: I) -> Result<(), DbError>
    where
        I: IntoIterator<Item = SsTableReader>,
    {
        let mut cache = self.block_cache.lock().map_err(|_| DbError::LockPoisoned)?;
        for sst in obsolete {
            cache.evict_sst(sst.id);
            let _ = fs::remove_file(sst_path(&self.dir, sst.level, sst.id));
        }
        Ok(())
    }
}

fn sst_path(dir: &Path, level: usize, id: u64) -> PathBuf {
    dir.join(format!("sst_L{level:02}_{id:08}.sst"))
}

fn list_sst_files(dir: &Path) -> Result<Vec<(usize, u64)>, DbError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if let Some(rest) = s.strip_prefix("sst_L").and_then(|r| r.strip_suffix(".sst"))
            && let Some((lvl_str, id_str)) = rest.split_once('_')
            && let (Ok(lvl), Ok(id)) = (lvl_str.parse::<usize>(), id_str.parse::<u64>())
        {
            out.push((lvl, id));
        }
    }
    Ok(out)
}

fn entry_size(v: &EntryValue) -> usize {
    match v {
        EntryValue::Put(b) => b.len(),
        EntryValue::Delete => 1,
    }
}

fn encode_wal_mutation(key: &[u8], val: &EntryValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    match val {
        EntryValue::Put(v) => {
            out.push(0x01);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        EntryValue::Delete => {
            out.push(0x02);
        }
    }
    out
}

fn decode_wal_mutation(buf: &[u8]) -> Result<(Vec<u8>, EntryValue), DbError> {
    if buf.len() < 5 {
        return Err(DbError::CorruptedData("Truncated LSM WAL record"));
    }
    let klen = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if buf.len() < 5 + klen {
        return Err(DbError::CorruptedData("Truncated LSM WAL key"));
    }
    let key = buf[4..4 + klen].to_vec();
    match buf[4 + klen] {
        0x01 => {
            let vstart = 5 + klen;
            if buf.len() < vstart + 4 {
                return Err(DbError::CorruptedData("Truncated LSM WAL value len"));
            }
            let vlen = u32::from_le_bytes(buf[vstart..vstart + 4].try_into().unwrap()) as usize;
            if buf.len() < vstart + 4 + vlen {
                return Err(DbError::CorruptedData("Truncated LSM WAL value"));
            }
            Ok((
                key,
                EntryValue::Put(buf[vstart + 4..vstart + 4 + vlen].to_vec()),
            ))
        }
        0x02 => Ok((key, EntryValue::Delete)),
        _ => Err(DbError::CorruptedData("Invalid LSM WAL tag")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsm_leveled_compaction_block_cache_and_recovery() {
        let dir = std::env::temp_dir().join(format!("lsm_leveled_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            // Small thresholds to exercise L0 -> L1 -> L2 leveled compaction and LRU BlockCache
            let lsm = LsmEngine::open_with_tuning(&dir, 1024, 3, 4096, 8, 4096).unwrap();
            for i in 0..250 {
                let k = format!("account:region_ap_northeast:{i:05}");
                let v = format!("balance_snapshot_payload_{i:05}");
                lsm.put(k.as_bytes(), v.as_bytes()).unwrap();
            }

            // Overwrite and delete across levels
            lsm.put(b"account:region_ap_northeast:00010", b"updated_10")
                .unwrap();
            lsm.delete(b"account:region_ap_northeast:00020").unwrap();
            lsm.flush().unwrap();

            // Repeated point lookups should hit the LRU BlockCache
            for _ in 0..3 {
                assert_eq!(
                    lsm.get(b"account:region_ap_northeast:00010").unwrap(),
                    Some(b"updated_10".to_vec())
                );
            }
            assert_eq!(lsm.get(b"account:region_ap_northeast:00020").unwrap(), None);

            let st = lsm.stats().unwrap();
            assert!(st.cache_stats.hits >= 2);
            // Data should have propagated beyond L0 into L1 / L2
            assert!(st.level_file_counts[1] > 0 || st.level_file_counts[2] > 0);

            let scan = lsm
                .scan_prefix(b"account:region_ap_northeast:0001")
                .unwrap();
            // 00010..00019 = 10 items
            assert_eq!(scan.len(), 10);
        }

        // Reopen and verify all levels + WAL state recover intact
        {
            let lsm = LsmEngine::open_with_tuning(&dir, 1024, 3, 4096, 8, 4096).unwrap();
            assert_eq!(
                lsm.get(b"account:region_ap_northeast:00010").unwrap(),
                Some(b"updated_10".to_vec())
            );
            assert_eq!(lsm.get(b"account:region_ap_northeast:00020").unwrap(), None);
            assert_eq!(
                lsm.get(b"account:region_ap_northeast:00249").unwrap(),
                Some(b"balance_snapshot_payload_00249".to_vec())
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
