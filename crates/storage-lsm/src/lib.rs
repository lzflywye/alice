pub mod bloom;
pub mod sstable;

use common::DbError;
use sstable::{EntryValue, SsTableReader};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use wal::{DEFAULT_WAL_SEGMENT_BYTES, WalManager};

pub const DEFAULT_MEMTABLE_MAX_BYTES: usize = 256 * 1024; // 256 KB
pub const DEFAULT_COMPACTION_TRIGGER: usize = 4;

pub type KvPair = (Vec<u8>, Vec<u8>);

struct LsmInner {
    memtable: BTreeMap<Vec<u8>, EntryValue>,
    memtable_bytes: usize,
    sstables: Vec<SsTableReader>, // Ordered oldest -> newest by ID
    next_sst_id: u64,
}

/// Thread-safe LSM-Tree storage engine backed by `wal::WalManager` and immutable SSTables.
pub struct LsmEngine {
    dir: PathBuf,
    memtable_max_bytes: usize,
    compaction_trigger: usize,
    inner: RwLock<LsmInner>,
    wal: Mutex<WalManager>,
    writer: Mutex<()>,
}

impl LsmEngine {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(
            dir,
            DEFAULT_MEMTABLE_MAX_BYTES,
            DEFAULT_COMPACTION_TRIGGER,
            DEFAULT_WAL_SEGMENT_BYTES,
        )
    }

    pub fn open_with_options(
        dir: impl AsRef<Path>,
        memtable_max_bytes: usize,
        compaction_trigger: usize,
        wal_segment_bytes: u64,
    ) -> Result<Self, DbError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // 1. Load existing SSTables sorted by ID ascending
        let mut sst_ids = list_sst_ids(&dir)?;
        sst_ids.sort_unstable();
        let mut sstables = Vec::with_capacity(sst_ids.len());
        for id in &sst_ids {
            sstables.push(SsTableReader::open(sst_path(&dir, *id), *id)?);
        }
        let next_sst_id = sst_ids.last().copied().unwrap_or(0) + 1;

        // 2. Recover unflushed mutations from WAL into MemTable
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
            memtable_max_bytes: memtable_max_bytes.max(1024),
            compaction_trigger: compaction_trigger.max(2),
            inner: RwLock::new(LsmInner {
                memtable,
                memtable_bytes,
                sstables,
                next_sst_id,
            }),
            wal: Mutex::new(wal_mgr),
            writer: Mutex::new(()),
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

        // 1. Check MemTable first (newest)
        if let Some(entry) = guard.memtable.get(key) {
            return Ok(match entry {
                EntryValue::Put(v) => Some(v.clone()),
                EntryValue::Delete => None,
            });
        }

        // 2. Search SSTables from newest to oldest
        for sst in guard.sstables.iter().rev() {
            if let Some(entry) = sst.get(key)? {
                return Ok(match entry {
                    EntryValue::Put(v) => Some(v),
                    EntryValue::Delete => None,
                });
            }
        }

        Ok(None)
    }

    /// Scans all live key-value pairs starting with `prefix` in lexicographical order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>, DbError> {
        let guard = self.inner.read().map_err(|_| DbError::LockPoisoned)?;

        // Merge oldest -> newest so newer entries overwrite older ones
        let mut merged: BTreeMap<Vec<u8>, EntryValue> = BTreeMap::new();
        for sst in &guard.sstables {
            for (k, v) in sst.read_all()? {
                if k.starts_with(prefix) {
                    merged.insert(k, v);
                }
            }
        }
        for (k, v) in &guard.memtable {
            if k.starts_with(prefix) {
                merged.insert(k.clone(), v.clone());
            }
        }

        let mut out = Vec::new();
        for (k, v) in merged {
            if let EntryValue::Put(bytes) = v {
                out.push((k, bytes));
            }
        }
        Ok(out)
    }

    /// Forces flushing the current MemTable to an immutable SSTable.
    pub fn flush(&self) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        self.flush_memtable_locked()
    }

    /// Merges all existing SSTables into a single compacted SSTable and drops tombstones.
    pub fn compact(&self) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        self.compact_locked()
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

        let reader =
            SsTableReader::write_from_sorted(sst_path(&self.dir, sst_id), sst_id, entries)?;

        let need_compact = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.sstables.push(reader);
            guard.sstables.len() >= self.compaction_trigger
        };

        // Reset WAL since flushed entries are now durable in SSTable
        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            let lsn = wal.current_lsn();
            wal.purge_all_and_reset(lsn)?;
        }

        if need_compact {
            self.compact_locked()?;
        }
        Ok(())
    }

    fn compact_locked(&self) -> Result<(), DbError> {
        let (old_ids, new_id, merged_live) = {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            if guard.sstables.len() < 2 {
                return Ok(());
            }

            let mut merged: BTreeMap<Vec<u8>, EntryValue> = BTreeMap::new();
            for sst in &guard.sstables {
                for (k, v) in sst.read_all()? {
                    merged.insert(k, v);
                }
            }

            // Full compaction across all SSTables: safe to purge Delete tombstones
            let live: Vec<(Vec<u8>, EntryValue)> = merged
                .into_iter()
                .filter(|(_, v)| matches!(v, EntryValue::Put(_)))
                .collect();

            let new_id = guard.next_sst_id;
            guard.next_sst_id += 1;
            let old_ids: Vec<u64> = guard.sstables.iter().map(|s| s.id).collect();
            (old_ids, new_id, live)
        };

        let new_reader =
            SsTableReader::write_from_sorted(sst_path(&self.dir, new_id), new_id, merged_live)?;

        {
            let mut guard = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
            guard.sstables = vec![new_reader];
        }

        for old_id in old_ids {
            let _ = fs::remove_file(sst_path(&self.dir, old_id));
        }

        Ok(())
    }
}

fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("sst_{id:08}.sst"))
}

fn list_sst_ids(dir: &Path) -> Result<Vec<u64>, DbError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if let Some(num) = s.strip_prefix("sst_").and_then(|r| r.strip_suffix(".sst"))
            && let Ok(id) = num.parse::<u64>()
        {
            ids.push(id);
        }
    }
    Ok(ids)
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
    fn test_lsm_put_flush_compact_and_recovery() {
        let dir = std::env::temp_dir().join(format!("lsm_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            // Small memtable threshold (1024B) and compaction trigger (3 SSTables)
            let lsm = LsmEngine::open_with_options(&dir, 1024, 3, 4096).unwrap();
            for i in 0..50 {
                let k = format!("user:{i:04}");
                let v = format!("profile_payload_data_{i}");
                lsm.put(k.as_bytes(), v.as_bytes()).unwrap();
            }
            // Overwrite and delete some keys
            lsm.put(b"user:0001", b"updated_user_1").unwrap();
            lsm.delete(b"user:0002").unwrap();

            assert_eq!(
                lsm.get(b"user:0001").unwrap(),
                Some(b"updated_user_1".to_vec())
            );
            assert_eq!(lsm.get(b"user:0002").unwrap(), None);
            assert_eq!(lsm.get(b"nonexistent").unwrap(), None);

            let scan = lsm.scan_prefix(b"user:000").unwrap();
            // 0000..0009 minus 0002 = 9 items
            assert_eq!(scan.len(), 9);
        }

        // Reopen after simulated restart
        {
            let lsm = LsmEngine::open_with_options(&dir, 1024, 3, 4096).unwrap();
            assert_eq!(
                lsm.get(b"user:0001").unwrap(),
                Some(b"updated_user_1".to_vec())
            );
            assert_eq!(lsm.get(b"user:0002").unwrap(), None);
            assert_eq!(
                lsm.get(b"user:0049").unwrap(),
                Some(b"profile_payload_data_49".to_vec())
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
