use common::{DbError, crc32};
use erasure::ReedSolomon;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use storage_lsm::{DEFAULT_L0_COMPACTION_TRIGGER, DEFAULT_MEMTABLE_MAX_BYTES, KvPair, LsmEngine};
use wal::DEFAULT_WAL_SEGMENT_BYTES;

const NO_EXPIRY: u64 = 0;
const SHARD_MAGIC: &[u8; 4] = b"ASHD";

/// Key-Value Store built on top of `storage-lsm` with TTL expiration,
/// atomic Compare-And-Swap (CAS), and Reed-Solomon erasure-coded shard snapshots.
pub struct KvStore {
    engine: LsmEngine,
    cas_lock: Mutex<()>,
}

impl KvStore {
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
        compaction_trigger: usize,
        wal_segment_bytes: u64,
    ) -> Result<Self, DbError> {
        let engine = LsmEngine::open_with_options(
            dir,
            memtable_max_bytes,
            compaction_trigger,
            wal_segment_bytes,
        )?;
        Ok(Self {
            engine,
            cas_lock: Mutex::new(()),
        })
    }

    /// Inserts or overwrites a key without expiration.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        let _guard = self.cas_lock.lock().map_err(|_| DbError::LockPoisoned)?;
        let encoded = encode_kv_payload(NO_EXPIRY, value);
        self.engine.put(key, &encoded)
    }

    /// Inserts or overwrites a key that automatically expires after `ttl_ms` milliseconds.
    pub fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl_ms: u64) -> Result<(), DbError> {
        let _guard = self.cas_lock.lock().map_err(|_| DbError::LockPoisoned)?;
        let expires_at = now_millis().saturating_add(ttl_ms.max(1));
        let encoded = encode_kv_payload(expires_at, value);
        self.engine.put(key, &encoded)
    }

    /// Retrieves the live value for `key`, returning `None` if missing or expired.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let Some(raw) = self.engine.get(key)? else {
            return Ok(None);
        };
        let (expires_at, payload) = decode_kv_payload(&raw)?;
        if is_expired(expires_at, now_millis()) {
            return Ok(None);
        }
        Ok(Some(payload.to_vec()))
    }

    /// Deletes `key` by writing a tombstone into the LSM engine.
    pub fn delete(&self, key: &[u8]) -> Result<(), DbError> {
        let _guard = self.cas_lock.lock().map_err(|_| DbError::LockPoisoned)?;
        self.engine.delete(key)
    }

    /// Atomically replaces `key` with `new_value` only if its current live value matches `expected`.
    /// Pass `expected = None` to insert only if the key does not currently exist (Put-If-Absent).
    /// Returns `Ok(true)` if swapped, `Ok(false)` if current value did not match `expected`.
    pub fn compare_and_swap(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: Option<&[u8]>,
    ) -> Result<bool, DbError> {
        let _guard = self.cas_lock.lock().map_err(|_| DbError::LockPoisoned)?;
        let current = match self.engine.get(key)? {
            Some(raw) => {
                let (expires_at, payload) = decode_kv_payload(&raw)?;
                if is_expired(expires_at, now_millis()) {
                    None
                } else {
                    Some(payload.to_vec())
                }
            }
            None => None,
        };

        if current.as_deref() != expected {
            return Ok(false);
        }

        match new_value {
            Some(val) => {
                let encoded = encode_kv_payload(NO_EXPIRY, val);
                self.engine.put(key, &encoded)?;
            }
            None => {
                self.engine.delete(key)?;
            }
        }
        Ok(true)
    }

    /// Scans all non-expired key-value pairs starting with `prefix` in sorted key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>, DbError> {
        let now = now_millis();
        let raw_pairs = self.engine.scan_prefix(prefix)?;
        let mut out = Vec::with_capacity(raw_pairs.len());
        for (k, raw) in raw_pairs {
            let (expires_at, payload) = decode_kv_payload(&raw)?;
            if !is_expired(expires_at, now) {
                out.push((k, payload.to_vec()));
            }
        }
        Ok(out)
    }

    pub fn flush(&self) -> Result<(), DbError> {
        self.engine.flush()
    }

    pub fn compact(&self) -> Result<(), DbError> {
        self.engine.compact()
    }

    /// Exports a full consistent snapshot encoded with Reed-Solomon `(k, m)` across `shard_dirs`.
    /// `shard_dirs.len()` must equal `data_shards + parity_shards` (e.g., 14 directories for RS(10,4)).
    pub fn export_erasure_snapshot(
        &self,
        shard_dirs: &[PathBuf],
        data_shards: usize,
        parity_shards: usize,
    ) -> Result<(), DbError> {
        if shard_dirs.len() != data_shards + parity_shards {
            return Err(DbError::CorruptedData(
                "shard_dirs count must match data_shards + parity_shards",
            ));
        }

        let rs = ReedSolomon::new(data_shards, parity_shards)?;
        let live_pairs = self.scan_prefix(b"")?;
        let snapshot_blob = serialize_snapshot(&live_pairs);
        let (shards, orig_len) = rs.encode(&snapshot_blob);

        for (idx, (dir, shard_data)) in shard_dirs.iter().zip(shards.iter()).enumerate() {
            fs::create_dir_all(dir)?;
            let frame = encode_shard_file(idx as u16, orig_len as u64, shard_data);
            let file_path = dir.join(format!("kv_snap_shard_{idx:03}.ec"));
            let tmp_path = dir.join(format!("kv_snap_shard_{idx:03}.ec.tmp"));
            fs::write(&tmp_path, &frame)?;
            fs::rename(&tmp_path, &file_path)?;
        }
        Ok(())
    }

    /// Restores all key-value pairs from `shard_dirs` using Reed-Solomon reconstruction,
    /// tolerating up to `parity_shards` missing or corrupted shard files.
    pub fn restore_erasure_snapshot(
        &self,
        shard_dirs: &[PathBuf],
        data_shards: usize,
        parity_shards: usize,
    ) -> Result<usize, DbError> {
        let total = data_shards + parity_shards;
        if shard_dirs.len() != total {
            return Err(DbError::CorruptedData(
                "shard_dirs count must match data_shards + parity_shards",
            ));
        }

        let rs = ReedSolomon::new(data_shards, parity_shards)?;
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut orig_len: Option<usize> = None;

        for (idx, dir) in shard_dirs.iter().enumerate() {
            let file_path = dir.join(format!("kv_snap_shard_{idx:03}.ec"));
            if let Ok(bytes) = fs::read(&file_path)
                && let Ok((shard_idx, parsed_orig_len, payload)) = decode_shard_file(&bytes)
                && shard_idx as usize == idx
            {
                if let Some(expected) = orig_len {
                    if parsed_orig_len as usize != expected {
                        continue;
                    }
                } else {
                    orig_len = Some(parsed_orig_len as usize);
                }
                shards[idx] = Some(payload);
            }
        }

        let orig_len = orig_len.ok_or(DbError::CorruptedData("No valid erasure shards found"))?;
        let snapshot_blob = rs.reconstruct(&mut shards, orig_len)?;
        let pairs = deserialize_snapshot(&snapshot_blob)?;

        let count = pairs.len();
        for (k, v) in pairs {
            self.put(&k, &v)?;
        }
        Ok(count)
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn is_expired(expires_at: u64, now: u64) -> bool {
    expires_at != NO_EXPIRY && now >= expires_at
}

fn encode_kv_payload(expires_at: u64, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + value.len());
    out.extend_from_slice(&expires_at.to_le_bytes());
    out.extend_from_slice(value);
    out
}

fn decode_kv_payload(raw: &[u8]) -> Result<(u64, &[u8]), DbError> {
    if raw.len() < 8 {
        return Err(DbError::CorruptedData("Truncated KV metadata header"));
    }
    let expires_at = u64::from_le_bytes(raw[0..8].try_into().unwrap());
    Ok((expires_at, &raw[8..]))
}

fn serialize_snapshot(pairs: &[KvPair]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
    for (k, v) in pairs {
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
        buf.extend_from_slice(v);
    }
    buf
}

fn deserialize_snapshot(buf: &[u8]) -> Result<Vec<KvPair>, DbError> {
    if buf.len() < 4 {
        return Err(DbError::CorruptedData("Truncated snapshot header"));
    }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut cur = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if cur + 4 > buf.len() {
            return Err(DbError::CorruptedData("Truncated snapshot key len"));
        }
        let klen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + klen + 4 > buf.len() {
            return Err(DbError::CorruptedData("Truncated snapshot key/val len"));
        }
        let k = buf[cur..cur + klen].to_vec();
        cur += klen;
        let vlen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + vlen > buf.len() {
            return Err(DbError::CorruptedData("Truncated snapshot val"));
        }
        let v = buf[cur..cur + vlen].to_vec();
        cur += vlen;
        out.push((k, v));
    }
    Ok(out)
}

fn encode_shard_file(shard_idx: u16, orig_len: u64, payload: &[u8]) -> Vec<u8> {
    // Frame: [magic: 4B][shard_idx: 2B][orig_len: 8B][payload_len: 4B][payload: NB][crc32: 4B]
    let mut buf = Vec::with_capacity(22 + payload.len());
    buf.extend_from_slice(SHARD_MAGIC);
    buf.extend_from_slice(&shard_idx.to_le_bytes());
    buf.extend_from_slice(&orig_len.to_le_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

fn decode_shard_file(bytes: &[u8]) -> Result<(u16, u64, Vec<u8>), DbError> {
    if bytes.len() < 22 {
        return Err(DbError::CorruptedData("Shard file too small"));
    }
    if &bytes[0..4] != SHARD_MAGIC {
        return Err(DbError::InvalidMagic);
    }
    let crc_offset = bytes.len() - 4;
    let expected_crc = u32::from_le_bytes(bytes[crc_offset..].try_into().unwrap());
    let actual_crc = crc32(&bytes[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(DbError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let shard_idx = u16::from_le_bytes([bytes[4], bytes[5]]);
    let orig_len = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let payload_len = u32::from_le_bytes(bytes[14..18].try_into().unwrap()) as usize;
    if 18 + payload_len != crc_offset {
        return Err(DbError::CorruptedData("Shard payload length mismatch"));
    }
    Ok((shard_idx, orig_len, bytes[18..crc_offset].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_kv_cas_insert_update_delete() {
        let dir = std::env::temp_dir().join(format!("kv_cas_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let kv = KvStore::open(&dir).unwrap();

        // Put-If-Absent (expected = None) succeeds only once
        assert!(
            kv.compare_and_swap(b"lock:job1", None, Some(b"worker_a"))
                .unwrap()
        );
        assert!(
            !kv.compare_and_swap(b"lock:job1", None, Some(b"worker_b"))
                .unwrap()
        );
        assert_eq!(kv.get(b"lock:job1").unwrap(), Some(b"worker_a".to_vec()));

        // Conditional update
        assert!(
            kv.compare_and_swap(b"lock:job1", Some(b"worker_a"), Some(b"worker_b"))
                .unwrap()
        );
        assert_eq!(kv.get(b"lock:job1").unwrap(), Some(b"worker_b".to_vec()));

        // Conditional delete (new_value = None)
        assert!(
            !kv.compare_and_swap(b"lock:job1", Some(b"wrong"), None)
                .unwrap()
        );
        assert!(
            kv.compare_and_swap(b"lock:job1", Some(b"worker_b"), None)
                .unwrap()
        );
        assert_eq!(kv.get(b"lock:job1").unwrap(), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_kv_ttl_expiration_and_scan_filtering() {
        let dir = std::env::temp_dir().join(format!("kv_ttl_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        let kv = KvStore::open(&dir).unwrap();
        kv.put(b"item:permanent", b"keep").unwrap();
        kv.put_with_ttl(b"item:ephemeral", b"temp", 40).unwrap();

        assert_eq!(kv.scan_prefix(b"item:").unwrap().len(), 2);

        thread::sleep(Duration::from_millis(60));
        assert_eq!(kv.get(b"item:ephemeral").unwrap(), None);

        let live = kv.scan_prefix(b"item:").unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0], (b"item:permanent".to_vec(), b"keep".to_vec()));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_kv_rs_10_4_snapshot_corruption_and_missing_recovery() {
        let base = std::env::temp_dir().join(format!("kv_rs_{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);

        let kv = KvStore::open(base.join("primary")).unwrap();
        for i in 0..25 {
            kv.put(
                format!("key:{i:02}").as_bytes(),
                format!("value_payload_{i}").as_bytes(),
            )
            .unwrap();
        }

        let shard_dirs: Vec<PathBuf> = (0..14).map(|i| base.join(format!("disk_{i:02}"))).collect();
        kv.export_erasure_snapshot(&shard_dirs, 10, 4).unwrap();

        // Delete 2 shard directories completely
        fs::remove_dir_all(&shard_dirs[1]).unwrap();
        fs::remove_dir_all(&shard_dirs[11]).unwrap();

        // Corrupt 1 byte inside 2 surviving shard files (bit-rot simulation)
        for &corrupt_idx in &[3usize, 8usize] {
            let shard_file =
                shard_dirs[corrupt_idx].join(format!("kv_snap_shard_{corrupt_idx:03}.ec"));
            let mut raw = fs::read(&shard_file).unwrap();
            let mid = raw.len() / 2;
            raw[mid] ^= 0x5A;
            fs::write(&shard_file, &raw).unwrap();
        }

        // Reconstruct from the remaining 10 intact shards
        let restored_kv = KvStore::open(base.join("restored")).unwrap();
        let count = restored_kv
            .restore_erasure_snapshot(&shard_dirs, 10, 4)
            .unwrap();
        assert_eq!(count, 25);
        assert_eq!(
            restored_kv.get(b"key:24").unwrap(),
            Some(b"value_payload_24".to_vec())
        );

        let _ = fs::remove_dir_all(&base);
    }
}
