use common::{DbError, crc32};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const DEFAULT_WAL_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const WAL_MAGIC: &[u8; 4] = b"AWAL";
const WAL_VERSION: u16 = 1;
const WAL_HEADER_SIZE: usize = 8;
const MAX_BACKLOG_ENTRIES: usize = 10_000;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: u64,
    pub payload: Vec<u8>,
}

/// Segmented write-ahead log supporting generic binary payloads, CRC32 verification,
/// rotation, purging, and bounded in-memory replication backlog.
pub struct WalManager {
    dir: PathBuf,
    segment_max_bytes: u64,
    active_seg_id: u64,
    active_file: File,
    active_bytes: u64,
    next_lsn: u64,
    backlog: VecDeque<WalRecord>,
}

impl WalManager {
    pub fn open(
        dir: impl AsRef<Path>,
        segment_max_bytes: u64,
    ) -> Result<(Self, Vec<WalRecord>), DbError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut seg_ids = list_segment_ids(&dir)?;
        if seg_ids.is_empty() {
            seg_ids.push(1);
        }

        let mut recovered = Vec::new();
        let mut max_lsn = 0u64;

        for &seg_id in &seg_ids {
            let path = segment_path(&dir, seg_id);
            if path.exists() {
                let entries = read_segment_file(&path)?;
                for e in entries {
                    if e.lsn > max_lsn {
                        max_lsn = e.lsn;
                    }
                    recovered.push(e);
                }
            }
        }

        let active_seg_id = *seg_ids.last().unwrap();
        let active_path = segment_path(&dir, active_seg_id);
        let active_file = open_or_init_segment(&active_path)?;
        let active_bytes = active_file.metadata()?.len();

        let mut backlog = VecDeque::with_capacity(recovered.len().min(MAX_BACKLOG_ENTRIES));
        for e in &recovered {
            if backlog.len() == MAX_BACKLOG_ENTRIES {
                backlog.pop_front();
            }
            backlog.push_back(e.clone());
        }

        Ok((
            Self {
                dir,
                segment_max_bytes: segment_max_bytes.max(1024),
                active_seg_id,
                active_file,
                active_bytes,
                next_lsn: max_lsn + 1,
                backlog,
            },
            recovered,
        ))
    }

    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.saturating_sub(1)
    }

    pub fn append(&mut self, payload: Vec<u8>) -> Result<u64, DbError> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        let frame = encode_record(lsn, &payload);
        self.active_file.write_all(&frame)?;
        self.active_file.sync_data()?;
        self.active_bytes += frame.len() as u64;

        if self.backlog.len() == MAX_BACKLOG_ENTRIES {
            self.backlog.pop_front();
        }
        self.backlog.push_back(WalRecord { lsn, payload });

        if self.active_bytes >= self.segment_max_bytes {
            self.rotate_segment()?;
        }

        Ok(lsn)
    }

    pub fn fetch_since(&self, after_lsn: u64) -> Result<Vec<WalRecord>, DbError> {
        let current = self.current_lsn();
        if after_lsn >= current {
            return Ok(Vec::new());
        }
        if let Some(oldest) = self.backlog.front()
            && after_lsn + 1 < oldest.lsn
        {
            return Err(DbError::ReplicationLag {
                requested_after_lsn: after_lsn,
                oldest_available_lsn: oldest.lsn,
            });
        }
        Ok(self
            .backlog
            .iter()
            .filter(|e| e.lsn > after_lsn)
            .cloned()
            .collect())
    }

    pub fn purge_all_and_reset(&mut self, base_lsn: u64) -> Result<(), DbError> {
        for seg_id in list_segment_ids(&self.dir)? {
            let _ = fs::remove_file(segment_path(&self.dir, seg_id));
        }
        self.active_seg_id += 1;
        let path = segment_path(&self.dir, self.active_seg_id);
        self.active_file = open_or_init_segment(&path)?;
        self.active_bytes = WAL_HEADER_SIZE as u64;
        self.next_lsn = base_lsn + 1;
        self.backlog.clear();
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), DbError> {
        self.active_seg_id += 1;
        let path = segment_path(&self.dir, self.active_seg_id);
        self.active_file = open_or_init_segment(&path)?;
        self.active_bytes = WAL_HEADER_SIZE as u64;

        let seg_ids = list_segment_ids(&self.dir)?;
        if seg_ids.len() > 3 {
            for &old_id in &seg_ids[..seg_ids.len() - 3] {
                let _ = fs::remove_file(segment_path(&self.dir, old_id));
            }
        }
        Ok(())
    }
}

fn segment_path(dir: &Path, seg_id: u64) -> PathBuf {
    dir.join(format!("wal_{seg_id:08}.log"))
}

fn list_segment_ids(dir: &Path) -> Result<Vec<u64>, DbError> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(num_part) = name_str
            .strip_prefix("wal_")
            .and_then(|s| s.strip_suffix(".log"))
            && let Ok(id) = num_part.parse::<u64>()
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn open_or_init_segment(path: &Path) -> Result<File, DbError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;

    if file.metadata()?.len() == 0 {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(WAL_MAGIC);
        header[4..6].copy_from_slice(&WAL_VERSION.to_le_bytes());
        file.write_all(&header)?;
        file.sync_data()?;
    } else {
        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;
        if &header[0..4] != WAL_MAGIC {
            return Err(DbError::InvalidMagic);
        }
        let ver = u16::from_le_bytes([header[4], header[5]]);
        if ver != WAL_VERSION {
            return Err(DbError::UnsupportedVersion(ver));
        }
        file.seek(SeekFrom::End(0))?;
    }
    Ok(file)
}

fn encode_record(lsn: u64, payload: &[u8]) -> Vec<u8> {
    let total_len = (8 + payload.len() + 4) as u32;
    let mut buf = Vec::with_capacity(4 + total_len as usize);
    buf.extend_from_slice(&total_len.to_le_bytes());
    buf.extend_from_slice(&lsn.to_le_bytes());
    buf.extend_from_slice(payload);
    let checksum = crc32(&buf[4..]);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

fn read_segment_file(path: &Path) -> Result<Vec<WalRecord>, DbError> {
    let bytes = fs::read(path)?;
    if bytes.len() < WAL_HEADER_SIZE {
        return Ok(Vec::new());
    }
    if &bytes[0..4] != WAL_MAGIC {
        return Err(DbError::InvalidMagic);
    }

    let mut cur = WAL_HEADER_SIZE;
    let mut out = Vec::new();
    while cur + 4 <= bytes.len() {
        let total_len = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap()) as usize;
        if total_len < 12 || cur + 4 + total_len > bytes.len() {
            break;
        }
        let body_start = cur + 4;
        let crc_start = body_start + total_len - 4;
        let expected_crc = u32::from_le_bytes(bytes[crc_start..crc_start + 4].try_into().unwrap());
        let actual_crc = crc32(&bytes[body_start..crc_start]);
        if expected_crc != actual_crc {
            break;
        }

        let lsn = u64::from_le_bytes(bytes[body_start..body_start + 8].try_into().unwrap());
        let payload = bytes[body_start + 8..crc_start].to_vec();
        out.push(WalRecord { lsn, payload });
        cur = crc_start + 4;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_append_recover_and_purge() {
        let dir = std::env::temp_dir().join(format!("wal_basic_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            let (mut wal, recovered) = WalManager::open(&dir, 4096).unwrap();
            assert!(recovered.is_empty());
            assert_eq!(wal.append(b"op1".to_vec()).unwrap(), 1);
            assert_eq!(wal.append(b"op2".to_vec()).unwrap(), 2);
            assert_eq!(wal.current_lsn(), 2);
        }

        {
            let (mut wal, recovered) = WalManager::open(&dir, 4096).unwrap();
            assert_eq!(recovered.len(), 2);
            assert_eq!(recovered[0].payload, b"op1");
            assert_eq!(recovered[1].payload, b"op2");

            wal.purge_all_and_reset(2).unwrap();
            assert_eq!(wal.append(b"op3".to_vec()).unwrap(), 3);
        }

        {
            let (_, recovered) = WalManager::open(&dir, 4096).unwrap();
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].lsn, 3);
            assert_eq!(recovered[0].payload, b"op3");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_segment_rotation_and_fetch_since() {
        let dir = std::env::temp_dir().join(format!("wal_rot_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        // Minimum segment size is clamped to 1024B; write enough records to trigger multiple rotations
        let (mut wal, _) = WalManager::open(&dir, 1024).unwrap();
        let payload = vec![0xABu8; 300];
        for i in 1..=20 {
            assert_eq!(wal.append(payload.clone()).unwrap(), i);
        }

        // Oldest archived segments beyond 3 must be cleaned up automatically
        let seg_ids = list_segment_ids(&dir).unwrap();
        assert!(seg_ids.len() <= 3);
        assert!(*seg_ids.last().unwrap() > 1);

        // Incremental fetch
        let batch = wal.fetch_since(17).unwrap();
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].lsn, 18);
        assert_eq!(batch[2].lsn, 20);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_torn_tail_and_crc_corruption_recovery() {
        let dir = std::env::temp_dir().join(format!("wal_corrupt_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            let (mut wal, _) = WalManager::open(&dir, 65536).unwrap();
            wal.append(b"valid_1".to_vec()).unwrap();
            wal.append(b"valid_2".to_vec()).unwrap();
            wal.append(b"corrupted_tail".to_vec()).unwrap();
        }

        // Corrupt the last byte (CRC32) of the 3rd record to simulate power loss mid-write
        let seg_file = segment_path(&dir, 1);
        let mut raw = fs::read(&seg_file).unwrap();
        let last_idx = raw.len() - 1;
        raw[last_idx] ^= 0xFF;
        fs::write(&seg_file, &raw).unwrap();

        // Recovery must preserve the first 2 valid records and cleanly discard the damaged tail
        let (mut wal, recovered) = WalManager::open(&dir, 65536).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].payload, b"valid_1");
        assert_eq!(recovered[1].payload, b"valid_2");
        assert_eq!(wal.current_lsn(), 2);
        assert_eq!(wal.append(b"after_recovery".to_vec()).unwrap(), 3);

        let _ = fs::remove_dir_all(&dir);
    }
}
