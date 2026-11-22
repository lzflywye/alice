use crate::error::HdbError;
use crate::node::NODE_SLOT_SIZE;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if (crc & 1) != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    WriteSlot = 1,
    WriteHeap = 2,
    CommitTxn = 3,
}

impl TryFrom<u8> for WalRecordType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WalRecordType::WriteSlot),
            2 => Ok(WalRecordType::WriteHeap),
            3 => Ok(WalRecordType::CommitTxn),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub enum WalOp {
    WriteSlot {
        node_id: u64,
        slot_bytes: Box<[u8; NODE_SLOT_SIZE]>,
    },
    WriteHeap {
        offset: u64,
        data: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub lsn: u64,
    pub txn_id: u64,
    pub op: WalOp,
}

struct SegmentMeta {
    seg_id: u64,
    max_lsn: u64,
}

/// Segmented append-only physical Write-Ahead Log with automatic rotation and GC.
pub struct WalWriter {
    dir: PathBuf,
    current_seg_id: u64,
    file: File,
    next_lsn: u64,
    next_txn_id: u64,
    segment_max_bytes: u64,
    closed_segments: Vec<SegmentMeta>,
}

impl WalWriter {
    pub fn open(dir: &Path, segment_max_bytes: u64) -> Result<(Self, Vec<WalEntry>), HdbError> {
        std::fs::create_dir_all(dir)?;
        let segments = Self::discover_segments(dir)?;

        let mut all_committed = Vec::new();
        let mut max_lsn: u64 = 0;
        let mut max_txn_id: u64 = 0;
        let mut closed_segments = Vec::new();

        if segments.is_empty() {
            let seg_id = 1;
            let file = Self::open_segment_file(dir, seg_id)?;
            return Ok((
                Self {
                    dir: dir.to_path_buf(),
                    current_seg_id: seg_id,
                    file,
                    next_lsn: 1,
                    next_txn_id: 1,
                    segment_max_bytes,
                    closed_segments,
                },
                all_committed,
            ));
        }

        let last_idx = segments.len() - 1;
        for (idx, (seg_id, path)) in segments.iter().enumerate() {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(false)
                .truncate(false)
                .open(path)?;

            let (mut entries, seg_max_lsn, seg_max_txn, valid_end_pos) =
                Self::recover_from_file(&mut file, max_lsn, max_txn_id)?;

            if seg_max_lsn > max_lsn {
                max_lsn = seg_max_lsn;
            }
            if seg_max_txn > max_txn_id {
                max_txn_id = seg_max_txn;
            }
            all_committed.append(&mut entries);

            if idx < last_idx {
                closed_segments.push(SegmentMeta {
                    seg_id: *seg_id,
                    max_lsn: seg_max_lsn,
                });
            } else {
                // Active (latest) segment: truncate any torn tail bytes and seek to end
                file.set_len(valid_end_pos)?;
                file.seek(SeekFrom::End(0))?;
                file.sync_data()?;

                return Ok((
                    Self {
                        dir: dir.to_path_buf(),
                        current_seg_id: *seg_id,
                        file,
                        next_lsn: max_lsn + 1,
                        next_txn_id: max_txn_id + 1,
                        segment_max_bytes,
                        closed_segments,
                    },
                    all_committed,
                ));
            }
        }

        unreachable!()
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn current_segment_id(&self) -> u64 {
        self.current_seg_id
    }

    pub fn active_byte_len(&self) -> u64 {
        self.file.metadata().map(|m| m.len()).unwrap_or(0)
    }

    /// Appends a batch of physical operations atomically, rotating segments when full.
    pub fn append_transaction(&mut self, ops: &[WalOp]) -> Result<u64, HdbError> {
        if self.active_byte_len() >= self.segment_max_bytes {
            self.rotate_segment()?;
        }

        let txn_id = self.next_txn_id;
        self.next_txn_id += 1;

        for op in ops {
            let lsn = self.next_lsn;
            self.next_lsn += 1;
            match op {
                WalOp::WriteSlot {
                    node_id,
                    slot_bytes,
                } => {
                    self.write_raw_record(
                        WalRecordType::WriteSlot,
                        lsn,
                        txn_id,
                        *node_id,
                        slot_bytes.as_slice(),
                    )?;
                }
                WalOp::WriteHeap { offset, data } => {
                    self.write_raw_record(WalRecordType::WriteHeap, lsn, txn_id, *offset, data)?;
                }
            }
        }

        let commit_lsn = self.next_lsn;
        self.next_lsn += 1;
        self.write_raw_record(WalRecordType::CommitTxn, commit_lsn, txn_id, 0, &[])?;

        self.file.sync_data()?;
        Ok(commit_lsn)
    }

    /// Closes the current WAL segment and opens the next numbered segment file.
    pub fn rotate_segment(&mut self) -> Result<(), HdbError> {
        self.file.sync_data()?;
        let last_committed_lsn = self.next_lsn.saturating_sub(1);
        self.closed_segments.push(SegmentMeta {
            seg_id: self.current_seg_id,
            max_lsn: last_committed_lsn,
        });

        self.current_seg_id += 1;
        self.file = Self::open_segment_file(&self.dir, self.current_seg_id)?;
        Ok(())
    }

    /// Deletes closed WAL segments whose entries are both checkpointed and replicated.
    pub fn purge_segments_up_to(&mut self, safe_lsn: u64) -> Result<usize, HdbError> {
        let mut removed = 0;
        let mut remaining = Vec::new();

        for seg in self.closed_segments.drain(..) {
            if seg.max_lsn <= safe_lsn {
                let path = Self::segment_path(&self.dir, seg.seg_id);
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
                removed += 1;
            } else {
                remaining.push(seg);
            }
        }
        self.closed_segments = remaining;
        Ok(removed)
    }

    fn segment_path(dir: &Path, seg_id: u64) -> PathBuf {
        dir.join(format!("wal_{:08}.log", seg_id))
    }

    fn open_segment_file(dir: &Path, seg_id: u64) -> Result<File, HdbError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(Self::segment_path(dir, seg_id))?;
        Ok(file)
    }

    fn discover_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>, HdbError> {
        let mut map = BTreeMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            if let Some(rest) = name
                .strip_prefix("wal_")
                .and_then(|s| s.strip_suffix(".log"))
                && let Ok(id) = rest.parse::<u64>()
            {
                map.insert(id, entry.path());
            }
        }
        Ok(map.into_iter().collect())
    }

    fn write_raw_record(
        &mut self,
        rec_type: WalRecordType,
        lsn: u64,
        txn_id: u64,
        target_id: u64,
        payload: &[u8],
    ) -> Result<(), HdbError> {
        let mut frame = Vec::with_capacity(32 + payload.len() + 4);
        frame.push(rec_type as u8);
        frame.extend_from_slice(&[0u8; 3]);
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&lsn.to_le_bytes());
        frame.extend_from_slice(&txn_id.to_le_bytes());
        frame.extend_from_slice(&target_id.to_le_bytes());
        frame.extend_from_slice(payload);

        let checksum = crc32(&frame);
        frame.extend_from_slice(&checksum.to_le_bytes());

        self.file.write_all(&frame)?;
        Ok(())
    }

    fn recover_from_file(
        file: &mut File,
        init_lsn: u64,
        init_txn_id: u64,
    ) -> Result<(Vec<WalEntry>, u64, u64, u64), HdbError> {
        file.seek(SeekFrom::Start(0))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;

        let mut pos: usize = 0;
        let mut valid_pos: usize = 0;
        let mut max_lsn: u64 = init_lsn;
        let mut max_txn_id: u64 = init_txn_id;

        let mut committed = Vec::new();
        let mut pending_txn_id: Option<u64> = None;
        let mut pending_ops: Vec<WalEntry> = Vec::new();

        while pos + 36 <= raw.len() {
            let Ok(rec_type) = WalRecordType::try_from(raw[pos]) else {
                break;
            };
            let len_bytes: [u8; 4] = raw[pos + 4..pos + 8]
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid WAL length slice"))?;
            let payload_len = u32::from_le_bytes(len_bytes) as usize;
            let total_len = 32 + payload_len + 4;

            if pos + total_len > raw.len() {
                break;
            }

            let crc_bytes: [u8; 4] = raw[pos + total_len - 4..pos + total_len]
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid WAL CRC slice"))?;
            let expected_crc = u32::from_le_bytes(crc_bytes);
            let actual_crc = crc32(&raw[pos..pos + total_len - 4]);
            if expected_crc != actual_crc {
                break;
            }

            let lsn = Self::read_u64(&raw[pos + 8..pos + 16])?;
            let txn_id = Self::read_u64(&raw[pos + 16..pos + 24])?;
            let target_id = Self::read_u64(&raw[pos + 24..pos + 32])?;
            let payload = &raw[pos + 32..pos + 32 + payload_len];

            match rec_type {
                WalRecordType::WriteSlot => {
                    if payload.len() != NODE_SLOT_SIZE {
                        break;
                    }
                    let mut slot_bytes = Box::new([0u8; NODE_SLOT_SIZE]);
                    slot_bytes.copy_from_slice(payload);
                    pending_txn_id = Some(txn_id);
                    pending_ops.push(WalEntry {
                        lsn,
                        txn_id,
                        op: WalOp::WriteSlot {
                            node_id: target_id,
                            slot_bytes,
                        },
                    });
                }
                WalRecordType::WriteHeap => {
                    pending_txn_id = Some(txn_id);
                    pending_ops.push(WalEntry {
                        lsn,
                        txn_id,
                        op: WalOp::WriteHeap {
                            offset: target_id,
                            data: payload.to_vec(),
                        },
                    });
                }
                WalRecordType::CommitTxn => {
                    if pending_txn_id == Some(txn_id) {
                        committed.append(&mut pending_ops);
                        pending_txn_id = None;
                        max_lsn = lsn;
                        max_txn_id = txn_id;
                        valid_pos = pos + total_len;
                    } else {
                        break;
                    }
                }
            }

            pos += total_len;
        }

        Ok((committed, max_lsn, max_txn_id, valid_pos as u64))
    }

    fn read_u64(slice: &[u8]) -> Result<u64, HdbError> {
        let arr: [u8; 8] = slice
            .try_into()
            .map_err(|_| HdbError::CorruptedData("Invalid u64 slice in WAL"))?;
        Ok(u64::from_le_bytes(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_rotation_and_purge() {
        let dir = std::env::temp_dir().join(format!("alice_wal_rot_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        // Small segment limit (300 bytes) to force rotation on every slot write
        let (mut wal, _) = WalWriter::open(&dir, 300).unwrap();

        let lsn1 = wal
            .append_transaction(&[WalOp::WriteSlot {
                node_id: 1,
                slot_bytes: Box::new([0x11; NODE_SLOT_SIZE]),
            }])
            .unwrap();
        assert_eq!(wal.current_segment_id(), 1);

        // Next txn triggers rotation to segment 2
        let lsn2 = wal
            .append_transaction(&[WalOp::WriteSlot {
                node_id: 2,
                slot_bytes: Box::new([0x22; NODE_SLOT_SIZE]),
            }])
            .unwrap();
        assert_eq!(wal.current_segment_id(), 2);
        assert!(dir.join("wal_00000001.log").exists());
        assert!(dir.join("wal_00000002.log").exists());

        // Purge segments up to lsn1: segment 1 must be deleted, segment 2 kept
        let removed = wal.purge_segments_up_to(lsn1).unwrap();
        assert_eq!(removed, 1);
        assert!(!dir.join("wal_00000001.log").exists());
        assert!(dir.join("wal_00000002.log").exists());
        assert!(lsn2 > lsn1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
