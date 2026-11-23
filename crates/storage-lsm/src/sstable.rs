use crate::bloom::BloomFilter;
use common::{DbError, crc32};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const SST_MAGIC: &[u8; 4] = b"ASST";
const SST_VERSION: u16 = 1;
const FOOTER_SIZE: usize = 36;
const SPARSE_INDEX_INTERVAL: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryValue {
    Put(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone)]
struct SparseIndexEntry {
    first_key: Vec<u8>,
    offset: u64,
}

/// Immutable on-disk Sorted String Table (SSTable) with sparse index and Bloom filter.
pub struct SsTableReader {
    pub id: u64,
    path: std::path::PathBuf,
    data_len: u64,
    sparse_index: Vec<SparseIndexEntry>,
    bloom: BloomFilter,
}

impl SsTableReader {
    /// Writes a sorted iterator of `(key, EntryValue)` into a new immutable SSTable file.
    pub fn write_from_sorted<I>(
        path: impl AsRef<Path>,
        id: u64,
        entries: I,
    ) -> Result<Self, DbError>
    where
        I: IntoIterator<Item = (Vec<u8>, EntryValue)>,
    {
        let items: Vec<(Vec<u8>, EntryValue)> = entries.into_iter().collect();
        let mut bloom = BloomFilter::new(items.len(), 10);
        let mut sparse_index = Vec::new();
        let mut data_buf = Vec::new();

        for (idx, (key, val)) in items.iter().enumerate() {
            bloom.insert(key);
            if idx % SPARSE_INDEX_INTERVAL == 0 {
                sparse_index.push(SparseIndexEntry {
                    first_key: key.clone(),
                    offset: data_buf.len() as u64,
                });
            }
            encode_entry(&mut data_buf, key, val);
        }

        let data_len = data_buf.len() as u64;
        let index_buf = encode_sparse_index(&sparse_index);
        let index_len = index_buf.len() as u64;
        let bloom_buf = bloom.encode();
        let bloom_len = bloom_buf.len() as u64;

        let mut file_buf =
            Vec::with_capacity(data_buf.len() + index_buf.len() + bloom_buf.len() + FOOTER_SIZE);
        file_buf.extend_from_slice(&data_buf);
        file_buf.extend_from_slice(&index_buf);
        file_buf.extend_from_slice(&bloom_buf);

        // Footer (36 bytes):
        // [data_len: 8B][index_len: 8B][bloom_len: 8B][version: 2B][reserved: 2B][crc32: 4B][magic: 4B]
        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&data_len.to_le_bytes());
        footer[8..16].copy_from_slice(&index_len.to_le_bytes());
        footer[16..24].copy_from_slice(&bloom_len.to_le_bytes());
        footer[24..26].copy_from_slice(&SST_VERSION.to_le_bytes());
        let checksum = crc32(&file_buf);
        footer[28..32].copy_from_slice(&checksum.to_le_bytes());
        footer[32..36].copy_from_slice(SST_MAGIC);
        file_buf.extend_from_slice(&footer);

        let path_ref = path.as_ref();
        let tmp_path = path_ref.with_extension("sst.tmp");
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&file_buf)?;
            f.sync_data()?;
        }
        fs::rename(&tmp_path, path_ref)?;

        Ok(Self {
            id,
            path: path_ref.to_path_buf(),
            data_len,
            sparse_index,
            bloom,
        })
    }

    pub fn open(path: impl AsRef<Path>, id: u64) -> Result<Self, DbError> {
        let path_ref = path.as_ref();
        let bytes = fs::read(path_ref)?;
        if bytes.len() < FOOTER_SIZE {
            return Err(DbError::CorruptedData("SSTable smaller than footer"));
        }

        let footer_start = bytes.len() - FOOTER_SIZE;
        let footer = &bytes[footer_start..];
        if &footer[32..36] != SST_MAGIC {
            return Err(DbError::InvalidMagic);
        }
        let ver = u16::from_le_bytes([footer[24], footer[25]]);
        if ver != SST_VERSION {
            return Err(DbError::UnsupportedVersion(ver));
        }

        let expected_crc = u32::from_le_bytes(footer[28..32].try_into().unwrap());
        let actual_crc = crc32(&bytes[..footer_start]);
        if expected_crc != actual_crc {
            return Err(DbError::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let data_len = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[16..24].try_into().unwrap());

        if data_len + index_len + bloom_len != footer_start as u64 {
            return Err(DbError::CorruptedData("SSTable section lengths mismatch"));
        }

        let index_start = data_len as usize;
        let bloom_start = index_start + index_len as usize;
        let sparse_index = decode_sparse_index(&bytes[index_start..bloom_start])?;
        let bloom = BloomFilter::decode(&bytes[bloom_start..footer_start])
            .ok_or(DbError::CorruptedData("Invalid Bloom filter in SSTable"))?;

        Ok(Self {
            id,
            path: path_ref.to_path_buf(),
            data_len,
            sparse_index,
            bloom,
        })
    }

    /// Point lookup using Bloom filter + sparse index binary search.
    pub fn get(&self, key: &[u8]) -> Result<Option<EntryValue>, DbError> {
        if !self.bloom.may_contain(key) || self.sparse_index.is_empty() {
            return Ok(None);
        }

        // Binary search sparse index for the largest block whose first_key <= key
        let block_idx = match self
            .sparse_index
            .binary_search_by(|entry| entry.first_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None),
            Err(i) => i - 1,
        };

        let start_offset = self.sparse_index[block_idx].offset;
        let end_offset = self
            .sparse_index
            .get(block_idx + 1)
            .map(|e| e.offset)
            .unwrap_or(self.data_len);

        let slice_len = (end_offset - start_offset) as usize;
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(start_offset))?;
        let mut buf = vec![0u8; slice_len];
        f.read_exact(&mut buf)?;

        let mut cur = 0;
        while cur < buf.len() {
            let (k, val, next_cur) = decode_entry(&buf, cur)?;
            match k.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(val)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => cur = next_cur,
            }
        }
        Ok(None)
    }

    /// Reads all entries in sorted order (used for range scans and compaction).
    pub fn read_all(&self) -> Result<Vec<(Vec<u8>, EntryValue)>, DbError> {
        let mut f = File::open(&self.path)?;
        let mut buf = vec![0u8; self.data_len as usize];
        f.read_exact(&mut buf)?;

        let mut out = Vec::new();
        let mut cur = 0;
        while cur < buf.len() {
            let (k, val, next_cur) = decode_entry(&buf, cur)?;
            out.push((k.to_vec(), val));
            cur = next_cur;
        }
        Ok(out)
    }
}

fn encode_entry(buf: &mut Vec<u8>, key: &[u8], val: &EntryValue) {
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    match val {
        EntryValue::Put(v) => {
            buf.push(0x01);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        EntryValue::Delete => {
            buf.push(0x02);
        }
    }
}

fn decode_entry(buf: &[u8], offset: usize) -> Result<(&[u8], EntryValue, usize), DbError> {
    if offset + 5 > buf.len() {
        return Err(DbError::CorruptedData("Truncated SSTable entry header"));
    }
    let key_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
    let key_end = offset + 4 + key_len;
    if key_end + 1 > buf.len() {
        return Err(DbError::CorruptedData("Truncated SSTable entry key"));
    }
    let key = &buf[offset + 4..key_end];
    let tag = buf[key_end];
    match tag {
        0x01 => {
            if key_end + 5 > buf.len() {
                return Err(DbError::CorruptedData("Truncated SSTable value length"));
            }
            let val_len =
                u32::from_le_bytes(buf[key_end + 1..key_end + 5].try_into().unwrap()) as usize;
            let val_end = key_end + 5 + val_len;
            if val_end > buf.len() {
                return Err(DbError::CorruptedData("Truncated SSTable value payload"));
            }
            Ok((
                key,
                EntryValue::Put(buf[key_end + 5..val_end].to_vec()),
                val_end,
            ))
        }
        0x02 => Ok((key, EntryValue::Delete, key_end + 1)),
        _ => Err(DbError::CorruptedData("Unknown SSTable entry tag")),
    }
}

fn encode_sparse_index(entries: &[SparseIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        buf.extend_from_slice(&(e.first_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.first_key);
        buf.extend_from_slice(&e.offset.to_le_bytes());
    }
    buf
}

fn decode_sparse_index(buf: &[u8]) -> Result<Vec<SparseIndexEntry>, DbError> {
    if buf.len() < 4 {
        return Err(DbError::CorruptedData("Truncated sparse index"));
    }
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut cur = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if cur + 4 > buf.len() {
            return Err(DbError::CorruptedData("Truncated sparse index entry"));
        }
        let klen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + klen + 8 > buf.len() {
            return Err(DbError::CorruptedData("Truncated sparse index key/offset"));
        }
        let first_key = buf[cur..cur + klen].to_vec();
        cur += klen;
        let offset = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap());
        cur += 8;
        out.push(SparseIndexEntry { first_key, offset });
    }
    Ok(out)
}
