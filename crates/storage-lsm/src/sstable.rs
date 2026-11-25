use crate::block::{BlockCache, DATA_BLOCK_TARGET_BYTES, DataBlockBuilder, decode_data_block};
use crate::bloom::BloomFilter;
use common::{DbError, crc32};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const SST_MAGIC: &[u8; 4] = b"ASST";
const SST_VERSION: u16 = 2;
const FOOTER_SIZE: usize = 36;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryValue {
    Put(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone)]
pub struct BlockIndexEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
}

/// Immutable block-based Sorted String Table (SSTable) with prefix compression,
/// block index, Bloom filter, and key-range bounds for Leveled Compaction.
pub struct SsTableReader {
    pub id: u64,
    pub level: usize,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub file_size: u64,
    path: PathBuf,
    block_index: Vec<BlockIndexEntry>,
    bloom: BloomFilter,
}

impl SsTableReader {
    pub fn write_from_sorted<I>(
        path: impl AsRef<Path>,
        id: u64,
        level: usize,
        entries: I,
    ) -> Result<Option<Self>, DbError>
    where
        I: IntoIterator<Item = (Vec<u8>, EntryValue)>,
    {
        let items: Vec<(Vec<u8>, EntryValue)> = entries.into_iter().collect();
        if items.is_empty() {
            return Ok(None);
        }

        let min_key = items.first().unwrap().0.clone();
        let max_key = items.last().unwrap().0.clone();

        let mut bloom = BloomFilter::new(items.len(), 10);
        let mut block_index = Vec::new();
        let mut data_buf = Vec::new();

        let mut builder = DataBlockBuilder::new();
        let mut current_first_key: Option<Vec<u8>> = None;

        for (key, val) in &items {
            bloom.insert(key);
            if current_first_key.is_none() {
                current_first_key = Some(key.clone());
            }
            builder.add(key, val);

            if builder.current_size() >= DATA_BLOCK_TARGET_BYTES {
                let raw_block = builder.finish();
                let offset = data_buf.len() as u64;
                let len = raw_block.len() as u32;
                data_buf.extend_from_slice(&raw_block);
                block_index.push(BlockIndexEntry {
                    first_key: current_first_key.take().unwrap(),
                    offset,
                    len,
                });
                builder = DataBlockBuilder::new();
            }
        }

        if !builder.is_empty() {
            let raw_block = builder.finish();
            let offset = data_buf.len() as u64;
            let len = raw_block.len() as u32;
            data_buf.extend_from_slice(&raw_block);
            block_index.push(BlockIndexEntry {
                first_key: current_first_key.take().unwrap(),
                offset,
                len,
            });
        }

        let data_len = data_buf.len() as u64;
        let index_buf = encode_block_index(&block_index, &min_key, &max_key);
        let index_len = index_buf.len() as u64;
        let bloom_buf = bloom.encode();
        let bloom_len = bloom_buf.len() as u64;

        let mut file_buf =
            Vec::with_capacity(data_buf.len() + index_buf.len() + bloom_buf.len() + FOOTER_SIZE);
        file_buf.extend_from_slice(&data_buf);
        file_buf.extend_from_slice(&index_buf);
        file_buf.extend_from_slice(&bloom_buf);

        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&data_len.to_le_bytes());
        footer[8..16].copy_from_slice(&index_len.to_le_bytes());
        footer[16..24].copy_from_slice(&bloom_len.to_le_bytes());
        footer[24..26].copy_from_slice(&SST_VERSION.to_le_bytes());
        let checksum = crc32(&file_buf);
        footer[28..32].copy_from_slice(&checksum.to_le_bytes());
        footer[32..36].copy_from_slice(SST_MAGIC);
        file_buf.extend_from_slice(&footer);

        let file_size = file_buf.len() as u64;
        let path_ref = path.as_ref();
        let tmp_path = path_ref.with_extension("sst.tmp");
        {
            let mut f = File::create(&tmp_path)?;
            f.write_all(&file_buf)?;
            f.sync_data()?;
        }
        fs::rename(&tmp_path, path_ref)?;

        Ok(Some(Self {
            id,
            level,
            min_key,
            max_key,
            file_size,
            path: path_ref.to_path_buf(),
            block_index,
            bloom,
        }))
    }

    pub fn open(path: impl AsRef<Path>, id: u64, level: usize) -> Result<Self, DbError> {
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
        let (block_index, min_key, max_key) = decode_block_index(&bytes[index_start..bloom_start])?;
        let bloom = BloomFilter::decode(&bytes[bloom_start..footer_start])
            .ok_or(DbError::CorruptedData("Invalid Bloom filter in SSTable"))?;

        Ok(Self {
            id,
            level,
            min_key,
            max_key,
            file_size: bytes.len() as u64,
            path: path_ref.to_path_buf(),
            block_index,
            bloom,
        })
    }

    pub fn overlaps_range(&self, start: &[u8], end: &[u8]) -> bool {
        self.min_key.as_slice() <= end && self.max_key.as_slice() >= start
    }

    /// Point lookup utilizing key-range check, Bloom filter, block index binary search, and LRU BlockCache.
    pub fn get(
        &self,
        key: &[u8],
        cache: &Mutex<BlockCache>,
    ) -> Result<Option<EntryValue>, DbError> {
        if key < self.min_key.as_slice()
            || key > self.max_key.as_slice()
            || !self.bloom.may_contain(key)
            || self.block_index.is_empty()
        {
            return Ok(None);
        }

        let block_idx = match self
            .block_index
            .binary_search_by(|entry| entry.first_key.as_slice().cmp(key))
        {
            Ok(i) => i,
            Err(0) => return Ok(None),
            Err(i) => i - 1,
        };

        let entries = self.read_block(block_idx, cache)?;
        match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(idx) => Ok(Some(entries[idx].1.clone())),
            Err(_) => Ok(None),
        }
    }

    pub fn block_count(&self) -> usize {
        self.block_index.len()
    }

    pub fn read_block(
        &self,
        block_idx: usize,
        cache: &Mutex<BlockCache>,
    ) -> Result<Vec<(Vec<u8>, EntryValue)>, DbError> {
        let meta = &self.block_index[block_idx];
        if let Ok(mut guard) = cache.lock()
            && let Some(cached_raw) = guard.get(self.id, meta.offset)
        {
            return decode_data_block(&cached_raw);
        }

        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(meta.offset))?;
        let mut raw = vec![0u8; meta.len as usize];
        f.read_exact(&mut raw)?;

        if let Ok(mut guard) = cache.lock() {
            guard.insert(self.id, meta.offset, raw.clone());
        }
        decode_data_block(&raw)
    }

    pub fn read_all(&self) -> Result<Vec<(Vec<u8>, EntryValue)>, DbError> {
        if self.block_index.is_empty() {
            return Ok(Vec::new());
        }
        let mut f = File::open(&self.path)?;
        let mut out = Vec::new();
        for meta in &self.block_index {
            f.seek(SeekFrom::Start(meta.offset))?;
            let mut raw = vec![0u8; meta.len as usize];
            f.read_exact(&mut raw)?;
            out.extend(decode_data_block(&raw)?);
        }
        Ok(out)
    }
}

fn encode_block_index(entries: &[BlockIndexEntry], min_key: &[u8], max_key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(min_key);
    buf.extend_from_slice(&(max_key.len() as u32).to_le_bytes());
    buf.extend_from_slice(max_key);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        buf.extend_from_slice(&(e.first_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&e.first_key);
        buf.extend_from_slice(&e.offset.to_le_bytes());
        buf.extend_from_slice(&e.len.to_le_bytes());
    }
    buf
}

type DecodedIndex = (Vec<BlockIndexEntry>, Vec<u8>, Vec<u8>);

fn decode_block_index(buf: &[u8]) -> Result<DecodedIndex, DbError> {
    if buf.len() < 12 {
        return Err(DbError::CorruptedData("Truncated block index header"));
    }
    let min_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut cur = 4;
    if cur + min_len + 4 > buf.len() {
        return Err(DbError::CorruptedData("Truncated min_key in block index"));
    }
    let min_key = buf[cur..cur + min_len].to_vec();
    cur += min_len;

    let max_len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;
    if cur + max_len + 4 > buf.len() {
        return Err(DbError::CorruptedData("Truncated max_key in block index"));
    }
    let max_key = buf[cur..cur + max_len].to_vec();
    cur += max_len;

    let count = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
    cur += 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        if cur + 4 > buf.len() {
            return Err(DbError::CorruptedData("Truncated block index entry"));
        }
        let klen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + klen + 12 > buf.len() {
            return Err(DbError::CorruptedData("Truncated block index key/offset"));
        }
        let first_key = buf[cur..cur + klen].to_vec();
        cur += klen;
        let offset = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap());
        cur += 8;
        let len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap());
        cur += 4;
        out.push(BlockIndexEntry {
            first_key,
            offset,
            len,
        });
    }
    Ok((out, min_key, max_key))
}
