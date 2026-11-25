use crate::sstable::EntryValue;
use common::DbError;
use std::collections::HashMap;

pub const DATA_BLOCK_TARGET_BYTES: usize = 4096;
pub const DEFAULT_BLOCK_CACHE_CAPACITY: usize = 256; // 256 * 4KB = 1 MB

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockCacheStats {
    pub capacity_blocks: usize,
    pub cached_blocks: usize,
    pub hits: u64,
    pub misses: u64,
}

struct CacheEntry {
    data: Vec<u8>,
    last_used: u64,
}

/// Thread-unsafe inner LRU cache keyed by `(sst_id, block_offset)`.
pub struct BlockCache {
    capacity: usize,
    entries: HashMap<(u64, u64), CacheEntry>,
    tick: u64,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    pub fn new(capacity_blocks: usize) -> Self {
        Self {
            capacity: capacity_blocks.max(1),
            entries: HashMap::with_capacity(capacity_blocks.max(1)),
            tick: 0,
            hits: 0,
            misses: 0,
        }
    }

    pub fn get(&mut self, sst_id: u64, offset: u64) -> Option<Vec<u8>> {
        self.tick += 1;
        let now = self.tick;
        if let Some(entry) = self.entries.get_mut(&(sst_id, offset)) {
            entry.last_used = now;
            self.hits += 1;
            return Some(entry.data.clone());
        }
        self.misses += 1;
        None
    }

    pub fn insert(&mut self, sst_id: u64, offset: u64, data: Vec<u8>) {
        self.tick += 1;
        let now = self.tick;
        if self.entries.len() >= self.capacity
            && !self.entries.contains_key(&(sst_id, offset))
            && let Some(victim_key) = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k)
        {
            self.entries.remove(&victim_key);
        }
        self.entries.insert(
            (sst_id, offset),
            CacheEntry {
                data,
                last_used: now,
            },
        );
    }

    pub fn evict_sst(&mut self, sst_id: u64) {
        self.entries.retain(|(id, _), _| *id != sst_id);
    }

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            capacity_blocks: self.capacity,
            cached_blocks: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
        }
    }
}

/// Builds a prefix-compressed Data Block from sorted `(key, EntryValue)` pairs.
#[derive(Default)]
pub struct DataBlockBuilder {
    buf: Vec<u8>,
    last_key: Vec<u8>,
    entry_count: usize,
}

impl DataBlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn current_size(&self) -> usize {
        self.buf.len()
    }

    /// Appends a sorted key-value entry using prefix compression against `last_key`.
    pub fn add(&mut self, key: &[u8], val: &EntryValue) {
        let shared_len = common_prefix_len(&self.last_key, key);
        let unshared = &key[shared_len..];

        // Record layout:
        // [shared_len: u16][unshared_len: u16][unshared_key][tag: u8][val_len: u32 (if Put)][val]
        self.buf
            .extend_from_slice(&(shared_len as u16).to_le_bytes());
        self.buf
            .extend_from_slice(&(unshared.len() as u16).to_le_bytes());
        self.buf.extend_from_slice(unshared);

        match val {
            EntryValue::Put(v) => {
                self.buf.push(0x01);
                self.buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                self.buf.extend_from_slice(v);
            }
            EntryValue::Delete => {
                self.buf.push(0x02);
            }
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Decodes all prefix-compressed entries inside a raw Data Block.
pub fn decode_data_block(buf: &[u8]) -> Result<Vec<(Vec<u8>, EntryValue)>, DbError> {
    let mut out = Vec::new();
    let mut prev_key = Vec::new();
    let mut cur = 0;

    while cur < buf.len() {
        if cur + 5 > buf.len() {
            return Err(DbError::CorruptedData(
                "Truncated prefix-compressed block header",
            ));
        }
        let shared_len = u16::from_le_bytes(buf[cur..cur + 2].try_into().unwrap()) as usize;
        let unshared_len = u16::from_le_bytes(buf[cur + 2..cur + 4].try_into().unwrap()) as usize;
        cur += 4;

        if shared_len > prev_key.len() || cur + unshared_len + 1 > buf.len() {
            return Err(DbError::CorruptedData(
                "Invalid prefix-compressed key bounds",
            ));
        }

        let mut full_key = Vec::with_capacity(shared_len + unshared_len);
        full_key.extend_from_slice(&prev_key[..shared_len]);
        full_key.extend_from_slice(&buf[cur..cur + unshared_len]);
        cur += unshared_len;

        let tag = buf[cur];
        cur += 1;

        let val = match tag {
            0x01 => {
                if cur + 4 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated block value length"));
                }
                let vlen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                if cur + vlen > buf.len() {
                    return Err(DbError::CorruptedData("Truncated block value payload"));
                }
                let v = buf[cur..cur + vlen].to_vec();
                cur += vlen;
                EntryValue::Put(v)
            }
            0x02 => EntryValue::Delete,
            _ => return Err(DbError::CorruptedData("Unknown block entry tag")),
        };

        prev_key = full_key.clone();
        out.push((full_key, val));
    }

    Ok(out)
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}
