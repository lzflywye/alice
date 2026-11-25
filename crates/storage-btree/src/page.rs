use common::{crc32, DbError};

pub const BTREE_PAGE_SIZE: usize = 4096;
const PAGE_MAGIC: &[u8; 4] = b"ABTR";
const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

pub type PageId = u64;
pub type KvEntry = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone)]
pub enum BTreeNode {
    Leaf {
        next_leaf: Option<PageId>,
        entries: Vec<KvEntry>,
    },
    Internal {
        /// `keys.len() + 1 == children.len()`
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        BTreeNode::Leaf {
            next_leaf: None,
            entries: Vec::new(),
        }
    }

    pub fn encoded_size(&self) -> usize {
        // Header (16B) + body + CRC32 (4B)
        let body = match self {
            BTreeNode::Leaf { entries, .. } => entries
                .iter()
                .map(|(k, v)| 8 + k.len() + v.len())
                .sum::<usize>(),
            BTreeNode::Internal { keys, children } => {
                children.len() * 8 + keys.iter().map(|k| 4 + k.len()).sum::<usize>()
            }
        };
        16 + body + 4
    }

    pub fn is_overflow(&self) -> bool {
        self.encoded_size() > BTREE_PAGE_SIZE
    }

    pub fn encode_page(&self) -> Result<[u8; BTREE_PAGE_SIZE], DbError> {
        let mut page = [0u8; BTREE_PAGE_SIZE];
        page[0..4].copy_from_slice(PAGE_MAGIC);

        let mut cur = 16;
        match self {
            BTreeNode::Leaf { next_leaf, entries } => {
                page[4] = PAGE_TYPE_LEAF;
                page[6..8].copy_from_slice(&(entries.len() as u16).to_le_bytes());
                page[8..16].copy_from_slice(&next_leaf.unwrap_or(0).to_le_bytes());

                for (k, v) in entries {
                    let needed = 8 + k.len() + v.len();
                    if cur + needed + 4 > BTREE_PAGE_SIZE {
                        return Err(DbError::CorruptedData("BTree leaf page overflow"));
                    }
                    page[cur..cur + 4].copy_from_slice(&(k.len() as u32).to_le_bytes());
                    cur += 4;
                    page[cur..cur + k.len()].copy_from_slice(k);
                    cur += k.len();
                    page[cur..cur + 4].copy_from_slice(&(v.len() as u32).to_le_bytes());
                    cur += 4;
                    page[cur..cur + v.len()].copy_from_slice(v);
                    cur += v.len();
                }
            }
            BTreeNode::Internal { keys, children } => {
                if children.len() != keys.len() + 1 {
                    return Err(DbError::CorruptedData("Invalid BTree internal arity"));
                }
                page[4] = PAGE_TYPE_INTERNAL;
                page[6..8].copy_from_slice(&(keys.len() as u16).to_le_bytes());
                page[8..16].copy_from_slice(&children[0].to_le_bytes());

                for (key, &child) in keys.iter().zip(children.iter().skip(1)) {
                    let needed = 4 + key.len() + 8;
                    if cur + needed + 4 > BTREE_PAGE_SIZE {
                        return Err(DbError::CorruptedData("BTree internal page overflow"));
                    }
                    page[cur..cur + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
                    cur += 4;
                    page[cur..cur + key.len()].copy_from_slice(key);
                    cur += key.len();
                    page[cur..cur + 8].copy_from_slice(&child.to_le_bytes());
                    cur += 8;
                }
            }
        }

        let checksum = crc32(&page[..BTREE_PAGE_SIZE - 4]);
        page[BTREE_PAGE_SIZE - 4..BTREE_PAGE_SIZE].copy_from_slice(&checksum.to_le_bytes());
        Ok(page)
    }

    pub fn decode_page(page: &[u8; BTREE_PAGE_SIZE]) -> Result<Self, DbError> {
        if &page[0..4] != PAGE_MAGIC {
            return Err(DbError::InvalidMagic);
        }
        let expected_crc = u32::from_le_bytes(
            page[BTREE_PAGE_SIZE - 4..BTREE_PAGE_SIZE]
                .try_into()
                .unwrap(),
        );
        let actual_crc = crc32(&page[..BTREE_PAGE_SIZE - 4]);
        if expected_crc != actual_crc {
            return Err(DbError::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        let page_type = page[4];
        let count = u16::from_le_bytes([page[6], page[7]]) as usize;
        let aux = u64::from_le_bytes(page[8..16].try_into().unwrap());
        let limit = BTREE_PAGE_SIZE - 4;
        let mut cur = 16;

        match page_type {
            PAGE_TYPE_LEAF => {
                let next_leaf = if aux == 0 { None } else { Some(aux) };
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    if cur + 4 > limit {
                        return Err(DbError::CorruptedData("Truncated leaf key len"));
                    }
                    let klen = u32::from_le_bytes(page[cur..cur + 4].try_into().unwrap()) as usize;
                    cur += 4;
                    if cur + klen + 4 > limit {
                        return Err(DbError::CorruptedData("Truncated leaf key/val len"));
                    }
                    let k = page[cur..cur + klen].to_vec();
                    cur += klen;
                    let vlen = u32::from_le_bytes(page[cur..cur + 4].try_into().unwrap()) as usize;
                    cur += 4;
                    if cur + vlen > limit {
                        return Err(DbError::CorruptedData("Truncated leaf val"));
                    }
                    let v = page[cur..cur + vlen].to_vec();
                    cur += vlen;
                    entries.push((k, v));
                }
                Ok(BTreeNode::Leaf { next_leaf, entries })
            }
            PAGE_TYPE_INTERNAL => {
                let mut keys = Vec::with_capacity(count);
                let mut children = Vec::with_capacity(count + 1);
                children.push(aux);

                for _ in 0..count {
                    if cur + 4 > limit {
                        return Err(DbError::CorruptedData("Truncated internal key len"));
                    }
                    let klen = u32::from_le_bytes(page[cur..cur + 4].try_into().unwrap()) as usize;
                    cur += 4;
                    if cur + klen + 8 > limit {
                        return Err(DbError::CorruptedData("Truncated internal key/child"));
                    }
                    let k = page[cur..cur + klen].to_vec();
                    cur += klen;
                    let child = u64::from_le_bytes(page[cur..cur + 8].try_into().unwrap());
                    cur += 8;
                    keys.push(k);
                    children.push(child);
                }
                Ok(BTreeNode::Internal { keys, children })
            }
            _ => Err(DbError::CorruptedData("Unknown BTree page type")),
        }
    }
}
