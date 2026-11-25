pub mod page;

use common::DbError;
use page::{BTREE_PAGE_SIZE, BTreeNode, KvEntry, PageId};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};
use wal::{DEFAULT_WAL_SEGMENT_BYTES, WalManager};

const META_MAGIC: &[u8; 4] = b"ABTM";

struct BTreeInner {
    file: File,
    root_page: PageId,
    next_page_id: PageId,
    /// In-memory page cache for fast traversal
    page_cache: HashMap<PageId, BTreeNode>,
}

/// Persistent 4KB-page B+Tree storage engine with leaf sibling links for range scans
/// and WAL-backed crash recovery.
pub struct BTreeEngine {
    meta_path: PathBuf,
    inner: RwLock<BTreeInner>,
    wal: Mutex<WalManager>,
    writer: Mutex<()>,
}

impl BTreeEngine {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_wal_size(dir, DEFAULT_WAL_SEGMENT_BYTES)
    }

    pub fn open_with_wal_size(
        dir: impl AsRef<Path>,
        wal_segment_bytes: u64,
    ) -> Result<Self, DbError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let data_path = dir.join("btree.db");
        let meta_path = dir.join("btree.meta");

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&data_path)?;

        let (mut root_page, mut next_page_id) = if meta_path.exists() {
            read_meta(&meta_path)?
        } else {
            (1, 2)
        };

        let mut page_cache = HashMap::new();
        if file.metadata()?.len() == 0 {
            let root = BTreeNode::new_leaf();
            write_page_to_disk(&mut file, root_page, &root)?;
            file.sync_data()?;
            write_meta(&meta_path, root_page, next_page_id)?;
            page_cache.insert(root_page, root);
        }

        let (wal_mgr, records) = WalManager::open(dir.join("wal"), wal_segment_bytes)?;
        let mut inner = BTreeInner {
            file,
            root_page,
            next_page_id,
            page_cache,
        };

        // Replay WAL mutations on recovery
        for rec in records {
            match decode_wal_op(&rec.payload)? {
                BTreeWalOp::Put { key, value } => {
                    insert_into_tree(&mut inner, &meta_path, key, value)?;
                }
                BTreeWalOp::Delete { key } => {
                    delete_from_tree(&mut inner, &key)?;
                }
            }
        }
        root_page = inner.root_page;
        next_page_id = inner.next_page_id;
        let _ = (root_page, next_page_id);

        Ok(Self {
            meta_path,
            inner: RwLock::new(inner),
            wal: Mutex::new(wal_mgr),
            writer: Mutex::new(()),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DbError> {
        if key.len() > 512 || value.len() > 2048 {
            return Err(DbError::KeyTooLarge {
                max: 512,
                actual: key.len(),
            });
        }
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let wal_bytes = encode_wal_put(key, value);
        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            wal.append(wal_bytes)?;
        }
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        insert_into_tree(&mut inner, &self.meta_path, key.to_vec(), value.to_vec())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let wal_bytes = encode_wal_delete(key);
        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            wal.append(wal_bytes)?;
        }
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        delete_from_tree(&mut inner, key)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DbError> {
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        let mut cur_id = inner.root_page;

        loop {
            let node = load_node(&mut inner, cur_id)?;
            match node {
                BTreeNode::Internal { keys, children } => {
                    let idx = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    cur_id = children[idx];
                }
                BTreeNode::Leaf { entries, .. } => {
                    return Ok(entries
                        .binary_search_by(|(k, _)| k.as_slice().cmp(key))
                        .ok()
                        .map(|idx| entries[idx].1.clone()));
                }
            }
        }
    }

    /// Scans all entries in `[start_inclusive, end_exclusive)` by traversing leaf sibling links.
    /// Pass `None` for open-ended bounds.
    pub fn scan_range(
        &self,
        start_inclusive: Option<&[u8]>,
        end_exclusive: Option<&[u8]>,
    ) -> Result<Vec<KvEntry>, DbError> {
        let mut inner = self.inner.write().map_err(|_| DbError::LockPoisoned)?;
        let mut cur_id = inner.root_page;

        // Descend to the starting leaf page
        loop {
            let node = load_node(&mut inner, cur_id)?;
            match node {
                BTreeNode::Internal { keys, children } => {
                    let idx = match start_inclusive {
                        Some(start) => match keys.binary_search_by(|k| k.as_slice().cmp(start)) {
                            Ok(i) => i + 1,
                            Err(i) => i,
                        },
                        None => 0,
                    };
                    cur_id = children[idx];
                }
                BTreeNode::Leaf { .. } => break,
            }
        }

        // Walk leaf sibling chain
        let mut out = Vec::new();
        let mut leaf_opt = Some(cur_id);
        while let Some(lid) = leaf_opt {
            let node = load_node(&mut inner, lid)?;
            if let BTreeNode::Leaf { next_leaf, entries } = node {
                for (k, v) in entries {
                    if let Some(start) = start_inclusive
                        && k.as_slice() < start
                    {
                        continue;
                    }
                    if let Some(end) = end_exclusive
                        && k.as_slice() >= end
                    {
                        return Ok(out);
                    }
                    out.push((k, v));
                }
                leaf_opt = next_leaf;
            } else {
                break;
            }
        }

        Ok(out)
    }
}

fn insert_into_tree(
    inner: &mut BTreeInner,
    meta_path: &Path,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), DbError> {
    let root_id = inner.root_page;
    if let Some((promoted_key, right_child)) = insert_recursive(inner, root_id, key, value)? {
        let new_root_id = inner.next_page_id;
        inner.next_page_id += 1;

        let new_root = BTreeNode::Internal {
            keys: vec![promoted_key],
            children: vec![root_id, right_child],
        };
        persist_node(inner, new_root_id, new_root)?;
        inner.root_page = new_root_id;
        write_meta(meta_path, inner.root_page, inner.next_page_id)?;
    }
    inner.file.sync_data()?;
    Ok(())
}

fn insert_recursive(
    inner: &mut BTreeInner,
    page_id: PageId,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<Option<(Vec<u8>, PageId)>, DbError> {
    let node = load_node(inner, page_id)?;
    match node {
        BTreeNode::Leaf {
            next_leaf,
            mut entries,
        } => {
            match entries.binary_search_by(|(k, _)| k.cmp(&key)) {
                Ok(idx) => entries[idx].1 = value,
                Err(idx) => entries.insert(idx, (key, value)),
            }

            let updated = BTreeNode::Leaf { next_leaf, entries };
            if !updated.is_overflow() {
                persist_node(inner, page_id, updated)?;
                return Ok(None);
            }

            // Split leaf page into left (existing `page_id`) and right (`new_leaf_id`)
            let BTreeNode::Leaf {
                next_leaf,
                mut entries,
            } = updated
            else {
                unreachable!()
            };
            let mid = entries.len() / 2;
            let right_entries = entries.split_off(mid);
            let split_key = right_entries[0].0.clone();

            let right_id = inner.next_page_id;
            inner.next_page_id += 1;

            let left_node = BTreeNode::Leaf {
                next_leaf: Some(right_id),
                entries,
            };
            let right_node = BTreeNode::Leaf {
                next_leaf,
                entries: right_entries,
            };

            persist_node(inner, page_id, left_node)?;
            persist_node(inner, right_id, right_node)?;
            Ok(Some((split_key, right_id)))
        }
        BTreeNode::Internal {
            mut keys,
            mut children,
        } => {
            let idx = match keys.binary_search(&key) {
                Ok(i) => i + 1,
                Err(i) => i,
            };
            let child_id = children[idx];
            if let Some((promoted_key, new_child_id)) =
                insert_recursive(inner, child_id, key, value)?
            {
                keys.insert(idx, promoted_key);
                children.insert(idx + 1, new_child_id);

                let updated = BTreeNode::Internal { keys, children };
                if !updated.is_overflow() {
                    persist_node(inner, page_id, updated)?;
                    return Ok(None);
                }

                // Split internal node and push middle key up to parent
                let BTreeNode::Internal {
                    mut keys,
                    mut children,
                } = updated
                else {
                    unreachable!()
                };
                let mid = keys.len() / 2;
                let push_up_key = keys[mid].clone();
                let right_keys = keys.split_off(mid + 1);
                keys.pop(); // Remove `push_up_key` from left node
                let right_children = children.split_off(mid + 1);

                let right_id = inner.next_page_id;
                inner.next_page_id += 1;

                persist_node(inner, page_id, BTreeNode::Internal { keys, children })?;
                persist_node(
                    inner,
                    right_id,
                    BTreeNode::Internal {
                        keys: right_keys,
                        children: right_children,
                    },
                )?;
                return Ok(Some((push_up_key, right_id)));
            }
            Ok(None)
        }
    }
}

fn delete_from_tree(inner: &mut BTreeInner, key: &[u8]) -> Result<(), DbError> {
    let mut cur_id = inner.root_page;
    loop {
        let node = load_node(inner, cur_id)?;
        match node {
            BTreeNode::Internal { keys, children } => {
                let idx = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };
                cur_id = children[idx];
            }
            BTreeNode::Leaf {
                next_leaf,
                mut entries,
            } => {
                if let Ok(idx) = entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                    entries.remove(idx);
                    persist_node(inner, cur_id, BTreeNode::Leaf { next_leaf, entries })?;
                    inner.file.sync_data()?;
                }
                return Ok(());
            }
        }
    }
}

fn load_node(inner: &mut BTreeInner, page_id: PageId) -> Result<BTreeNode, DbError> {
    if let Some(cached) = inner.page_cache.get(&page_id) {
        return Ok(cached.clone());
    }
    let offset = page_id * (BTREE_PAGE_SIZE as u64);
    inner.file.seek(SeekFrom::Start(offset))?;
    let mut buf = [0u8; BTREE_PAGE_SIZE];
    inner.file.read_exact(&mut buf)?;
    let node = BTreeNode::decode_page(&buf)?;
    inner.page_cache.insert(page_id, node.clone());
    Ok(node)
}

fn persist_node(inner: &mut BTreeInner, page_id: PageId, node: BTreeNode) -> Result<(), DbError> {
    write_page_to_disk(&mut inner.file, page_id, &node)?;
    inner.page_cache.insert(page_id, node);
    Ok(())
}

fn write_page_to_disk(file: &mut File, page_id: PageId, node: &BTreeNode) -> Result<(), DbError> {
    let encoded = node.encode_page()?;
    let offset = page_id * (BTREE_PAGE_SIZE as u64);
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&encoded)?;
    Ok(())
}

fn write_meta(path: &Path, root_page: PageId, next_page_id: PageId) -> Result<(), DbError> {
    let mut buf = [0u8; 20];
    buf[0..4].copy_from_slice(META_MAGIC);
    buf[4..12].copy_from_slice(&root_page.to_le_bytes());
    buf[12..20].copy_from_slice(&next_page_id.to_le_bytes());
    let tmp = path.with_extension("meta.tmp");
    fs::write(&tmp, buf)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn read_meta(path: &Path) -> Result<(PageId, PageId), DbError> {
    let buf = fs::read(path)?;
    if buf.len() != 20 || &buf[0..4] != META_MAGIC {
        return Err(DbError::InvalidMagic);
    }
    let root = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let next = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    Ok((root, next))
}

enum BTreeWalOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

fn encode_wal_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + key.len() + value.len());
    out.push(0x01);
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    out
}

fn encode_wal_delete(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + key.len());
    out.push(0x02);
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out
}

fn decode_wal_op(buf: &[u8]) -> Result<BTreeWalOp, DbError> {
    if buf.len() < 5 {
        return Err(DbError::CorruptedData("Truncated BTree WAL record"));
    }
    let tag = buf[0];
    let klen = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
    if buf.len() < 5 + klen {
        return Err(DbError::CorruptedData("Truncated BTree WAL key"));
    }
    let key = buf[5..5 + klen].to_vec();
    match tag {
        0x01 => {
            let voff = 5 + klen;
            if buf.len() < voff + 4 {
                return Err(DbError::CorruptedData("Truncated BTree WAL val len"));
            }
            let vlen = u32::from_le_bytes(buf[voff..voff + 4].try_into().unwrap()) as usize;
            if buf.len() < voff + 4 + vlen {
                return Err(DbError::CorruptedData("Truncated BTree WAL val"));
            }
            Ok(BTreeWalOp::Put {
                key,
                value: buf[voff + 4..voff + 4 + vlen].to_vec(),
            })
        }
        0x02 => Ok(BTreeWalOp::Delete { key }),
        _ => Err(DbError::CorruptedData("Unknown BTree WAL tag")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_multi_level_splits_range_scan_and_recovery() {
        let dir = std::env::temp_dir().join(format!("btree_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            let btree = BTreeEngine::open(&dir).unwrap();
            // Insert 300 entries to trigger multiple leaf & internal page splits across 4KB pages
            for i in 0..300u32 {
                let key = format!("pk:{i:05}");
                let val = format!("row_tuple_payload_{i:05}_padding_for_split");
                btree.put(key.as_bytes(), val.as_bytes()).unwrap();
            }

            btree.put(b"pk:00050", b"updated_row_50").unwrap();
            btree.delete(b"pk:00051").unwrap();

            assert_eq!(
                btree.get(b"pk:00050").unwrap(),
                Some(b"updated_row_50".to_vec())
            );
            assert_eq!(btree.get(b"pk:00051").unwrap(), None);

            // Range scan across multiple split leaf pages
            let range = btree
                .scan_range(Some(b"pk:00048"), Some(b"pk:00055"))
                .unwrap();
            // 00048, 00049, 00050, 00052, 00053, 00054 -> 6 rows
            assert_eq!(range.len(), 6);
            assert_eq!(range[0].0, b"pk:00048");
            assert_eq!(range[5].0, b"pk:00054");
        }

        // Reopen and verify multi-page tree persisted cleanly
        {
            let btree = BTreeEngine::open(&dir).unwrap();
            assert_eq!(
                btree.get(b"pk:00050").unwrap(),
                Some(b"updated_row_50".to_vec())
            );
            assert_eq!(btree.get(b"pk:00051").unwrap(), None);
            assert_eq!(
                btree.get(b"pk:00299").unwrap(),
                Some(b"row_tuple_payload_00299_padding_for_split".to_vec())
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
