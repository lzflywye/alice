use crate::catalog::CatalogStorage;
use crate::error::HdbError;
use crate::index::SecondaryIndex;
use crate::node::{INLINE_PAYLOAD_CAP, NodeSlot};
use crate::record::{HeapReader, StagedHeapAppender, decode_record, encode_record};
use crate::schema::{SegmentSchema, Value};
use crate::storage::DiskStorage;
use crate::wal::{WalEntry, WalOp, WalWriter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

const DEFAULT_WAL_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPLICATION_BACKLOG: usize = 10_000;

/// Result item returned by rich hierarchy read queries.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeView {
    pub node_id: u64,
    pub parent_id: u64,
    pub depth: usize,
    pub node_type: u16,
    pub values: Vec<Value>,
}

/// Summary metrics returned after a `vacuum()` compaction run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VacuumStats {
    pub active_nodes: u64,
    pub old_slot_capacity: u64,
    pub old_heap_bytes: u64,
    pub new_heap_bytes: u64,
}

struct WriteState {
    wal: WalWriter,
    free_list: Vec<u64>,
    next_slot_id: u64,
    heap_tail: u64,
    is_replica: bool,
    applied_lsn: u64,
    checkpointed_lsn: u64,
    replicated_lsn: u64,
    has_standby: bool,
    repl_backlog: VecDeque<WalEntry>,
}

/// Point-in-time consistent physical snapshot for Standby base backup initialization.
#[derive(Debug, Clone)]
pub struct BaseSnapshot {
    pub snapshot_lsn: u64,
    pub schemas: Vec<SegmentSchema>,
    pub nodes_bytes: Vec<u8>,
    pub heap_bytes: Vec<u8>,
}

/// Persistent Hierarchical Database with single-writer serialization, segmented WAL,
/// catalog persistence, secondary indexes, compaction, and Active-Standby replication.
pub struct Database {
    dir: PathBuf,
    schemas: RwLock<HashMap<u16, SegmentSchema>>,
    writer: Mutex<WriteState>,
    slot_cache: RwLock<Vec<NodeSlot>>,
    storage: RwLock<DiskStorage>,
    index: RwLock<SecondaryIndex>,
}

impl HeapReader for DiskStorage {
    fn read_bytes(&self, offset: u64, len: u32) -> Result<Vec<u8>, HdbError> {
        self.read_heap(offset, len)
    }
}

impl Database {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, HdbError> {
        Self::open_with_segment_size(dir, DEFAULT_WAL_SEGMENT_BYTES)
    }

    pub fn open_with_segment_size(
        dir: impl AsRef<Path>,
        segment_max_bytes: u64,
    ) -> Result<Self, HdbError> {
        let dir = dir.as_ref().to_path_buf();
        let storage = DiskStorage::open(&dir)?;
        let schemas = CatalogStorage::load(&dir.join("catalog.db"))?;
        let (wal, committed_entries) = WalWriter::open(&dir, segment_max_bytes)?;

        // 1. Load existing slots from nodes.db
        let count = storage.node_count()?;
        let mut slots = Vec::with_capacity(count as usize);
        for id in 0..count {
            slots.push(storage.read_slot(id)?);
        }

        // 2. Replay committed WAL records (Crash Recovery)
        let had_wal_entries = !committed_entries.is_empty();
        for entry in committed_entries {
            match entry.op {
                WalOp::WriteSlot {
                    node_id,
                    slot_bytes,
                } => {
                    let slot = NodeSlot::from_bytes(&slot_bytes);
                    let idx = node_id as usize;
                    if idx >= slots.len() {
                        slots.resize(idx + 1, NodeSlot::empty());
                    }
                    slots[idx] = slot;
                    storage.write_slot(node_id, &slot)?;
                }
                WalOp::WriteHeap { offset, data } => {
                    storage.write_heap_at(offset, &data)?;
                }
            }
        }
        if had_wal_entries {
            storage.sync_all()?;
        }

        // 3. Rebuild free list & secondary indexes from active slots (skipping NodeId 0)
        let mut free_list = Vec::new();
        let mut index = SecondaryIndex::new();
        for (id, slot) in slots.iter().enumerate().skip(1) {
            if !slot.is_active() {
                free_list.push(id as u64);
            } else if let Some(schema) = schemas.get(&slot.node_type) {
                let values = decode_record(schema, &slot.inline_data, &storage)?;
                index.insert_node(schema, id as u64, &values);
            }
        }

        let next_slot_id = slots.len() as u64;
        let heap_tail = storage.heap_len()?;
        let last_lsn = wal.next_lsn().saturating_sub(1);

        Ok(Self {
            dir,
            schemas: RwLock::new(schemas),
            writer: Mutex::new(WriteState {
                wal,
                free_list,
                next_slot_id,
                heap_tail,
                is_replica: false,
                applied_lsn: last_lsn,
                checkpointed_lsn: last_lsn,
                replicated_lsn: 0,
                has_standby: false,
                repl_backlog: VecDeque::new(),
            }),
            slot_cache: RwLock::new(slots),
            storage: RwLock::new(storage),
            index: RwLock::new(index),
        })
    }

    pub fn define_segment(&self, schema: SegmentSchema) -> Result<(), HdbError> {
        let mut schemas = self.schemas.write().map_err(|_| HdbError::LockPoisoned)?;
        if schemas.contains_key(&schema.node_type) {
            return Err(HdbError::SchemaAlreadyExists(schema.node_type));
        }
        schema.validate(INLINE_PAYLOAD_CAP)?;
        schemas.insert(schema.node_type, schema);
        CatalogStorage::save(&self.dir.join("catalog.db"), &schemas)?;
        Ok(())
    }

    pub fn insert(
        &self,
        parent_id: u64,
        node_type: u16,
        values: &[Value],
    ) -> Result<u64, HdbError> {
        let schema = {
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            schemas
                .get(&node_type)
                .ok_or(HdbError::SchemaNotFound(node_type))?
                .clone()
        };

        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let (parent_slot, old_first_child_slot) = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let actual_parent_type = if parent_id == 0 {
                None
            } else {
                let p = Self::get_slot_from_cache(&cache, parent_id)?;
                Some(p.node_type)
            };
            if schema.parent_type != actual_parent_type {
                return Err(HdbError::InvalidParentType {
                    expected: schema.parent_type,
                    actual: actual_parent_type,
                });
            }

            let p_slot = if parent_id != 0 {
                Some(cache[parent_id as usize])
            } else {
                None
            };
            let first_child_id = p_slot.map(|p| p.first_child_id).unwrap_or(0);
            let fc_slot = if first_child_id != 0 {
                Some(cache[first_child_id as usize])
            } else {
                None
            };
            (p_slot, fc_slot)
        };

        // Validate unique constraints before allocating slots or appending heap
        {
            let index = self.index.read().map_err(|_| HdbError::LockPoisoned)?;
            index.check_unique(&schema, values, None)?;
        }

        let mut heap_appender = StagedHeapAppender::new(w.heap_tail);
        let (inline_data, total_len) = encode_record(&schema, values, &mut heap_appender)?;

        let new_id = if let Some(id) = w.free_list.pop() {
            id
        } else {
            let id = w.next_slot_id;
            w.next_slot_id += 1;
            id
        };

        let old_first_child_id = parent_slot.map(|p| p.first_child_id).unwrap_or(0);

        let mut new_slot = NodeSlot::empty();
        new_slot.flags = NodeSlot::FLAG_ACTIVE;
        new_slot.node_type = node_type;
        new_slot.total_payload_len = total_len;
        new_slot.parent_id = parent_id;
        new_slot.next_sibling_id = old_first_child_id;
        new_slot.prev_sibling_id = 0;
        new_slot.inline_data = inline_data;

        let mut wal_ops = Vec::new();
        if !heap_appender.staged_data.is_empty() {
            wal_ops.push(WalOp::WriteHeap {
                offset: heap_appender.start_offset(),
                data: heap_appender.staged_data.clone(),
            });
        }
        wal_ops.push(WalOp::WriteSlot {
            node_id: new_id,
            slot_bytes: Box::new(new_slot.to_bytes()),
        });

        let updated_fc = old_first_child_slot.map(|mut fc| {
            fc.prev_sibling_id = new_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: old_first_child_id,
                slot_bytes: Box::new(fc.to_bytes()),
            });
            fc
        });

        let updated_parent = parent_slot.map(|mut p| {
            p.first_child_id = new_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: parent_id,
                slot_bytes: Box::new(p.to_bytes()),
            });
            p
        });

        let commit_lsn = w.wal.append_transaction(&wal_ops)?;
        Self::push_to_backlog(&mut w, commit_lsn, &wal_ops);

        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            if !heap_appender.staged_data.is_empty() {
                storage.write_heap_at(heap_appender.start_offset(), &heap_appender.staged_data)?;
                w.heap_tail += heap_appender.staged_data.len() as u64;
            }
            storage.write_slot(new_id, &new_slot)?;
            if let Some(fc) = updated_fc {
                storage.write_slot(old_first_child_id, &fc)?;
            }
            if let Some(p) = updated_parent {
                storage.write_slot(parent_id, &p)?;
            }
        }

        {
            let mut cache = self
                .slot_cache
                .write()
                .map_err(|_| HdbError::LockPoisoned)?;
            let idx = new_id as usize;
            if idx >= cache.len() {
                cache.resize(idx + 1, NodeSlot::empty());
            }
            cache[idx] = new_slot;
            if let Some(fc) = updated_fc {
                cache[old_first_child_id as usize] = fc;
            }
            if let Some(p) = updated_parent {
                cache[parent_id as usize] = p;
            }
        }

        {
            let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;
            index.insert_node(&schema, new_id, values);
        }

        Ok(new_id)
    }

    pub fn update(&self, node_id: u64, values: &[Value]) -> Result<(), HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let mut slot = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            *Self::get_slot_from_cache(&cache, node_id)?
        };

        let schema = {
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            schemas
                .get(&slot.node_type)
                .ok_or(HdbError::SchemaNotFound(slot.node_type))?
                .clone()
        };

        let old_values = {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            decode_record(&schema, &slot.inline_data, &*storage)?
        };
        {
            let index = self.index.read().map_err(|_| HdbError::LockPoisoned)?;
            index.check_unique(&schema, values, Some(node_id))?;
        }

        let mut heap_appender = StagedHeapAppender::new(w.heap_tail);
        let (inline_data, total_len) = encode_record(&schema, values, &mut heap_appender)?;

        slot.inline_data = inline_data;
        slot.total_payload_len = total_len;

        let mut wal_ops = Vec::new();
        if !heap_appender.staged_data.is_empty() {
            wal_ops.push(WalOp::WriteHeap {
                offset: heap_appender.start_offset(),
                data: heap_appender.staged_data.clone(),
            });
        }
        wal_ops.push(WalOp::WriteSlot {
            node_id,
            slot_bytes: Box::new(slot.to_bytes()),
        });

        let commit_lsn = w.wal.append_transaction(&wal_ops)?;
        Self::push_to_backlog(&mut w, commit_lsn, &wal_ops);

        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            if !heap_appender.staged_data.is_empty() {
                storage.write_heap_at(heap_appender.start_offset(), &heap_appender.staged_data)?;
                w.heap_tail += heap_appender.staged_data.len() as u64;
            }
            storage.write_slot(node_id, &slot)?;
        }

        {
            let mut cache = self
                .slot_cache
                .write()
                .map_err(|_| HdbError::LockPoisoned)?;
            cache[node_id as usize] = slot;
        }

        {
            let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;
            index.remove_node(&schema, node_id, &old_values);
            index.insert_node(&schema, node_id, values);
        }

        Ok(())
    }

    /// Moves an entire subtree rooted at `node_id` under `new_parent_id` in O(1) pointer updates.
    pub fn move_node(&self, node_id: u64, new_parent_id: u64) -> Result<(), HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let (mut node, old_prev, old_next, old_parent, new_parent, new_first_child) = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let n = *Self::get_slot_from_cache(&cache, node_id)?;

            if n.parent_id == new_parent_id {
                return Ok(());
            }

            let mut ancestor = new_parent_id;
            while ancestor != 0 {
                if ancestor == node_id {
                    return Err(HdbError::CyclicMove {
                        node_id,
                        target_parent_id: new_parent_id,
                    });
                }
                ancestor = Self::get_slot_from_cache(&cache, ancestor)?.parent_id;
            }

            let schema = {
                let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
                schemas
                    .get(&n.node_type)
                    .ok_or(HdbError::SchemaNotFound(n.node_type))?
                    .clone()
            };
            let target_parent_type = if new_parent_id == 0 {
                None
            } else {
                Some(Self::get_slot_from_cache(&cache, new_parent_id)?.node_type)
            };
            if schema.parent_type != target_parent_type {
                return Err(HdbError::InvalidParentType {
                    expected: schema.parent_type,
                    actual: target_parent_type,
                });
            }

            let oprev = if n.prev_sibling_id != 0 {
                Some(cache[n.prev_sibling_id as usize])
            } else {
                None
            };
            let onext = if n.next_sibling_id != 0 {
                Some(cache[n.next_sibling_id as usize])
            } else {
                None
            };
            let oparent = if n.prev_sibling_id == 0 && n.parent_id != 0 {
                Some(cache[n.parent_id as usize])
            } else {
                None
            };
            let nparent = if new_parent_id != 0 {
                Some(cache[new_parent_id as usize])
            } else {
                None
            };
            let nfc_id = nparent.map(|p| p.first_child_id).unwrap_or(0);
            let nfc = if nfc_id != 0 {
                Some(cache[nfc_id as usize])
            } else {
                None
            };

            (n, oprev, onext, oparent, nparent, nfc)
        };

        let mut mutated: HashMap<u64, NodeSlot> = HashMap::new();

        if let Some(mut p) = old_prev {
            p.next_sibling_id = node.next_sibling_id;
            mutated.insert(node.prev_sibling_id, p);
        }
        if let Some(mut p) = old_parent {
            p.first_child_id = node.next_sibling_id;
            mutated.insert(node.parent_id, p);
        }
        if let Some(mut nx) = old_next {
            nx.prev_sibling_id = node.prev_sibling_id;
            mutated.insert(node.next_sibling_id, nx);
        }

        let new_fc_id = new_parent.map(|p| p.first_child_id).unwrap_or(0);
        node.parent_id = new_parent_id;
        node.prev_sibling_id = 0;
        node.next_sibling_id = new_fc_id;
        mutated.insert(node_id, node);

        if let Some(mut fc) = new_first_child {
            fc.prev_sibling_id = node_id;
            mutated.insert(new_fc_id, fc);
        }
        if let Some(mut np) = new_parent {
            np.first_child_id = node_id;
            mutated.insert(new_parent_id, np);
        }

        let mut wal_ops = Vec::with_capacity(mutated.len());
        for (&id, slot) in &mutated {
            wal_ops.push(WalOp::WriteSlot {
                node_id: id,
                slot_bytes: Box::new(slot.to_bytes()),
            });
        }

        let commit_lsn = w.wal.append_transaction(&wal_ops)?;
        Self::push_to_backlog(&mut w, commit_lsn, &wal_ops);

        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            for (&id, slot) in &mutated {
                storage.write_slot(id, slot)?;
            }
        }

        let mut cache = self
            .slot_cache
            .write()
            .map_err(|_| HdbError::LockPoisoned)?;
        for (id, slot) in mutated {
            cache[id as usize] = slot;
        }

        Ok(())
    }

    pub fn get(&self, node_id: u64) -> Result<(u16, Vec<Value>), HdbError> {
        let slot = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            *Self::get_slot_from_cache(&cache, node_id)?
        };
        let schema = {
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            schemas
                .get(&slot.node_type)
                .ok_or(HdbError::SchemaNotFound(slot.node_type))?
                .clone()
        };
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
        let values = decode_record(&schema, &slot.inline_data, &*storage)?;
        Ok((slot.node_type, values))
    }

    pub fn children_ids(&self, parent_id: u64) -> Result<Vec<u64>, HdbError> {
        let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
        let parent = Self::get_slot_from_cache(&cache, parent_id)?;
        let mut ids = Vec::new();
        let mut curr = parent.first_child_id;
        while curr != 0 {
            ids.push(curr);
            curr = cache[curr as usize].next_sibling_id;
        }
        Ok(ids)
    }

    /// Deletes a leaf node (fails with `HasChildren` if it has children).
    pub fn delete(&self, node_id: u64) -> Result<(), HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let (slot, prev_slot, next_slot, parent_slot) = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let s = *Self::get_slot_from_cache(&cache, node_id)?;
            if s.first_child_id != 0 {
                return Err(HdbError::HasChildren(node_id));
            }
            let prev = if s.prev_sibling_id != 0 {
                Some(cache[s.prev_sibling_id as usize])
            } else {
                None
            };
            let next = if s.next_sibling_id != 0 {
                Some(cache[s.next_sibling_id as usize])
            } else {
                None
            };
            let parent = if s.prev_sibling_id == 0 && s.parent_id != 0 {
                Some(cache[s.parent_id as usize])
            } else {
                None
            };
            (s, prev, next, parent)
        };

        let schema = {
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            schemas
                .get(&slot.node_type)
                .ok_or(HdbError::SchemaNotFound(slot.node_type))?
                .clone()
        };
        let old_values = {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            decode_record(&schema, &slot.inline_data, &*storage)?
        };

        let mut wal_ops = Vec::new();
        let empty_slot = NodeSlot::empty();
        wal_ops.push(WalOp::WriteSlot {
            node_id,
            slot_bytes: Box::new(empty_slot.to_bytes()),
        });

        let updated_prev = prev_slot.map(|mut p| {
            p.next_sibling_id = slot.next_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: slot.prev_sibling_id,
                slot_bytes: Box::new(p.to_bytes()),
            });
            p
        });

        let updated_parent = parent_slot.map(|mut p| {
            p.first_child_id = slot.next_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: slot.parent_id,
                slot_bytes: Box::new(p.to_bytes()),
            });
            p
        });

        let updated_next = next_slot.map(|mut n| {
            n.prev_sibling_id = slot.prev_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: slot.next_sibling_id,
                slot_bytes: Box::new(n.to_bytes()),
            });
            n
        });

        let commit_lsn = w.wal.append_transaction(&wal_ops)?;
        Self::push_to_backlog(&mut w, commit_lsn, &wal_ops);

        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            storage.write_slot(node_id, &empty_slot)?;
            if let Some(p) = updated_prev {
                storage.write_slot(slot.prev_sibling_id, &p)?;
            }
            if let Some(p) = updated_parent {
                storage.write_slot(slot.parent_id, &p)?;
            }
            if let Some(n) = updated_next {
                storage.write_slot(slot.next_sibling_id, &n)?;
            }
        }

        {
            let mut cache = self
                .slot_cache
                .write()
                .map_err(|_| HdbError::LockPoisoned)?;
            cache[node_id as usize] = empty_slot;
            if let Some(p) = updated_prev {
                cache[slot.prev_sibling_id as usize] = p;
            }
            if let Some(p) = updated_parent {
                cache[slot.parent_id as usize] = p;
            }
            if let Some(n) = updated_next {
                cache[slot.next_sibling_id as usize] = n;
            }
        }
        w.free_list.push(node_id);

        {
            let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;
            index.remove_node(&schema, node_id, &old_values);
        }

        Ok(())
    }

    /// Recursively deletes `root_node_id` and all of its descendants in a single atomic transaction.
    pub fn delete_subtree(&self, root_node_id: u64) -> Result<usize, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let (root_slot, to_delete, prev_slot, next_slot, parent_slot) = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let root = *Self::get_slot_from_cache(&cache, root_node_id)?;

            let mut visited = HashSet::new();
            let mut stack = vec![root_node_id];
            while let Some(curr_id) = stack.pop() {
                if visited.insert(curr_id) {
                    let mut child = cache[curr_id as usize].first_child_id;
                    while child != 0 {
                        stack.push(child);
                        child = cache[child as usize].next_sibling_id;
                    }
                }
            }

            let prev = if root.prev_sibling_id != 0 {
                Some(cache[root.prev_sibling_id as usize])
            } else {
                None
            };
            let next = if root.next_sibling_id != 0 {
                Some(cache[root.next_sibling_id as usize])
            } else {
                None
            };
            let parent = if root.prev_sibling_id == 0 && root.parent_id != 0 {
                Some(cache[root.parent_id as usize])
            } else {
                None
            };

            (root, visited, prev, next, parent)
        };

        let mut index_removals = Vec::with_capacity(to_delete.len());
        {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            for &id in &to_delete {
                let s = &cache[id as usize];
                if let Some(schema) = schemas.get(&s.node_type) {
                    let vals = decode_record(schema, &s.inline_data, &*storage)?;
                    index_removals.push((id, schema.clone(), vals));
                }
            }
        }

        let mut wal_ops = Vec::with_capacity(to_delete.len() + 3);
        let empty_slot = NodeSlot::empty();
        let empty_bytes = empty_slot.to_bytes();

        for &id in &to_delete {
            wal_ops.push(WalOp::WriteSlot {
                node_id: id,
                slot_bytes: Box::new(empty_bytes),
            });
        }

        let updated_prev = prev_slot.map(|mut p| {
            p.next_sibling_id = root_slot.next_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: root_slot.prev_sibling_id,
                slot_bytes: Box::new(p.to_bytes()),
            });
            p
        });

        let updated_parent = parent_slot.map(|mut p| {
            p.first_child_id = root_slot.next_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: root_slot.parent_id,
                slot_bytes: Box::new(p.to_bytes()),
            });
            p
        });

        let updated_next = next_slot.map(|mut n| {
            n.prev_sibling_id = root_slot.prev_sibling_id;
            wal_ops.push(WalOp::WriteSlot {
                node_id: root_slot.next_sibling_id,
                slot_bytes: Box::new(n.to_bytes()),
            });
            n
        });

        let commit_lsn = w.wal.append_transaction(&wal_ops)?;
        Self::push_to_backlog(&mut w, commit_lsn, &wal_ops);

        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            for &id in &to_delete {
                storage.write_slot(id, &empty_slot)?;
            }
            if let Some(p) = updated_prev {
                storage.write_slot(root_slot.prev_sibling_id, &p)?;
            }
            if let Some(p) = updated_parent {
                storage.write_slot(root_slot.parent_id, &p)?;
            }
            if let Some(n) = updated_next {
                storage.write_slot(root_slot.next_sibling_id, &n)?;
            }
        }

        {
            let mut cache = self
                .slot_cache
                .write()
                .map_err(|_| HdbError::LockPoisoned)?;
            for &id in &to_delete {
                cache[id as usize] = empty_slot;
                w.free_list.push(id);
            }
            if let Some(p) = updated_prev {
                cache[root_slot.prev_sibling_id as usize] = p;
            }
            if let Some(p) = updated_parent {
                cache[root_slot.parent_id as usize] = p;
            }
            if let Some(n) = updated_next {
                cache[root_slot.next_sibling_id as usize] = n;
            }
        }

        {
            let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;
            for (id, schema, vals) in index_removals {
                index.remove_node(&schema, id, &vals);
            }
        }

        Ok(to_delete.len())
    }

    // --- Secondary Index & Rich Read APIs ---

    pub fn find_by_index(
        &self,
        node_type: u16,
        col_name: &str,
        value: &Value,
    ) -> Result<Vec<u64>, HdbError> {
        let col_idx = {
            let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
            let schema = schemas
                .get(&node_type)
                .ok_or(HdbError::SchemaNotFound(node_type))?;
            let (idx, col) = schema
                .find_column(col_name)
                .ok_or_else(|| HdbError::ColumnNotFound(col_name.to_string()))?;
            if !(col.indexed || col.unique) {
                return Err(HdbError::ColumnNotIndexed(col_name.to_string()));
            }
            idx
        };

        let index = self.index.read().map_err(|_| HdbError::LockPoisoned)?;
        Ok(index.lookup(node_type, col_idx, value))
    }

    pub fn get_by_index(
        &self,
        node_type: u16,
        col_name: &str,
        value: &Value,
    ) -> Result<Vec<NodeView>, HdbError> {
        let ids = self.find_by_index(node_type, col_name, value)?;
        let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
        let schema = schemas
            .get(&node_type)
            .ok_or(HdbError::SchemaNotFound(node_type))?;

        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(slot) = Self::get_slot_from_cache(&cache, id) {
                let values = decode_record(schema, &slot.inline_data, &*storage)?;
                out.push(NodeView {
                    node_id: id,
                    parent_id: slot.parent_id,
                    depth: 0,
                    node_type,
                    values,
                });
            }
        }
        Ok(out)
    }

    pub fn children_of_type(
        &self,
        parent_id: u64,
        target_node_type: u16,
    ) -> Result<Vec<NodeView>, HdbError> {
        let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
        let schema = schemas
            .get(&target_node_type)
            .ok_or(HdbError::SchemaNotFound(target_node_type))?;

        let parent = Self::get_slot_from_cache(&cache, parent_id)?;
        let mut out = Vec::new();
        let mut curr = parent.first_child_id;

        while curr != 0 {
            let slot = &cache[curr as usize];
            if slot.is_active() && slot.node_type == target_node_type {
                let values = decode_record(schema, &slot.inline_data, &*storage)?;
                out.push(NodeView {
                    node_id: curr,
                    parent_id,
                    depth: 1,
                    node_type: target_node_type,
                    values,
                });
            }
            curr = slot.next_sibling_id;
        }
        Ok(out)
    }

    pub fn walk_subtree(
        &self,
        root_node_id: u64,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeView>, HdbError> {
        let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;

        Self::get_slot_from_cache(&cache, root_node_id)?;

        let mut out = Vec::new();
        let mut stack = vec![(root_node_id, 0usize)];

        while let Some((curr_id, depth)) = stack.pop() {
            let slot = &cache[curr_id as usize];
            if !slot.is_active() {
                continue;
            }
            let schema = schemas
                .get(&slot.node_type)
                .ok_or(HdbError::SchemaNotFound(slot.node_type))?;
            let values = decode_record(schema, &slot.inline_data, &*storage)?;

            out.push(NodeView {
                node_id: curr_id,
                parent_id: slot.parent_id,
                depth,
                node_type: slot.node_type,
                values,
            });

            if max_depth.is_none_or(|limit| depth < limit) {
                let mut children = Vec::new();
                let mut child = slot.first_child_id;
                while child != 0 {
                    children.push(child);
                    child = cache[child as usize].next_sibling_id;
                }
                for child_id in children.into_iter().rev() {
                    stack.push((child_id, depth + 1));
                }
            }
        }

        Ok(out)
    }

    // --- Maintenance: Checkpoint & Vacuum Compaction ---

    pub fn checkpoint(&self) -> Result<usize, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        {
            let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
            storage.sync_all()?;
        }
        w.checkpointed_lsn = w.applied_lsn;

        if w.wal.active_byte_len() > 0 {
            w.wal.rotate_segment()?;
        }

        let safe_purge_lsn = if w.has_standby {
            w.checkpointed_lsn.min(w.replicated_lsn)
        } else {
            w.checkpointed_lsn
        };

        w.wal.purge_segments_up_to(safe_purge_lsn)
    }

    /// Compacts `nodes.db` and `heap.db` by reclaiming dead heap space and gaps in slot IDs.
    /// Active NodeIds are reassigned sequentially starting from 1.
    pub fn vacuum(&self) -> Result<VacuumStats, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let mut cache = self
            .slot_cache
            .write()
            .map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let mut storage = self.storage.write().map_err(|_| HdbError::LockPoisoned)?;
        let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;

        let old_slot_capacity = cache.len() as u64;
        let old_heap_bytes = storage.heap_len()?;

        // 1. Build old_id -> new_id mapping for all active slots
        let mut id_map: HashMap<u64, u64> = HashMap::new();
        id_map.insert(0, 0);
        let mut next_new_id: u64 = 1;
        for (old_id, slot) in cache.iter().enumerate().skip(1) {
            if slot.is_active() {
                id_map.insert(old_id as u64, next_new_id);
                next_new_id += 1;
            }
        }

        // 2. Re-encode active records into a fresh heap buffer and rewrite hierarchy pointers
        let mut new_slots = Vec::with_capacity(next_new_id as usize);
        new_slots.push(NodeSlot::empty()); // Reserve NodeId 0
        let mut new_heap = StagedHeapAppender::new(0);
        let mut new_index = SecondaryIndex::new();

        for (old_id, old_slot) in cache.iter().enumerate().skip(1) {
            if !old_slot.is_active() {
                continue;
            }
            let new_id = id_map[&(old_id as u64)];
            let schema = schemas
                .get(&old_slot.node_type)
                .ok_or(HdbError::SchemaNotFound(old_slot.node_type))?;

            let values = decode_record(schema, &old_slot.inline_data, &*storage)?;
            let (new_inline, total_len) = encode_record(schema, &values, &mut new_heap)?;

            let mut compacted = NodeSlot::empty();
            compacted.flags = NodeSlot::FLAG_ACTIVE;
            compacted.node_type = old_slot.node_type;
            compacted.total_payload_len = total_len;
            compacted.parent_id = *id_map.get(&old_slot.parent_id).unwrap_or(&0);
            compacted.first_child_id = *id_map.get(&old_slot.first_child_id).unwrap_or(&0);
            compacted.next_sibling_id = *id_map.get(&old_slot.next_sibling_id).unwrap_or(&0);
            compacted.prev_sibling_id = *id_map.get(&old_slot.prev_sibling_id).unwrap_or(&0);
            compacted.inline_data = new_inline;

            new_slots.push(compacted);
            new_index.insert_node(schema, new_id, &values);
        }

        // 3. Write compacted files to disk and fsync
        let tmp_nodes_path = self.dir.join("nodes.db.compact");
        let tmp_heap_path = self.dir.join("heap.db.compact");
        {
            let mut nf = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_nodes_path)?;
            for s in &new_slots {
                nf.write_all(&s.to_bytes())?;
            }
            nf.sync_data()?;

            let mut hf = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_heap_path)?;
            hf.write_all(&new_heap.staged_data)?;
            hf.sync_data()?;
        }

        // 4. Atomically swap files and update in-memory state
        storage.replace_with_compacted(&self.dir, &tmp_nodes_path, &tmp_heap_path)?;

        let new_heap_bytes = new_heap.staged_data.len() as u64;
        *cache = new_slots;
        *index = new_index;
        w.free_list.clear();
        w.next_slot_id = next_new_id;
        w.heap_tail = new_heap_bytes;

        if w.wal.active_byte_len() > 0 {
            w.wal.rotate_segment()?;
        }
        if w.wal.active_byte_len() > 0 {
            w.wal.rotate_segment()?;
        }
        w.checkpointed_lsn = w.applied_lsn;
        let safe_lsn = w.checkpointed_lsn;
        w.wal.purge_segments_up_to(safe_lsn)?;

        Ok(VacuumStats {
            active_nodes: next_new_id - 1,
            old_slot_capacity,
            old_heap_bytes,
            new_heap_bytes,
        })
    }

    // --- Replication & Failover Methods ---

    pub fn set_replica_mode(&self, is_replica: bool) -> Result<(), HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        w.is_replica = is_replica;
        Ok(())
    }

    pub fn is_replica(&self) -> Result<bool, HdbError> {
        let w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        Ok(w.is_replica)
    }

    pub fn current_lsn(&self) -> Result<u64, HdbError> {
        let w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        Ok(w.applied_lsn)
    }

    pub fn export_schemas(&self) -> Result<Vec<SegmentSchema>, HdbError> {
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let mut list: Vec<SegmentSchema> = schemas.values().cloned().collect();
        list.sort_by_key(|s| s.node_type);
        Ok(list)
    }

    pub fn import_schemas(&self, incoming: Vec<SegmentSchema>) -> Result<(), HdbError> {
        let mut schemas = self.schemas.write().map_err(|_| HdbError::LockPoisoned)?;
        for s in incoming {
            s.validate(INLINE_PAYLOAD_CAP)?;
            schemas.insert(s.node_type, s);
        }
        CatalogStorage::save(&self.dir.join("catalog.db"), &schemas)?;
        Ok(())
    }

    pub fn fetch_wal_since(&self, after_lsn: u64) -> Result<Vec<WalEntry>, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        w.has_standby = true;
        if after_lsn > w.replicated_lsn {
            w.replicated_lsn = after_lsn;
        }

        if after_lsn >= w.applied_lsn {
            return Ok(Vec::new());
        }
        if let Some(oldest) = w.repl_backlog.front()
            && after_lsn + 1 < oldest.lsn
        {
            return Err(HdbError::ReplicationLag {
                requested_lsn: after_lsn + 1,
                oldest_available_lsn: oldest.lsn,
            });
        }
        Ok(w.repl_backlog
            .iter()
            .filter(|e| e.lsn > after_lsn)
            .cloned()
            .collect())
    }

    pub fn apply_replicated_entries(&self, entries: &[WalEntry]) -> Result<u64, HdbError> {
        if entries.is_empty() {
            return self.current_lsn();
        }

        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        let mut ops_to_persist = Vec::new();

        for entry in entries {
            if entry.lsn <= w.applied_lsn {
                continue;
            }
            ops_to_persist.push(entry.op.clone());
        }

        if ops_to_persist.is_empty() {
            return Ok(w.applied_lsn);
        }

        w.wal.append_transaction(&ops_to_persist)?;

        let mut cache = self
            .slot_cache
            .write()
            .map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;
        let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;

        for entry in entries {
            if entry.lsn <= w.applied_lsn {
                continue;
            }
            if let WalOp::WriteHeap { offset, data } = &entry.op {
                storage.write_heap_at(*offset, data)?;
                let end = *offset + (data.len() as u64);
                if end > w.heap_tail {
                    w.heap_tail = end;
                }
            }
        }

        for entry in entries {
            if entry.lsn <= w.applied_lsn {
                continue;
            }
            if let WalOp::WriteSlot {
                node_id,
                slot_bytes,
            } = &entry.op
            {
                let idx = *node_id as usize;
                if let Some(old_slot) = cache.get(idx).filter(|s| s.is_active())
                    && let Some(schema) = schemas.get(&old_slot.node_type)
                {
                    let old_vals = decode_record(schema, &old_slot.inline_data, &*storage)?;
                    index.remove_node(schema, *node_id, &old_vals);
                }

                let new_slot = NodeSlot::from_bytes(slot_bytes);
                storage.write_slot(*node_id, &new_slot)?;
                if idx >= cache.len() {
                    cache.resize(idx + 1, NodeSlot::empty());
                }
                cache[idx] = new_slot;

                if new_slot.is_active()
                    && let Some(schema) = schemas.get(&new_slot.node_type)
                {
                    let new_vals = decode_record(schema, &new_slot.inline_data, &*storage)?;
                    index.insert_node(schema, *node_id, &new_vals);
                }
            }
            w.applied_lsn = entry.lsn;
        }

        w.free_list.clear();
        for (id, slot) in cache.iter().enumerate().skip(1) {
            if !slot.is_active() {
                w.free_list.push(id as u64);
            }
        }
        w.next_slot_id = cache.len() as u64;

        Ok(w.applied_lsn)
    }

    fn push_to_backlog(w: &mut WriteState, commit_lsn: u64, ops: &[WalOp]) {
        let first_op_lsn = commit_lsn - (ops.len() as u64);
        for (i, op) in ops.iter().enumerate() {
            if w.repl_backlog.len() >= MAX_REPLICATION_BACKLOG {
                w.repl_backlog.pop_front();
            }
            w.repl_backlog.push_back(WalEntry {
                lsn: first_op_lsn + (i as u64),
                txn_id: 0,
                op: op.clone(),
            });
        }
        w.applied_lsn = commit_lsn;
    }

    fn get_slot_from_cache(cache: &[NodeSlot], node_id: u64) -> Result<&NodeSlot, HdbError> {
        if node_id == 0 {
            return Err(HdbError::NodeNotFound(0));
        }
        cache
            .get(node_id as usize)
            .filter(|s| s.is_active())
            .ok_or(HdbError::NodeNotFound(node_id))
    }

    /// Captures a point-in-time consistent physical snapshot for a lagging or new Standby.
    pub fn create_snapshot(&self) -> Result<BaseSnapshot, HdbError> {
        let w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        let schemas = self.schemas.read().map_err(|_| HdbError::LockPoisoned)?;
        let storage = self.storage.read().map_err(|_| HdbError::LockPoisoned)?;

        storage.sync_all()?;
        let (nodes_bytes, heap_bytes) = storage.read_all_files()?;

        let mut schema_list: Vec<SegmentSchema> = schemas.values().cloned().collect();
        schema_list.sort_by_key(|s| s.node_type);

        Ok(BaseSnapshot {
            snapshot_lsn: w.applied_lsn,
            schemas: schema_list,
            nodes_bytes,
            heap_bytes,
        })
    }

    /// Replaces local storage and in-memory caches with a base backup snapshot from Active.
    pub fn install_snapshot(&self, snap: BaseSnapshot) -> Result<(), HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        let mut cache = self
            .slot_cache
            .write()
            .map_err(|_| HdbError::LockPoisoned)?;
        let mut schemas = self.schemas.write().map_err(|_| HdbError::LockPoisoned)?;
        let mut storage = self.storage.write().map_err(|_| HdbError::LockPoisoned)?;
        let mut index = self.index.write().map_err(|_| HdbError::LockPoisoned)?;

        if !snap
            .nodes_bytes
            .len()
            .is_multiple_of(crate::node::NODE_SLOT_SIZE)
        {
            return Err(HdbError::CorruptedData("Snapshot nodes_bytes misaligned"));
        }

        // 1. Write snapshot files to temporary paths and atomically swap
        let tmp_nodes = self.dir.join("nodes.db.snap");
        let tmp_heap = self.dir.join("heap.db.snap");
        {
            let mut nf = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_nodes)?;
            nf.write_all(&snap.nodes_bytes)?;
            nf.sync_data()?;

            let mut hf = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_heap)?;
            hf.write_all(&snap.heap_bytes)?;
            hf.sync_data()?;
        }
        storage.replace_with_compacted(&self.dir, &tmp_nodes, &tmp_heap)?;

        // 2. Replace schemas & persist catalog.db
        schemas.clear();
        for s in snap.schemas {
            s.validate(INLINE_PAYLOAD_CAP)?;
            schemas.insert(s.node_type, s);
        }
        CatalogStorage::save(&self.dir.join("catalog.db"), &schemas)?;

        // 3. Rebuild slot_cache, free_list, and secondary indexes
        let (slot_chunks, remainder) = snap
            .nodes_bytes
            .as_chunks::<{ crate::node::NODE_SLOT_SIZE }>();
        if !remainder.is_empty() {
            return Err(HdbError::CorruptedData("Snapshot nodes_bytes misaligned"));
        }
        let mut new_slots = Vec::with_capacity(slot_chunks.len());
        for arr in slot_chunks {
            new_slots.push(NodeSlot::from_bytes(arr));
        }
        if new_slots.is_empty() {
            new_slots.push(NodeSlot::empty());
        }

        let mut new_index = SecondaryIndex::new();
        w.free_list.clear();
        for (id, slot) in new_slots.iter().enumerate().skip(1) {
            if !slot.is_active() {
                w.free_list.push(id as u64);
            } else if let Some(schema) = schemas.get(&slot.node_type) {
                let vals = decode_record(schema, &slot.inline_data, &*storage)?;
                new_index.insert_node(schema, id as u64, &vals);
            }
        }

        *cache = new_slots;
        *index = new_index;
        w.next_slot_id = cache.len() as u64;
        w.heap_tail = snap.heap_bytes.len() as u64;
        w.applied_lsn = snap.snapshot_lsn;
        w.checkpointed_lsn = snap.snapshot_lsn;

        // 4. Reset local WAL segments cleanly to match the snapshot baseline
        if w.wal.active_byte_len() > 0 {
            w.wal.rotate_segment()?;
        }
        w.wal.purge_segments_up_to(u64::MAX)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType, Decimal};

    fn define_sample_schemas(db: &Database) {
        db.define_segment(SegmentSchema {
            node_type: 1,
            name: "Root".into(),
            parent_type: None,
            columns: vec![
                ColumnDef::new("code", DataType::FixedString(16), false).with_index(true),
            ],
        })
        .unwrap();

        db.define_segment(SegmentSchema {
            node_type: 2,
            name: "Folder".into(),
            parent_type: Some(1),
            columns: vec![
                ColumnDef::new("val", DataType::I64, false).with_index(false),
                ColumnDef::new("note", DataType::VarString, true),
            ],
        })
        .unwrap();

        db.define_segment(SegmentSchema {
            node_type: 5,
            name: "Invoice".into(),
            parent_type: Some(2),
            columns: vec![
                ColumnDef::new("invoice_no", DataType::FixedString(16), false).with_index(true),
                ColumnDef::new("amount", DataType::Decimal, false).with_index(false),
            ],
        })
        .unwrap();
    }

    fn setup_db(name: &str) -> (Database, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("alice_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();
        define_sample_schemas(&db);
        (db, dir)
    }

    #[test]
    fn test_vacuum_compaction_reclaims_heap_and_slots() {
        let (db, dir) = setup_db("vacuum");

        let root = db.insert(0, 1, &[Value::String("ORG-VAC".into())]).unwrap();
        let f_dead = db
            .insert(root, 2, &[Value::I64(1), Value::String("x".repeat(500))])
            .unwrap();
        let f_live = db
            .insert(root, 2, &[Value::I64(2), Value::String("initial".into())])
            .unwrap();

        // Update f_live multiple times to leave stale heap garbage
        for i in 0..5 {
            db.update(
                f_live,
                &[
                    Value::I64(2),
                    Value::String(format!("payload_v{}_{}", i, "y".repeat(200))),
                ],
            )
            .unwrap();
        }
        // Delete f_dead to leave a slot gap
        db.delete(f_dead).unwrap();

        let stats = db.vacuum().unwrap();
        assert_eq!(stats.active_nodes, 2); // root + f_live
        assert_eq!(stats.old_slot_capacity, 4); // dummy(0) + root(1) + f_dead(2) + f_live(3)
        assert!(stats.new_heap_bytes < stats.old_heap_bytes);

        // Verify tree & secondary index still resolve properly after ID reassignment
        let new_root = db
            .find_by_index(1, "code", &Value::String("ORG-VAC".into()))
            .unwrap()[0];
        assert_eq!(new_root, 1);
        let children = db.children_of_type(new_root, 2).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].node_id, 2);
        assert_eq!(
            children[0].values[1],
            Value::String(format!("payload_v4_{}", "y".repeat(200)))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_secondary_index_decimal_and_rich_reads() {
        let (db, dir) = setup_db("idx_reads");

        let root = db.insert(0, 1, &[Value::String("ORG-A".into())]).unwrap();

        let dup_err = db
            .insert(0, 1, &[Value::String("ORG-A".into())])
            .unwrap_err();
        assert!(matches!(
            dup_err,
            HdbError::UniqueConstraintViolation {
                node_type: 1,
                col_index: 0
            }
        ));

        let f1 = db
            .insert(root, 2, &[Value::I64(10), Value::String("sales".into())])
            .unwrap();
        let f2 = db
            .insert(root, 2, &[Value::I64(10), Value::String("dev".into())])
            .unwrap();

        let matching_folders = db.find_by_index(2, "val", &Value::I64(10)).unwrap();
        assert_eq!(matching_folders, vec![f1, f2]);

        let inv1 = db
            .insert(
                f1,
                5,
                &[
                    Value::String("INV-001".into()),
                    Value::Decimal(Decimal::new(125050, 2)),
                ],
            )
            .unwrap();

        let by_dec = db
            .get_by_index(5, "amount", &Value::Decimal(Decimal::new(12505, 1)))
            .unwrap();
        assert_eq!(by_dec.len(), 1);
        assert_eq!(by_dec[0].node_id, inv1);

        let folders = db.children_of_type(root, 2).unwrap();
        assert_eq!(folders.len(), 2);

        let tree = db.walk_subtree(root, None).unwrap();
        assert_eq!(tree.len(), 4);
        assert_eq!(tree[0].depth, 0);
        assert_eq!(tree[3].node_id, inv1);
        assert_eq!(tree[3].depth, 2);

        let shallow_tree = db.walk_subtree(root, Some(1)).unwrap();
        assert_eq!(shallow_tree.len(), 3);

        db.update(
            inv1,
            &[
                Value::String("INV-001".into()),
                Value::Decimal(Decimal::new(99900, 2)),
            ],
        )
        .unwrap();
        assert!(
            db.find_by_index(5, "amount", &Value::Decimal(Decimal::new(12505, 1)))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            db.find_by_index(5, "amount", &Value::Decimal(Decimal::new(999, 0)))
                .unwrap(),
            vec![inv1]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_sibling_pointers_delete_and_freelist_reuse() {
        let (db, dir) = setup_db("pointers");

        let root = db.insert(0, 1, &[Value::String("root".into())]).unwrap();
        let c1 = db.insert(root, 2, &[Value::I64(10), Value::Null]).unwrap();
        let c2 = db.insert(root, 2, &[Value::I64(20), Value::Null]).unwrap();
        let c3 = db.insert(root, 2, &[Value::I64(30), Value::Null]).unwrap();

        assert_eq!(db.children_ids(root).unwrap(), vec![c3, c2, c1]);

        db.delete(c2).unwrap();
        assert_eq!(db.children_ids(root).unwrap(), vec![c3, c1]);
        assert!(matches!(db.get(c2), Err(HdbError::NodeNotFound(_))));
        assert!(
            db.find_by_index(2, "val", &Value::I64(20))
                .unwrap()
                .is_empty()
        );

        let c4 = db.insert(root, 2, &[Value::I64(40), Value::Null]).unwrap();
        assert_eq!(c4, c2);
        assert_eq!(db.children_ids(root).unwrap(), vec![c4, c3, c1]);

        assert!(matches!(db.delete(root), Err(HdbError::HasChildren(_))));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_hierarchy_constraints() {
        let (db, dir) = setup_db("constraints");

        let err = db.insert(0, 2, &[Value::I64(1), Value::Null]).unwrap_err();
        assert!(matches!(
            err,
            HdbError::InvalidParentType {
                expected: Some(1),
                actual: None
            }
        ));

        let root = db.insert(0, 1, &[Value::String("root".into())]).unwrap();

        let err2 = db
            .insert(root, 1, &[Value::String("subroot".into())])
            .unwrap_err();
        assert!(matches!(
            err2,
            HdbError::InvalidParentType {
                expected: None,
                actual: Some(1)
            }
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_move_node_and_cyclic_guard() {
        let (db, dir) = setup_db("move_node");

        let root_a = db.insert(0, 1, &[Value::String("RootA".into())]).unwrap();
        let root_b = db.insert(0, 1, &[Value::String("RootB".into())]).unwrap();

        let folder1 = db.insert(root_a, 2, &[Value::I64(1), Value::Null]).unwrap();
        let folder2 = db.insert(root_a, 2, &[Value::I64(2), Value::Null]).unwrap();
        let inv = db
            .insert(
                folder1,
                5,
                &[
                    Value::String("INV-MOVE".into()),
                    Value::Decimal(Decimal::new(500, 0)),
                ],
            )
            .unwrap();

        db.move_node(folder1, root_b).unwrap();

        assert_eq!(db.children_ids(root_a).unwrap(), vec![folder2]);
        assert_eq!(db.children_ids(root_b).unwrap(), vec![folder1]);
        assert_eq!(db.children_ids(folder1).unwrap(), vec![inv]);

        let err = db.move_node(inv, root_b).unwrap_err();
        assert!(matches!(
            err,
            HdbError::InvalidParentType {
                expected: Some(2),
                actual: Some(1)
            }
        ));

        let err_self = db.move_node(root_a, root_a).unwrap_err();
        assert!(matches!(
            err_self,
            HdbError::CyclicMove {
                node_id,
                target_parent_id
            } if node_id == root_a && target_parent_id == root_a
        ));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_delete_subtree_and_slot_reclamation() {
        let (db, dir) = setup_db("subtree_del");

        let root = db.insert(0, 1, &[Value::String("Company".into())]).unwrap();
        let f_keep = db.insert(root, 2, &[Value::I64(1), Value::Null]).unwrap();
        let f_del = db.insert(root, 2, &[Value::I64(2), Value::Null]).unwrap();
        let f_keep2 = db.insert(root, 2, &[Value::I64(3), Value::Null]).unwrap();

        let inv1 = db
            .insert(
                f_del,
                5,
                &[
                    Value::String("INV-A".into()),
                    Value::Decimal(Decimal::new(100, 0)),
                ],
            )
            .unwrap();
        let inv2 = db
            .insert(
                f_del,
                5,
                &[
                    Value::String("INV-B".into()),
                    Value::Decimal(Decimal::new(200, 0)),
                ],
            )
            .unwrap();

        let deleted_count = db.delete_subtree(f_del).unwrap();
        assert_eq!(deleted_count, 3);
        assert_eq!(db.children_ids(root).unwrap(), vec![f_keep2, f_keep]);

        assert!(
            db.find_by_index(5, "invoice_no", &Value::String("INV-A".into()))
                .unwrap()
                .is_empty()
        );

        let r1 = db.insert(root, 2, &[Value::I64(10), Value::Null]).unwrap();
        let r2 = db.insert(root, 2, &[Value::I64(20), Value::Null]).unwrap();
        let r3 = db.insert(root, 2, &[Value::I64(30), Value::Null]).unwrap();
        let reclaimed = [r1, r2, r3];
        assert!(reclaimed.contains(&f_del));
        assert!(reclaimed.contains(&inv1));
        assert!(reclaimed.contains(&inv2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_persistence_wal_recovery_and_checkpoint() {
        let dir = std::env::temp_dir().join(format!("alice_step3_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let (root_id, child_id) = {
            let db = Database::open(&dir).unwrap();
            define_sample_schemas(&db);

            let r = db.insert(0, 1, &[Value::String("HQ".into())]).unwrap();
            let c = db
                .insert(r, 2, &[Value::I64(100), Value::String("initial".into())])
                .unwrap();

            db.update(c, &[Value::I64(200), Value::String("updated note".into())])
                .unwrap();

            (r, c)
        };

        {
            let db2 = Database::open(&dir).unwrap();
            let (_, r_vals) = db2.get(root_id).unwrap();
            assert_eq!(r_vals[0], Value::String("HQ".into()));

            let (_, c_vals) = db2.get(child_id).unwrap();
            assert_eq!(c_vals[0], Value::I64(200));
            assert_eq!(c_vals[1], Value::String("updated note".into()));

            assert_eq!(
                db2.find_by_index(2, "val", &Value::I64(200)).unwrap(),
                vec![child_id]
            );

            let purged = db2.checkpoint().unwrap();
            assert_eq!(purged, 1);
            assert!(!dir.join("wal_00000001.log").exists());
            assert!(dir.join("wal_00000002.log").exists());
        }

        {
            let db3 = Database::open(&dir).unwrap();
            let (_, c_vals) = db3.get(child_id).unwrap();
            assert_eq!(c_vals[0], Value::I64(200));
            assert_eq!(c_vals[1], Value::String("updated note".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
    #[test]
    fn test_snapshot_export_and_install() {
        let (src_db, src_dir) = setup_db("snap_src");

        let root = src_db
            .insert(0, 1, &[Value::String("ORG-SNAP".into())])
            .unwrap();
        let folder = src_db
            .insert(
                root,
                2,
                &[Value::I64(77), Value::String("heap_payload".into())],
            )
            .unwrap();
        let _inv = src_db
            .insert(
                folder,
                5,
                &[
                    Value::String("INV-SNAP".into()),
                    Value::Decimal(Decimal::new(4250, 2)),
                ],
            )
            .unwrap();

        let snap = src_db.create_snapshot().unwrap();
        assert!(snap.snapshot_lsn > 0);

        // Install snapshot into a brand-new target database directory
        let dst_dir = std::env::temp_dir().join(format!("alice_snap_dst_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dst_dir);
        let dst_db = Database::open(&dst_dir).unwrap();
        dst_db.install_snapshot(snap.clone()).unwrap();

        assert_eq!(dst_db.current_lsn().unwrap(), snap.snapshot_lsn);
        let found = dst_db
            .find_by_index(5, "invoice_no", &Value::String("INV-SNAP".into()))
            .unwrap();
        assert_eq!(found.len(), 1);

        let walked = dst_db.walk_subtree(root, None).unwrap();
        assert_eq!(walked.len(), 3);

        let _ = std::fs::remove_dir_all(&src_dir);
        let _ = std::fs::remove_dir_all(&dst_dir);
    }
}
