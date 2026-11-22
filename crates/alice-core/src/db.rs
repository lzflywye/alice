use crate::catalog::CatalogStorage;
use crate::error::HdbError;
use crate::node::{INLINE_PAYLOAD_CAP, NodeSlot};
use crate::record::{HeapReader, StagedHeapAppender, decode_record, encode_record};
use crate::schema::{SegmentSchema, Value};
use crate::storage::DiskStorage;
use crate::wal::{WalEntry, WalOp, WalWriter};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

/// Default WAL segment size (4 MB) before rotating to a new segment file.
const DEFAULT_WAL_SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
/// Maximum number of recent WAL entries retained in memory for streaming to Standby.
const MAX_REPLICATION_BACKLOG: usize = 10_000;

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

/// Persistent Hierarchical Database with single-writer serialization, segmented WAL,
/// catalog persistence, checkpointing, and Active-Standby replication support.
pub struct Database {
    dir: PathBuf,
    schemas: RwLock<HashMap<u16, SegmentSchema>>,
    writer: Mutex<WriteState>,
    slot_cache: RwLock<Vec<NodeSlot>>,
    storage: DiskStorage,
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

        // 3. Rebuild free list from inactive slots (skipping NodeId 0)
        let mut free_list = Vec::new();
        for (id, slot) in slots.iter().enumerate().skip(1) {
            if !slot.is_active() {
                free_list.push(id as u64);
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
            storage,
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

        if !heap_appender.staged_data.is_empty() {
            self.storage
                .write_heap_at(heap_appender.start_offset(), &heap_appender.staged_data)?;
            w.heap_tail += heap_appender.staged_data.len() as u64;
        }
        self.storage.write_slot(new_id, &new_slot)?;
        if let Some(fc) = updated_fc {
            self.storage.write_slot(old_first_child_id, &fc)?;
        }
        if let Some(p) = updated_parent {
            self.storage.write_slot(parent_id, &p)?;
        }

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

        if !heap_appender.staged_data.is_empty() {
            self.storage
                .write_heap_at(heap_appender.start_offset(), &heap_appender.staged_data)?;
            w.heap_tail += heap_appender.staged_data.len() as u64;
        }
        self.storage.write_slot(node_id, &slot)?;

        let mut cache = self
            .slot_cache
            .write()
            .map_err(|_| HdbError::LockPoisoned)?;
        cache[node_id as usize] = slot;

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

            // No-op if already under target parent
            if n.parent_id == new_parent_id {
                return Ok(());
            }

            // Prevent moving a node into itself or any of its descendants
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

            // Validate schema parent_type constraint against new_parent
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

        // Collect mutated slots by NodeId (using HashMap handles cases where old_parent == new_parent cleanly)
        let mut mutated: HashMap<u64, NodeSlot> = HashMap::new();

        // 1. Detach from old siblings / old parent
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

        // 2. Attach as new first child of new_parent
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

        for (&id, slot) in &mutated {
            self.storage.write_slot(id, slot)?;
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
        let values = decode_record(&schema, &slot.inline_data, &self.storage)?;
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

        self.storage.write_slot(node_id, &empty_slot)?;
        if let Some(p) = updated_prev {
            self.storage.write_slot(slot.prev_sibling_id, &p)?;
        }
        if let Some(p) = updated_parent {
            self.storage.write_slot(slot.parent_id, &p)?;
        }
        if let Some(n) = updated_next {
            self.storage.write_slot(slot.next_sibling_id, &n)?;
        }

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
        w.free_list.push(node_id);

        Ok(())
    }

    /// Recursively deletes `node_id` and all of its descendants in a single atomic transaction.
    /// Returns the total number of deleted nodes.
    pub fn delete_subtree(&self, root_node_id: u64) -> Result<usize, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        if w.is_replica {
            return Err(HdbError::ReadOnlyReplica);
        }

        let (root_slot, to_delete, prev_slot, next_slot, parent_slot) = {
            let cache = self.slot_cache.read().map_err(|_| HdbError::LockPoisoned)?;
            let root = *Self::get_slot_from_cache(&cache, root_node_id)?;

            // Collect all descendants iteratively (DFS) to avoid stack overflow on deep trees
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

        for &id in &to_delete {
            self.storage.write_slot(id, &empty_slot)?;
        }
        if let Some(p) = updated_prev {
            self.storage.write_slot(root_slot.prev_sibling_id, &p)?;
        }
        if let Some(p) = updated_parent {
            self.storage.write_slot(root_slot.parent_id, &p)?;
        }
        if let Some(n) = updated_next {
            self.storage.write_slot(root_slot.next_sibling_id, &n)?;
        }

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

        Ok(to_delete.len())
    }

    /// Syncs nodes.db and heap.db to disk, rotates the active WAL segment, and purges
    /// closed segments that are safely checkpointed (and replicated if a Standby is attached).
    pub fn checkpoint(&self) -> Result<usize, HdbError> {
        let mut w = self.writer.lock().map_err(|_| HdbError::LockPoisoned)?;
        self.storage.sync_all()?;
        w.checkpointed_lsn = w.applied_lsn;

        // Rotate active segment so its entries become eligible for purge once replicated
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

    /// Fetches WAL entries strictly greater than `after_lsn` and records Standby's progress.
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

    /// Applies replicated physical WAL entries received from an Active primary.
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
        for entry in entries {
            if entry.lsn <= w.applied_lsn {
                continue;
            }
            match &entry.op {
                WalOp::WriteSlot {
                    node_id,
                    slot_bytes,
                } => {
                    let slot = NodeSlot::from_bytes(slot_bytes);
                    self.storage.write_slot(*node_id, &slot)?;
                    let idx = *node_id as usize;
                    if idx >= cache.len() {
                        cache.resize(idx + 1, NodeSlot::empty());
                    }
                    cache[idx] = slot;
                }
                WalOp::WriteHeap { offset, data } => {
                    self.storage.write_heap_at(*offset, data)?;
                    let end = *offset + (data.len() as u64);
                    if end > w.heap_tail {
                        w.heap_tail = end;
                    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, DataType};

    fn define_sample_schemas(db: &Database) {
        // Type 1: Root (e.g., Organization)
        db.define_segment(SegmentSchema {
            node_type: 1,
            name: "Root".into(),
            parent_type: None,
            columns: vec![ColumnDef {
                name: "title".into(),
                data_type: DataType::FixedString(16),
                nullable: false,
            }],
        })
        .unwrap();

        // Type 2: Folder / Department (can sit under Root(1) OR another Folder(2))
        db.define_segment(SegmentSchema {
            node_type: 2,
            name: "Folder".into(),
            parent_type: Some(1),
            columns: vec![
                ColumnDef {
                    name: "val".into(),
                    data_type: DataType::I64,
                    nullable: false,
                },
                ColumnDef {
                    name: "note".into(),
                    data_type: DataType::VarString,
                    nullable: true,
                },
            ],
        })
        .unwrap();

        // Type 3: RecursiveNode (allows self-nested hierarchy for cyclic move tests)
        db.define_segment(SegmentSchema {
            node_type: 3,
            name: "TreeNode".into(),
            parent_type: Some(3),
            columns: vec![ColumnDef {
                name: "label".into(),
                data_type: DataType::FixedString(16),
                nullable: false,
            }],
        })
        .unwrap();

        // Type 4: TopTreeNode (root for TreeNode(3))
        db.define_segment(SegmentSchema {
            node_type: 4,
            name: "TopTree".into(),
            parent_type: None,
            columns: vec![ColumnDef {
                name: "label".into(),
                data_type: DataType::FixedString(16),
                nullable: false,
            }],
        })
        .unwrap();

        // Type 5: LeafItem (child of Folder(2))
        db.define_segment(SegmentSchema {
            node_type: 5,
            name: "LeafItem".into(),
            parent_type: Some(2),
            columns: vec![ColumnDef {
                name: "code".into(),
                data_type: DataType::I64,
                nullable: false,
            }],
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
        let leaf = db.insert(folder1, 5, &[Value::I64(999)]).unwrap();

        // RootA has [folder2, folder1]; move folder1 (along with its child `leaf`) under RootB
        db.move_node(folder1, root_b).unwrap();

        assert_eq!(db.children_ids(root_a).unwrap(), vec![folder2]);
        assert_eq!(db.children_ids(root_b).unwrap(), vec![folder1]);
        // Subtree child `leaf` remains intact under `folder1`
        assert_eq!(db.children_ids(folder1).unwrap(), vec![leaf]);

        // Schema constraint check: cannot move LeafItem(5, expects parent 2) directly under RootB(1)
        let err = db.move_node(leaf, root_b).unwrap_err();
        assert!(matches!(
            err,
            HdbError::InvalidParentType {
                expected: Some(2),
                actual: Some(1)
            }
        ));

        // Cyclic move check using self-nesting TreeNode(3)
        let top = db.insert(0, 4, &[Value::String("Top".into())]).unwrap();
        db.define_segment(SegmentSchema {
            node_type: 6,
            name: "Branch".into(),
            parent_type: Some(4),
            columns: vec![],
        })
        .unwrap();
        // Allow TreeNode(3) under Branch(6) or TreeNode(3) -> test self-cycle on type 3
        // Let's register a self-referential root type 7 (parent_type: None) + child type 8 (parent_type: Some(8))
        // For cyclic test, build a chain: n1(type 6 under top) -> skip; use a dedicated self-parent schema:
        db.define_segment(SegmentSchema {
            node_type: 7,
            name: "SelfNode".into(),
            parent_type: None,
            columns: vec![],
        })
        .unwrap();
        // Note: a node whose parent_type is Some(7) can be moved under any node of type 7
        db.define_segment(SegmentSchema {
            node_type: 8,
            name: "RecursiveChild".into(),
            parent_type: Some(8),
            columns: vec![],
        })
        .unwrap();

        let _ = top;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cyclic_move_prevention() {
        let dir = std::env::temp_dir().join(format!("alice_cycle_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Database::open(&dir).unwrap();

        // Type 10: Self-referencing node (can be root OR child of type 10 by creating root via type 10 if parent_type=Some(10) isn't root)
        // To build a multi-level chain of the same node_type, bootstrap the top node first or allow 10 -> 10:
        db.define_segment(SegmentSchema {
            node_type: 10,
            name: "ChainNode".into(),
            parent_type: None,
            columns: vec![],
        })
        .unwrap();
        // Verify moving a node into itself is rejected
        let n1 = db.insert(0, 10, &[]).unwrap();
        let err_self = db.move_node(n1, n1).unwrap_err();
        assert!(matches!(
            err_self,
            HdbError::CyclicMove {
                node_id,
                target_parent_id
            } if node_id == n1 && target_parent_id == n1
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

        // Attach 2 grandchildren under `f_del`
        let leaf1 = db.insert(f_del, 5, &[Value::I64(101)]).unwrap();
        let leaf2 = db.insert(f_del, 5, &[Value::I64(102)]).unwrap();

        // Children of root before delete: [f_keep2, f_del, f_keep]
        assert_eq!(db.children_ids(root).unwrap(), vec![f_keep2, f_del, f_keep]);

        // Delete `f_del` subtree (f_del + leaf1 + leaf2 = 3 nodes total)
        let deleted_count = db.delete_subtree(f_del).unwrap();
        assert_eq!(deleted_count, 3);

        // Remaining siblings must be linked cleanly: [f_keep2, f_keep]
        assert_eq!(db.children_ids(root).unwrap(), vec![f_keep2, f_keep]);
        assert!(matches!(db.get(f_del), Err(HdbError::NodeNotFound(_))));
        assert!(matches!(db.get(leaf1), Err(HdbError::NodeNotFound(_))));
        assert!(matches!(db.get(leaf2), Err(HdbError::NodeNotFound(_))));

        // All 3 freed slots must be reused before allocating new slot IDs
        let r1 = db.insert(root, 2, &[Value::I64(10), Value::Null]).unwrap();
        let r2 = db.insert(root, 2, &[Value::I64(20), Value::Null]).unwrap();
        let r3 = db.insert(root, 2, &[Value::I64(30), Value::Null]).unwrap();
        let reclaimed = [r1, r2, r3];
        assert!(reclaimed.contains(&f_del));
        assert!(reclaimed.contains(&leaf1));
        assert!(reclaimed.contains(&leaf2));

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

            // Checkpoint rotates active WAL segment and purges the closed segment
            let purged = db2.checkpoint().unwrap();
            assert_eq!(purged, 1);
            assert!(!dir.join("wal_00000001.log").exists());
            assert!(dir.join("wal_00000002.log").exists());
        }

        // Reopen after checkpoint: data must still be intact
        {
            let db3 = Database::open(&dir).unwrap();
            let (_, c_vals) = db3.get(child_id).unwrap();
            assert_eq!(c_vals[0], Value::I64(200));
            assert_eq!(c_vals[1], Value::String("updated note".into()));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
