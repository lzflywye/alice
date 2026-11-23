use crate::catalog::Catalog;
use crate::index::SecondaryIndex;
use crate::record::{EncodedPayload, decode_record, encode_record};
use crate::schema::{SegmentSchema, Value};
use crate::wal_codec::{SlotOp, decode_slot_op, encode_slot_op};
use common::{DbError, crc32};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use storage_slotted::{
    BufferPoolStats, DEFAULT_POOL_PAGES, DiskStorage, NodeId, NodeSlot, SLOT_SIZE,
    SlottedBufferPool,
};
use wal::{DEFAULT_WAL_SEGMENT_BYTES, WalManager, WalRecord};

const ROOT_NODE_ID: NodeId = 0;

#[derive(Debug, Clone, PartialEq)]
pub struct NodeView {
    pub node_id: NodeId,
    pub parent_id: NodeId,
    pub node_type: u16,
    pub depth: usize,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VacuumStats {
    pub live_nodes: usize,
    pub reclaimed_slots: usize,
    pub old_heap_bytes: u64,
    pub new_heap_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct SnapshotBundle {
    pub base_lsn: u64,
    pub nodes_bytes: Vec<u8>,
    pub heap_bytes: Vec<u8>,
    pub schemas: Vec<SegmentSchema>,
}

struct DbState {
    pool: SlottedBufferPool,
    catalog: Catalog,
    index: SecondaryIndex,
    free_list: BTreeSet<NodeId>,
}

pub struct Database {
    data_dir: PathBuf,
    state: RwLock<DbState>,
    wal: Mutex<WalManager>,
    writer: Mutex<()>,
    is_replica: AtomicBool,
}

impl Database {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(dir, DEFAULT_WAL_SEGMENT_BYTES, DEFAULT_POOL_PAGES)
    }

    pub fn open_with_segment_size(
        dir: impl AsRef<Path>,
        segment_max_bytes: u64,
    ) -> Result<Self, DbError> {
        Self::open_with_options(dir, segment_max_bytes, DEFAULT_POOL_PAGES)
    }

    pub fn open_with_options(
        dir: impl AsRef<Path>,
        segment_max_bytes: u64,
        pool_capacity_pages: usize,
    ) -> Result<Self, DbError> {
        let data_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;

        let storage = DiskStorage::open(&data_dir)?;
        let mut pool = SlottedBufferPool::new(storage, pool_capacity_pages)?;
        let catalog = Catalog::open(data_dir.join("catalog.db"))?;
        let (wal_mgr, records) = WalManager::open(data_dir.join("wal"), segment_max_bytes)?;

        if pool.total_slots() == 0 {
            let root = NodeSlot::new_root();
            pool.put_slot(ROOT_NODE_ID, &root)?;
            pool.flush_all()?;
        }

        // Replay WAL records through the buffer pool
        for rec in &records {
            match decode_slot_op(rec)? {
                SlotOp::WriteSlot { node_id, slot } => {
                    pool.put_slot(node_id, &slot)?;
                }
                SlotOp::WriteHeap { offset, data } => {
                    pool.write_heap_at(offset, &data)?;
                }
            }
        }
        pool.flush_all()?;

        let mut free_list = BTreeSet::new();
        let mut index = SecondaryIndex::default();
        rebuild_metadata_from_pool(&mut pool, &catalog, &mut free_list, &mut index)?;

        Ok(Self {
            data_dir,
            state: RwLock::new(DbState {
                pool,
                catalog,
                index,
                free_list,
            }),
            wal: Mutex::new(wal_mgr),
            writer: Mutex::new(()),
            is_replica: AtomicBool::new(false),
        })
    }

    pub fn buffer_pool_stats(&self) -> Result<BufferPoolStats, DbError> {
        let st = self.state.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(st.pool.stats())
    }

    pub fn set_replica_mode(&self, replica: bool) -> Result<(), DbError> {
        self.is_replica.store(replica, Ordering::SeqCst);
        Ok(())
    }

    pub fn current_lsn(&self) -> Result<u64, DbError> {
        let wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
        Ok(wal.current_lsn())
    }

    pub fn define_segment(&self, schema: SegmentSchema) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        if self.is_replica.load(Ordering::SeqCst) {
            return Err(DbError::ReadOnlyReplica);
        }
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        st.catalog.register(schema)
    }

    pub fn export_schemas(&self) -> Result<Vec<SegmentSchema>, DbError> {
        let st = self.state.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(st.catalog.all())
    }

    pub fn import_schemas(&self, schemas: Vec<SegmentSchema>) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        st.catalog.replace_all(schemas)?;
        let DbState {
            ref mut pool,
            ref catalog,
            ref mut free_list,
            ref mut index,
        } = *st;
        rebuild_metadata_from_pool(pool, catalog, free_list, index)
    }

    pub fn insert(
        &self,
        parent_id: NodeId,
        node_type: u16,
        values: &[Value],
    ) -> Result<NodeId, DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        if self.is_replica.load(Ordering::SeqCst) {
            return Err(DbError::ReadOnlyReplica);
        }

        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        let schema = st
            .catalog
            .get(node_type)
            .ok_or(DbError::SchemaNotFound(node_type))?
            .clone();

        if parent_id >= st.pool.total_slots() {
            return Err(DbError::ParentNotFound(parent_id));
        }
        let mut parent_slot = st.pool.get_slot(parent_id)?;
        if !parent_slot.is_active() {
            return Err(DbError::ParentNotFound(parent_id));
        }

        let actual_parent_type = if parent_id == ROOT_NODE_ID {
            None
        } else {
            Some(parent_slot.node_type)
        };
        if schema.parent_type != actual_parent_type {
            return Err(DbError::InvalidParentType {
                child_type: node_type,
                expected_parent: schema.parent_type,
                actual_parent: actual_parent_type,
            });
        }

        st.index.check_unique(&schema, values, None)?;
        let encoded = encode_record(&schema, values)?;

        let new_id = if let Some(&reused) = st.free_list.iter().next() {
            st.free_list.remove(&reused);
            reused
        } else {
            st.pool.total_slots()
        };

        let mut ops = Vec::new();
        let mut new_slot = NodeSlot::new_active(node_type, parent_id);

        match encoded {
            EncodedPayload::Inline(arr, len) => {
                new_slot.set_inline_payload(&arr[..len as usize]);
            }
            EncodedPayload::Heap(bytes) => {
                let checksum = crc32(&bytes);
                let offset = st.pool.heap_len()?;
                ops.push(SlotOp::WriteHeap {
                    offset,
                    data: bytes.clone(),
                });
                new_slot.set_heap_ref(offset, bytes.len() as u32, checksum);
            }
        }

        if parent_slot.first_child_id == 0 {
            parent_slot.first_child_id = new_id;
            ops.push(SlotOp::WriteSlot {
                node_id: parent_id,
                slot: Box::new(parent_slot),
            });
        } else {
            let mut cur_id = parent_slot.first_child_id;
            let mut cur_slot = st.pool.get_slot(cur_id)?;
            while cur_slot.next_sibling_id != 0 {
                cur_id = cur_slot.next_sibling_id;
                cur_slot = st.pool.get_slot(cur_id)?;
            }
            cur_slot.next_sibling_id = new_id;
            new_slot.prev_sibling_id = cur_id;
            ops.push(SlotOp::WriteSlot {
                node_id: cur_id,
                slot: Box::new(cur_slot),
            });
        }

        ops.push(SlotOp::WriteSlot {
            node_id: new_id,
            slot: Box::new(new_slot),
        });

        self.commit_ops(&mut st, &ops)?;
        st.index.insert_record(&schema, new_id, values);

        Ok(new_id)
    }

    pub fn update(&self, node_id: NodeId, values: &[Value]) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        if self.is_replica.load(Ordering::SeqCst) {
            return Err(DbError::ReadOnlyReplica);
        }
        if node_id == ROOT_NODE_ID {
            return Err(DbError::CannotDeleteRoot);
        }

        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        if node_id >= st.pool.total_slots() {
            return Err(DbError::NodeNotFound(node_id));
        }
        let mut slot = st.pool.get_slot(node_id)?;
        if !slot.is_active() {
            return Err(DbError::NodeNotFound(node_id));
        }

        let schema = st
            .catalog
            .get(slot.node_type)
            .ok_or(DbError::SchemaNotFound(slot.node_type))?
            .clone();

        st.index.check_unique(&schema, values, Some(node_id))?;
        let old_bytes = read_slot_payload(&st.pool, &slot)?;
        let old_values = decode_record(&schema, &old_bytes)?;
        let encoded = encode_record(&schema, values)?;

        let mut ops = Vec::new();
        match encoded {
            EncodedPayload::Inline(arr, len) => {
                slot.set_inline_payload(&arr[..len as usize]);
            }
            EncodedPayload::Heap(bytes) => {
                let checksum = crc32(&bytes);
                let offset = st.pool.heap_len()?;
                ops.push(SlotOp::WriteHeap {
                    offset,
                    data: bytes.clone(),
                });
                slot.set_heap_ref(offset, bytes.len() as u32, checksum);
            }
        }
        ops.push(SlotOp::WriteSlot {
            node_id,
            slot: Box::new(slot),
        });

        self.commit_ops(&mut st, &ops)?;
        st.index.remove_record(&schema, node_id, &old_values);
        st.index.insert_record(&schema, node_id, values);

        Ok(())
    }

    pub fn delete(&self, node_id: NodeId) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        if self.is_replica.load(Ordering::SeqCst) {
            return Err(DbError::ReadOnlyReplica);
        }
        if node_id == ROOT_NODE_ID {
            return Err(DbError::CannotDeleteRoot);
        }

        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        if node_id >= st.pool.total_slots() {
            return Err(DbError::NodeNotFound(node_id));
        }
        let slot = st.pool.get_slot(node_id)?;
        if !slot.is_active() {
            return Err(DbError::NodeNotFound(node_id));
        }
        if slot.first_child_id != 0 {
            return Err(DbError::HasChildren(node_id));
        }

        let schema = st
            .catalog
            .get(slot.node_type)
            .ok_or(DbError::SchemaNotFound(slot.node_type))?
            .clone();
        let old_bytes = read_slot_payload(&st.pool, &slot)?;
        let old_values = decode_record(&schema, &old_bytes)?;

        let mut ops = Vec::new();
        let parent_id = slot.parent_id;
        let prev_id = slot.prev_sibling_id;
        let next_id = slot.next_sibling_id;

        if prev_id == 0 {
            let mut parent_slot = st.pool.get_slot(parent_id)?;
            parent_slot.first_child_id = next_id;
            ops.push(SlotOp::WriteSlot {
                node_id: parent_id,
                slot: Box::new(parent_slot),
            });
        } else {
            let mut prev_slot = st.pool.get_slot(prev_id)?;
            prev_slot.next_sibling_id = next_id;
            ops.push(SlotOp::WriteSlot {
                node_id: prev_id,
                slot: Box::new(prev_slot),
            });
        }

        if next_id != 0 {
            let mut next_slot = st.pool.get_slot(next_id)?;
            next_slot.prev_sibling_id = prev_id;
            ops.push(SlotOp::WriteSlot {
                node_id: next_id,
                slot: Box::new(next_slot),
            });
        }

        ops.push(SlotOp::WriteSlot {
            node_id,
            slot: Box::new(NodeSlot::default()),
        });

        self.commit_ops(&mut st, &ops)?;
        st.free_list.insert(node_id);
        st.index.remove_record(&schema, node_id, &old_values);

        Ok(())
    }

    pub fn get(&self, node_id: NodeId) -> Result<(u16, Vec<Value>), DbError> {
        if node_id == ROOT_NODE_ID {
            return Err(DbError::NodeNotFound(ROOT_NODE_ID));
        }
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        if node_id >= st.pool.total_slots() {
            return Err(DbError::NodeNotFound(node_id));
        }
        let slot = st.pool.get_slot(node_id)?;
        if !slot.is_active() {
            return Err(DbError::NodeNotFound(node_id));
        }

        let schema = st
            .catalog
            .get(slot.node_type)
            .ok_or(DbError::SchemaNotFound(slot.node_type))?;
        let raw = read_slot_payload(&st.pool, &slot)?;
        let values = decode_record(schema, &raw)?;
        Ok((slot.node_type, values))
    }

    pub fn children_ids(&self, parent_id: NodeId) -> Result<Vec<NodeId>, DbError> {
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        if parent_id >= st.pool.total_slots() {
            return Err(DbError::ParentNotFound(parent_id));
        }
        let parent_slot = st.pool.get_slot(parent_id)?;
        if !parent_slot.is_active() {
            return Err(DbError::ParentNotFound(parent_id));
        }

        let mut ids = Vec::new();
        let mut cur = parent_slot.first_child_id;
        while cur != 0 {
            ids.push(cur);
            cur = st.pool.get_slot(cur)?.next_sibling_id;
        }
        Ok(ids)
    }

    pub fn find_by_index(
        &self,
        node_type: u16,
        col_name: &str,
        value: &Value,
    ) -> Result<Vec<NodeId>, DbError> {
        let st = self.state.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(st.index.lookup(node_type, col_name, value))
    }

    pub fn walk_subtree(
        &self,
        root_node_id: NodeId,
        max_depth: Option<usize>,
    ) -> Result<Vec<NodeView>, DbError> {
        if root_node_id == ROOT_NODE_ID {
            return Err(DbError::NodeNotFound(ROOT_NODE_ID));
        }
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        if root_node_id >= st.pool.total_slots() {
            return Err(DbError::NodeNotFound(root_node_id));
        }
        let root_slot = st.pool.get_slot(root_node_id)?;
        if !root_slot.is_active() {
            return Err(DbError::NodeNotFound(root_node_id));
        }

        let mut result = Vec::new();
        let mut stack = vec![(root_node_id, 0usize)];

        while let Some((curr_id, depth)) = stack.pop() {
            let slot = st.pool.get_slot(curr_id)?;
            let schema = st
                .catalog
                .get(slot.node_type)
                .ok_or(DbError::SchemaNotFound(slot.node_type))?;
            let raw = read_slot_payload(&st.pool, &slot)?;
            let values = decode_record(schema, &raw)?;

            result.push(NodeView {
                node_id: curr_id,
                parent_id: slot.parent_id,
                node_type: slot.node_type,
                depth,
                values,
            });

            if max_depth.is_none_or(|m| depth < m) {
                let mut children = Vec::new();
                let mut c = slot.first_child_id;
                while c != 0 {
                    children.push(c);
                    c = st.pool.get_slot(c)?.next_sibling_id;
                }
                for child_id in children.into_iter().rev() {
                    stack.push((child_id, depth + 1));
                }
            }
        }
        Ok(result)
    }

    pub fn fetch_wal_since(&self, after_lsn: u64) -> Result<Vec<WalRecord>, DbError> {
        let wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
        wal.fetch_since(after_lsn)
    }

    pub fn apply_replicated_entries(&self, records: &[WalRecord]) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;

        for rec in records {
            wal.append(rec.payload.clone())?;
            match decode_slot_op(rec)? {
                SlotOp::WriteSlot { node_id, slot } => {
                    st.pool.put_slot(node_id, &slot)?;
                }
                SlotOp::WriteHeap { offset, data } => {
                    st.pool.write_heap_at(offset, &data)?;
                }
            }
        }
        st.pool.flush_all()?;

        let DbState {
            ref mut pool,
            ref catalog,
            ref mut free_list,
            ref mut index,
        } = *st;
        rebuild_metadata_from_pool(pool, catalog, free_list, index)
    }

    pub fn create_snapshot(&self) -> Result<SnapshotBundle, DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        st.pool.flush_all()?;
        let wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;

        let nodes_bytes = fs::read(self.data_dir.join("nodes.db"))?;
        let heap_bytes = fs::read(self.data_dir.join("heap.db"))?;

        Ok(SnapshotBundle {
            base_lsn: wal.current_lsn(),
            nodes_bytes,
            heap_bytes,
            schemas: st.catalog.all(),
        })
    }

    pub fn install_snapshot(&self, snap: SnapshotBundle) -> Result<(), DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;

        st.pool.replace_files(&snap.nodes_bytes, &snap.heap_bytes)?;
        st.catalog.replace_all(snap.schemas)?;
        wal.purge_all_and_reset(snap.base_lsn)?;

        let DbState {
            ref mut pool,
            ref catalog,
            ref mut free_list,
            ref mut index,
        } = *st;
        rebuild_metadata_from_pool(pool, catalog, free_list, index)
    }

    pub fn vacuum(&self) -> Result<VacuumStats, DbError> {
        let _w = self.writer.lock().map_err(|_| DbError::LockPoisoned)?;
        if self.is_replica.load(Ordering::SeqCst) {
            return Err(DbError::ReadOnlyReplica);
        }

        let mut st = self.state.write().map_err(|_| DbError::LockPoisoned)?;
        st.pool.flush_all()?;
        let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;

        let old_heap_bytes = st.pool.heap_len()?;
        let total_slots = st.pool.total_slots();
        let mut all_slots = Vec::with_capacity(total_slots as usize);
        for id in 0..total_slots {
            all_slots.push(st.pool.get_slot(id)?);
        }

        let mut new_heap = Vec::new();
        let mut live_nodes = 0usize;

        for slot in all_slots.iter_mut().skip(1) {
            if !slot.is_active() {
                continue;
            }
            live_nodes += 1;
            if slot.is_heap_payload() {
                let raw = read_slot_payload(&st.pool, slot)?;
                let checksum = crc32(&raw);
                let new_offset = new_heap.len() as u64;
                new_heap.extend_from_slice(&raw);
                slot.set_heap_ref(new_offset, raw.len() as u32, checksum);
            }
        }

        let old_slot_count = all_slots.len();
        while all_slots.len() > 1 && !all_slots.last().unwrap().is_active() {
            all_slots.pop();
        }
        let reclaimed_slots = old_slot_count - all_slots.len();

        let mut new_nodes_bytes = Vec::with_capacity(all_slots.len() * SLOT_SIZE);
        for slot in &all_slots {
            new_nodes_bytes.extend_from_slice(&slot.to_bytes());
        }

        let new_heap_bytes = new_heap.len() as u64;
        st.pool.replace_files(&new_nodes_bytes, &new_heap)?;

        st.free_list.clear();
        for (idx, slot) in all_slots.iter().enumerate().skip(1) {
            if !slot.is_active() {
                st.free_list.insert(idx as NodeId);
            }
        }

        let lsn = wal.current_lsn();
        wal.purge_all_and_reset(lsn)?;

        Ok(VacuumStats {
            live_nodes,
            reclaimed_slots,
            old_heap_bytes,
            new_heap_bytes,
        })
    }

    fn commit_ops(&self, st: &mut DbState, ops: &[SlotOp]) -> Result<(), DbError> {
        {
            let mut wal = self.wal.lock().map_err(|_| DbError::LockPoisoned)?;
            for op in ops {
                wal.append(encode_slot_op(op))?;
            }
        }

        for op in ops {
            match op {
                SlotOp::WriteSlot { node_id, slot } => {
                    st.pool.put_slot(*node_id, slot)?;
                }
                SlotOp::WriteHeap { offset, data } => {
                    st.pool.write_heap_at(*offset, data)?;
                }
            }
        }
        Ok(())
    }
}

fn read_slot_payload(pool: &SlottedBufferPool, slot: &NodeSlot) -> Result<Vec<u8>, DbError> {
    if !slot.is_heap_payload() {
        Ok(slot.inline_payload().to_vec())
    } else {
        let (offset, len, expected_crc) = slot.heap_ref();
        let bytes = pool.read_heap(offset, len)?;
        let actual_crc = crc32(&bytes);
        if actual_crc != expected_crc {
            return Err(DbError::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }
        Ok(bytes)
    }
}

fn rebuild_metadata_from_pool(
    pool: &mut SlottedBufferPool,
    catalog: &Catalog,
    free_list: &mut BTreeSet<NodeId>,
    index: &mut SecondaryIndex,
) -> Result<(), DbError> {
    free_list.clear();
    index.clear();

    let total = pool.total_slots();
    for node_id in 1..total {
        let slot = pool.get_slot(node_id)?;
        if !slot.is_active() {
            free_list.insert(node_id);
            continue;
        }
        if let Some(schema) = catalog.get(slot.node_type) {
            let raw = read_slot_payload(pool, &slot)?;
            let values = decode_record(schema, &raw)?;
            index.insert_record(schema, node_id, &values);
        }
    }
    Ok(())
}
