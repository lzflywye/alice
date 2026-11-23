use crate::node::{NodeId, NodeSlot, SLOT_SIZE};
use crate::storage::{DiskStorage, PAGE_SIZE, SLOTS_PER_PAGE};
use common::DbError;
use std::collections::HashMap;

pub const DEFAULT_POOL_PAGES: usize = 256; // 256 * 4KB = 1 MB default cache

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    pub capacity_pages: usize,
    pub cached_pages: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

struct Frame {
    page_id: u64,
    data: [u8; PAGE_SIZE],
    is_dirty: bool,
    last_used: u64,
}

/// Fixed-capacity 4KB LRU page cache over `DiskStorage`.
/// Eliminates the need to keep all `NodeSlot` records resident in RAM.
pub struct SlottedBufferPool {
    storage: DiskStorage,
    capacity: usize,
    frames: Vec<Frame>,
    page_table: HashMap<u64, usize>,
    tick: u64,
    total_slots: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
}

impl SlottedBufferPool {
    pub fn new(storage: DiskStorage, capacity_pages: usize) -> Result<Self, DbError> {
        let file_len = storage.nodes_file_len()?;
        let total_slots = file_len / (SLOT_SIZE as u64);
        Ok(Self {
            storage,
            capacity: capacity_pages.max(1),
            frames: Vec::with_capacity(capacity_pages.max(1)),
            page_table: HashMap::new(),
            tick: 0,
            total_slots,
            hits: 0,
            misses: 0,
            evictions: 0,
        })
    }

    pub fn total_slots(&self) -> u64 {
        self.total_slots
    }

    pub fn stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            capacity_pages: self.capacity,
            cached_pages: self.frames.len(),
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }

    /// Reads a single `NodeSlot` via the 4KB page cache.
    pub fn get_slot(&mut self, node_id: NodeId) -> Result<NodeSlot, DbError> {
        let page_id = node_id / (SLOTS_PER_PAGE as u64);
        let slot_offset = ((node_id as usize) % SLOTS_PER_PAGE) * SLOT_SIZE;
        let frame_idx = self.fetch_frame(page_id)?;
        let slice: &[u8; SLOT_SIZE] = self.frames[frame_idx].data
            [slot_offset..slot_offset + SLOT_SIZE]
            .try_into()
            .unwrap();
        Ok(NodeSlot::from_bytes(slice))
    }

    /// Updates a single `NodeSlot` in the page cache and marks the 4KB page dirty.
    pub fn put_slot(&mut self, node_id: NodeId, slot: &NodeSlot) -> Result<(), DbError> {
        let page_id = node_id / (SLOTS_PER_PAGE as u64);
        let slot_offset = ((node_id as usize) % SLOTS_PER_PAGE) * SLOT_SIZE;
        let frame_idx = self.fetch_frame(page_id)?;
        self.frames[frame_idx].data[slot_offset..slot_offset + SLOT_SIZE]
            .copy_from_slice(&slot.to_bytes());
        self.frames[frame_idx].is_dirty = true;
        if node_id + 1 > self.total_slots {
            self.total_slots = node_id + 1;
        }
        Ok(())
    }

    /// Scans all active slot IDs up to `total_slots` via cached pages (for startup index rebuild / vacuum).
    pub fn scan_all_slots<F>(&mut self, mut f: F) -> Result<(), DbError>
    where
        F: FnMut(NodeId, &NodeSlot) -> Result<(), DbError>,
    {
        for node_id in 0..self.total_slots {
            let slot = self.get_slot(node_id)?;
            f(node_id, &slot)?;
        }
        Ok(())
    }

    /// Trims trailing inactive slots when vacuuming.
    pub fn set_total_slots(&mut self, new_total: u64) {
        self.total_slots = new_total;
    }

    /// Flushes all dirty pages in the buffer pool to `nodes.db` and syncs disk.
    pub fn flush_all(&mut self) -> Result<(), DbError> {
        let mut any_dirty = false;
        for frame in &mut self.frames {
            if frame.is_dirty {
                self.storage.write_page(frame.page_id, &frame.data)?;
                frame.is_dirty = false;
                any_dirty = true;
            }
        }
        if any_dirty {
            self.storage.sync_nodes()?;
        }
        Ok(())
    }

    pub fn heap_len(&self) -> Result<u64, DbError> {
        self.storage.heap_len()
    }

    pub fn write_heap_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DbError> {
        self.storage.write_heap_at(offset, data)
    }

    pub fn read_heap(&self, offset: u64, len: u32) -> Result<Vec<u8>, DbError> {
        self.storage.read_heap(offset, len)
    }

    /// Atomically replaces underlying `nodes.db` and `heap.db` (for snapshot install / vacuum)
    /// and invalidates stale cached frames.
    pub fn replace_files(&mut self, nodes_bytes: &[u8], heap_bytes: &[u8]) -> Result<(), DbError> {
        self.frames.clear();
        self.page_table.clear();
        self.storage.replace_files(nodes_bytes, heap_bytes)?;
        self.total_slots = (nodes_bytes.len() / SLOT_SIZE) as u64;
        Ok(())
    }

    fn fetch_frame(&mut self, page_id: u64) -> Result<usize, DbError> {
        self.tick += 1;
        let now = self.tick;

        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.hits += 1;
            self.frames[frame_idx].last_used = now;
            return Ok(frame_idx);
        }

        self.misses += 1;
        let data = self.storage.read_page(page_id)?;

        if self.frames.len() < self.capacity {
            let idx = self.frames.len();
            self.frames.push(Frame {
                page_id,
                data,
                is_dirty: false,
                last_used: now,
            });
            self.page_table.insert(page_id, idx);
            return Ok(idx);
        }

        // Evict least-recently-used (LRU) frame
        let victim_idx = self
            .frames
            .iter()
            .enumerate()
            .min_by_key(|(_, f)| f.last_used)
            .map(|(i, _)| i)
            .unwrap();

        if self.frames[victim_idx].is_dirty {
            self.storage.write_page(
                self.frames[victim_idx].page_id,
                &self.frames[victim_idx].data,
            )?;
            self.storage.sync_nodes()?;
        }

        let old_page_id = self.frames[victim_idx].page_id;
        self.page_table.remove(&old_page_id);
        self.evictions += 1;

        self.frames[victim_idx] = Frame {
            page_id,
            data,
            is_dirty: false,
            last_used: now,
        };
        self.page_table.insert(page_id, victim_idx);
        Ok(victim_idx)
    }
}
