pub mod buffer_pool;
pub mod node;
pub mod storage;

pub use buffer_pool::{BufferPoolStats, DEFAULT_POOL_PAGES, SlottedBufferPool};
pub use node::{HEADER_SIZE, INLINE_CAPACITY, NodeId, NodeSlot, SLOT_SIZE};
pub use storage::{DiskStorage, PAGE_SIZE, SLOTS_PER_PAGE};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_buffer_pool_lru_eviction_and_dirty_flush() {
        let dir = std::env::temp_dir().join(format!("bp_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);

        {
            let disk = DiskStorage::open(&dir).unwrap();
            // Tiny 2-page buffer pool (holds 32 slots max in RAM at once)
            let mut pool = SlottedBufferPool::new(disk, 2).unwrap();

            // Write 64 slots across 4 pages (pages 0, 1, 2, 3) -> forces LRU evictions
            for id in 0..64u64 {
                let mut slot = NodeSlot::new_active(1, 0);
                slot.set_inline_payload(format!("node_{id}").as_bytes());
                pool.put_slot(id, &slot).unwrap();
            }

            let stats = pool.stats();
            assert_eq!(stats.cached_pages, 2);
            assert!(stats.evictions >= 2);
            assert!(stats.hits > 0);

            // Flush remaining dirty pages before drop
            pool.flush_all().unwrap();
        }

        // Reopen with fresh pool and verify all 64 slots persisted accurately across evictions
        {
            let disk = DiskStorage::open(&dir).unwrap();
            let mut pool = SlottedBufferPool::new(disk, 4).unwrap();
            assert_eq!(pool.total_slots(), 64);
            for id in 0..64u64 {
                let slot = pool.get_slot(id).unwrap();
                assert_eq!(slot.inline_payload(), format!("node_{id}").as_bytes());
            }
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
