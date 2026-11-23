use crate::node::{NodeId, NodeSlot, SLOT_SIZE};
use common::DbError;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const PAGE_SIZE: usize = 4096;
pub const SLOTS_PER_PAGE: usize = PAGE_SIZE / SLOT_SIZE; // 16 slots per 4KB page

/// Low-level persistent file manager for `nodes.db` (4KB pages of fixed-size slots)
/// and `heap.db` (variable-length overflow payloads).
pub struct DiskStorage {
    nodes_path: PathBuf,
    heap_path: PathBuf,
    nodes_file: File,
    heap_file: File,
}

impl DiskStorage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, DbError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;

        let nodes_path = dir.join("nodes.db");
        let heap_path = dir.join("heap.db");

        let nodes_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&nodes_path)?;

        let heap_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&heap_path)?;

        Ok(Self {
            nodes_path,
            heap_path,
            nodes_file,
            heap_file,
        })
    }

    pub fn nodes_file_len(&self) -> Result<u64, DbError> {
        Ok(self.nodes_file.metadata()?.len())
    }

    /// Reads a 4KB page from `nodes.db`. Zero-fills if reading beyond current EOF.
    pub fn read_page(&mut self, page_id: u64) -> Result<[u8; PAGE_SIZE], DbError> {
        let offset = page_id * (PAGE_SIZE as u64);
        let file_len = self.nodes_file_len()?;
        let mut buf = [0u8; PAGE_SIZE];
        if offset >= file_len {
            return Ok(buf);
        }

        self.nodes_file.seek(SeekFrom::Start(offset))?;
        let readable = ((file_len - offset) as usize).min(PAGE_SIZE);
        self.nodes_file.read_exact(&mut buf[..readable])?;
        Ok(buf)
    }

    /// Writes a 4KB page to `nodes.db`.
    pub fn write_page(&mut self, page_id: u64, data: &[u8; PAGE_SIZE]) -> Result<(), DbError> {
        let offset = page_id * (PAGE_SIZE as u64);
        self.nodes_file.seek(SeekFrom::Start(offset))?;
        self.nodes_file.write_all(data)?;
        Ok(())
    }

    pub fn sync_nodes(&mut self) -> Result<(), DbError> {
        self.nodes_file.sync_data()?;
        Ok(())
    }

    pub fn load_all_slots(&mut self) -> Result<Vec<NodeSlot>, DbError> {
        self.nodes_file.seek(SeekFrom::Start(0))?;
        let mut raw = Vec::new();
        self.nodes_file.read_to_end(&mut raw)?;

        let (chunks, remainder) = raw.as_chunks::<SLOT_SIZE>();
        if !remainder.is_empty() {
            return Err(DbError::CorruptedData(
                "nodes.db size is not a multiple of SLOT_SIZE",
            ));
        }

        let mut slots = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            slots.push(NodeSlot::from_bytes(chunk));
        }
        Ok(slots)
    }

    pub fn write_slot(&mut self, node_id: NodeId, slot: &NodeSlot) -> Result<(), DbError> {
        let offset = node_id * (SLOT_SIZE as u64);
        self.nodes_file.seek(SeekFrom::Start(offset))?;
        self.nodes_file.write_all(&slot.to_bytes())?;
        self.nodes_file.sync_data()?;
        Ok(())
    }

    pub fn heap_len(&self) -> Result<u64, DbError> {
        Ok(self.heap_file.metadata()?.len())
    }

    pub fn write_heap_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DbError> {
        self.heap_file.seek(SeekFrom::Start(offset))?;
        self.heap_file.write_all(data)?;
        self.heap_file.sync_data()?;
        Ok(())
    }

    pub fn read_heap(&self, offset: u64, len: u32) -> Result<Vec<u8>, DbError> {
        let mut f = File::open(&self.heap_path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn replace_files(&mut self, nodes_bytes: &[u8], heap_bytes: &[u8]) -> Result<(), DbError> {
        let tmp_nodes = self.nodes_path.with_extension("db.tmp");
        let tmp_heap = self.heap_path.with_extension("db.tmp");

        {
            let mut f = File::create(&tmp_nodes)?;
            f.write_all(nodes_bytes)?;
            f.sync_data()?;
        }
        {
            let mut f = File::create(&tmp_heap)?;
            f.write_all(heap_bytes)?;
            f.sync_data()?;
        }

        fs::rename(&tmp_nodes, &self.nodes_path)?;
        fs::rename(&tmp_heap, &self.heap_path)?;

        self.nodes_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.nodes_path)?;

        self.heap_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.heap_path)?;

        Ok(())
    }
}
