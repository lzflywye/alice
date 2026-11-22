use crate::error::HdbError;
use crate::node::{NODE_SLOT_SIZE, NodeSlot};
use std::fs::{File, OpenOptions};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<(), HdbError> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let n = file.read_at(buf, offset)?;
        #[cfg(windows)]
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(HdbError::CorruptedData("Unexpected EOF during read_at"));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> Result<(), HdbError> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let n = file.write_at(buf, offset)?;
        #[cfg(windows)]
        let n = file.seek_write(buf, offset)?;
        if n == 0 {
            return Err(HdbError::CorruptedData(
                "Zero bytes written during write_at",
            ));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Manages disk files for fixed-size node slots and append-only variable heap.
pub struct DiskStorage {
    nodes_file: File,
    heap_file: File,
}

impl DiskStorage {
    pub fn open(dir: &Path) -> Result<Self, HdbError> {
        std::fs::create_dir_all(dir)?;
        let nodes_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Preserve existing database slots
            .open(dir.join("nodes.db"))?;
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false) // Preserve existing variable-length heap data
            .open(dir.join("heap.db"))?;

        // Reserve NodeId 0 (first 256 bytes) if nodes.db is brand new
        if nodes_file.metadata()?.len() == 0 {
            let zero_slot = NodeSlot::empty().to_bytes();
            write_all_at(&nodes_file, &zero_slot, 0)?;
            nodes_file.sync_data()?;
        }

        Ok(Self {
            nodes_file,
            heap_file,
        })
    }

    pub fn node_count(&self) -> Result<u64, HdbError> {
        let len = self.nodes_file.metadata()?.len();
        Ok(len / (NODE_SLOT_SIZE as u64))
    }

    pub fn heap_len(&self) -> Result<u64, HdbError> {
        Ok(self.heap_file.metadata()?.len())
    }

    pub fn read_slot(&self, node_id: u64) -> Result<NodeSlot, HdbError> {
        let mut buf = [0u8; NODE_SLOT_SIZE];
        let offset = node_id * (NODE_SLOT_SIZE as u64);
        read_exact_at(&self.nodes_file, &mut buf, offset)?;
        Ok(NodeSlot::from_bytes(&buf))
    }

    pub fn write_slot(&self, node_id: u64, slot: &NodeSlot) -> Result<(), HdbError> {
        let offset = node_id * (NODE_SLOT_SIZE as u64);
        write_all_at(&self.nodes_file, &slot.to_bytes(), offset)
    }

    pub fn read_heap(&self, offset: u64, len: u32) -> Result<Vec<u8>, HdbError> {
        let mut buf = vec![0u8; len as usize];
        read_exact_at(&self.heap_file, &mut buf, offset)?;
        Ok(buf)
    }

    pub fn write_heap_at(&self, offset: u64, data: &[u8]) -> Result<(), HdbError> {
        write_all_at(&self.heap_file, data, offset)
    }

    pub fn sync_all(&self) -> Result<(), HdbError> {
        self.nodes_file.sync_data()?;
        self.heap_file.sync_data()?;
        Ok(())
    }

    /// Atomically replaces `nodes.db` and `heap.db` with compacted files and reopens handles.
    pub fn replace_with_compacted(
        &mut self,
        dir: &Path,
        new_nodes_path: &Path,
        new_heap_path: &Path,
    ) -> Result<(), HdbError> {
        // Close existing file descriptors before renaming (required on Windows, clean on Unix)
        let dummy_nodes = OpenOptions::new().read(true).open(new_nodes_path)?;
        let dummy_heap = OpenOptions::new().read(true).open(new_heap_path)?;
        self.nodes_file = dummy_nodes;
        self.heap_file = dummy_heap;

        std::fs::rename(new_nodes_path, dir.join("nodes.db"))?;
        std::fs::rename(new_heap_path, dir.join("heap.db"))?;

        self.nodes_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("nodes.db"))?;
        self.heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("heap.db"))?;

        Ok(())
    }

    /// Reads the entire contents of `nodes.db` and `heap.db` for base backup snapshots.
    pub fn read_all_files(&self) -> Result<(Vec<u8>, Vec<u8>), HdbError> {
        let node_bytes_len = self.nodes_file.metadata()?.len() as usize;
        let mut nodes_buf = vec![0u8; node_bytes_len];
        if !nodes_buf.is_empty() {
            read_exact_at(&self.nodes_file, &mut nodes_buf, 0)?;
        }

        let heap_bytes_len = self.heap_file.metadata()?.len() as usize;
        let mut heap_buf = vec![0u8; heap_bytes_len];
        if !heap_buf.is_empty() {
            read_exact_at(&self.heap_file, &mut heap_buf, 0)?;
        }

        Ok((nodes_buf, heap_buf))
    }
}
