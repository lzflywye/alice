use common::DbError;

pub type NodeId = u64;

pub const SLOT_SIZE: usize = 256;
pub const HEADER_SIZE: usize = 52;
pub const INLINE_CAPACITY: usize = SLOT_SIZE - HEADER_SIZE; // 204 bytes

const FLAG_ACTIVE: u16 = 0x0001;
const FLAG_HEAP: u16 = 0x0002;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeSlot {
    // 8-byte aligned fields first (4 * 8 = 32 bytes, offset 0..32)
    pub parent_id: NodeId,
    pub first_child_id: NodeId,
    pub prev_sibling_id: NodeId,
    pub next_sibling_id: NodeId,
    pub heap_offset: u64, // 5 * 8 = 40 bytes (offset 0..40)

    // 4-byte aligned fields next (2 * 4 = 8 bytes, offset 40..48)
    pub payload_len: u32,
    pub heap_crc32: u32,

    // 2-byte aligned fields next (2 * 2 = 4 bytes, offset 48..52)
    pub flags: u16,
    pub node_type: u16,

    // 1-byte aligned payload (204 bytes, offset 52..256)
    pub inline_data: [u8; INLINE_CAPACITY],
}

const _: () = assert!(std::mem::size_of::<NodeSlot>() == SLOT_SIZE);

impl Default for NodeSlot {
    fn default() -> Self {
        Self {
            flags: 0,
            node_type: 0,
            parent_id: 0,
            first_child_id: 0,
            prev_sibling_id: 0,
            next_sibling_id: 0,
            payload_len: 0,
            heap_offset: 0,
            heap_crc32: 0,
            inline_data: [0u8; INLINE_CAPACITY],
        }
    }
}

impl NodeSlot {
    pub fn new_root() -> Self {
        Self {
            flags: FLAG_ACTIVE,
            node_type: 0,
            ..Self::default()
        }
    }

    pub fn new_active(node_type: u16, parent_id: NodeId) -> Self {
        Self {
            flags: FLAG_ACTIVE,
            node_type,
            parent_id,
            ..Self::default()
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        (self.flags & FLAG_ACTIVE) != 0
    }

    #[inline]
    pub fn is_heap_payload(&self) -> bool {
        (self.flags & FLAG_HEAP) != 0
    }

    pub fn set_inline_payload(&mut self, data: &[u8]) {
        debug_assert!(data.len() <= INLINE_CAPACITY);
        self.flags &= !FLAG_HEAP;
        self.payload_len = data.len() as u32;
        self.heap_offset = 0;
        self.heap_crc32 = 0;
        self.inline_data.fill(0);
        self.inline_data[..data.len()].copy_from_slice(data);
    }

    pub fn inline_payload(&self) -> &[u8] {
        let len = (self.payload_len as usize).min(INLINE_CAPACITY);
        &self.inline_data[..len]
    }

    pub fn set_heap_ref(&mut self, offset: u64, len: u32, crc: u32) {
        self.flags |= FLAG_HEAP;
        self.payload_len = len;
        self.heap_offset = offset;
        self.heap_crc32 = crc;
        self.inline_data.fill(0);
    }

    pub fn heap_ref(&self) -> (u64, u32, u32) {
        (self.heap_offset, self.payload_len, self.heap_crc32)
    }

    pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
        let mut buf = [0u8; SLOT_SIZE];
        buf[0..2].copy_from_slice(&self.flags.to_le_bytes());
        buf[2..4].copy_from_slice(&self.node_type.to_le_bytes());
        buf[4..12].copy_from_slice(&self.parent_id.to_le_bytes());
        buf[12..20].copy_from_slice(&self.first_child_id.to_le_bytes());
        buf[20..28].copy_from_slice(&self.prev_sibling_id.to_le_bytes());
        buf[28..36].copy_from_slice(&self.next_sibling_id.to_le_bytes());
        buf[36..40].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[40..48].copy_from_slice(&self.heap_offset.to_le_bytes());
        buf[48..52].copy_from_slice(&self.heap_crc32.to_le_bytes());
        buf[52..SLOT_SIZE].copy_from_slice(&self.inline_data);
        buf
    }

    pub fn from_bytes(buf: &[u8; SLOT_SIZE]) -> Self {
        let mut inline_data = [0u8; INLINE_CAPACITY];
        inline_data.copy_from_slice(&buf[52..SLOT_SIZE]);
        Self {
            flags: u16::from_le_bytes([buf[0], buf[1]]),
            node_type: u16::from_le_bytes([buf[2], buf[3]]),
            parent_id: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
            first_child_id: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            prev_sibling_id: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            next_sibling_id: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            payload_len: u32::from_le_bytes(buf[36..40].try_into().unwrap()),
            heap_offset: u64::from_le_bytes(buf[40..48].try_into().unwrap()),
            heap_crc32: u32::from_le_bytes(buf[48..52].try_into().unwrap()),
            inline_data,
        }
    }

    pub fn try_from_slice(slice: &[u8]) -> Result<Self, DbError> {
        let arr: &[u8; SLOT_SIZE] = slice
            .try_into()
            .map_err(|_| DbError::CorruptedData("Invalid NodeSlot byte slice length"))?;
        Ok(Self::from_bytes(arr))
    }
}
