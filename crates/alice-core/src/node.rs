pub const NODE_SLOT_SIZE: usize = 256;
pub const INLINE_PAYLOAD_CAP: usize = 204;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct NodeSlot {
    // --- Header & Metadata (8 bytes) ---
    pub flags: u8,              // 0x00: Free (in free list), 0x01: Active
    pub _reserved: u8,          // Reserved for future flags / alignment
    pub node_type: u16,         // SegmentSchema identifier
    pub total_payload_len: u32, // Total byte length of encoded inline record

    // --- Hierarchy Pointers (32 bytes) ---
    pub parent_id: u64,       // Parent NodeId (0 if root node)
    pub first_child_id: u64,  // First child NodeId (0 if leaf node)
    pub next_sibling_id: u64, // Next sibling NodeId (0 if tail)
    pub prev_sibling_id: u64, // Previous sibling NodeId (0 if head, enables O(1) delete)

    // --- Inline Payload Area (216 bytes total with padding) ---
    pub inline_data: [u8; INLINE_PAYLOAD_CAP],
    pub _padding: [u8; 12], // Align struct size to exactly 256 bytes
}

// Compile-time assertion to guarantee exact 256-byte slot alignment
const _: () = assert!(std::mem::size_of::<NodeSlot>() == NODE_SLOT_SIZE);

impl NodeSlot {
    pub const FLAG_FREE: u8 = 0x00;
    pub const FLAG_ACTIVE: u8 = 0x01;

    pub fn empty() -> Self {
        Self {
            flags: Self::FLAG_FREE,
            _reserved: 0,
            node_type: 0,
            total_payload_len: 0,
            parent_id: 0,
            first_child_id: 0,
            next_sibling_id: 0,
            prev_sibling_id: 0,
            inline_data: [0; INLINE_PAYLOAD_CAP],
            _padding: [0; 12],
        }
    }

    pub fn is_active(&self) -> bool {
        self.flags == Self::FLAG_ACTIVE
    }

    /// Serializes this 256-byte slot into a fixed byte array (little-endian).
    pub fn to_bytes(&self) -> [u8; NODE_SLOT_SIZE] {
        let mut buf = [0u8; NODE_SLOT_SIZE];
        buf[0] = self.flags;
        buf[1] = self._reserved;
        buf[2..4].copy_from_slice(&self.node_type.to_le_bytes());
        buf[4..8].copy_from_slice(&self.total_payload_len.to_le_bytes());
        buf[8..16].copy_from_slice(&self.parent_id.to_le_bytes());
        buf[16..24].copy_from_slice(&self.first_child_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.next_sibling_id.to_le_bytes());
        buf[32..40].copy_from_slice(&self.prev_sibling_id.to_le_bytes());
        buf[40..40 + INLINE_PAYLOAD_CAP].copy_from_slice(&self.inline_data);
        buf
    }

    /// Deserializes a 256-byte buffer into a NodeSlot.
    pub fn from_bytes(buf: &[u8; NODE_SLOT_SIZE]) -> Self {
        let mut inline_data = [0u8; INLINE_PAYLOAD_CAP];
        inline_data.copy_from_slice(&buf[40..40 + INLINE_PAYLOAD_CAP]);
        Self {
            flags: buf[0],
            _reserved: buf[1],
            node_type: u16::from_le_bytes(buf[2..4].try_into().unwrap()),
            total_payload_len: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            parent_id: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            first_child_id: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            next_sibling_id: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            prev_sibling_id: u64::from_le_bytes(buf[32..40].try_into().unwrap()),
            inline_data,
            _padding: [0; 12],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_slot_roundtrip() {
        let mut slot = NodeSlot::empty();
        slot.flags = NodeSlot::FLAG_ACTIVE;
        slot.node_type = 42;
        slot.total_payload_len = 128;
        slot.parent_id = 10;
        slot.first_child_id = 20;
        slot.next_sibling_id = 30;
        slot.prev_sibling_id = 40;
        slot.inline_data[0] = 0xAA;
        slot.inline_data[INLINE_PAYLOAD_CAP - 1] = 0xBB;

        let bytes = slot.to_bytes();
        let decoded = NodeSlot::from_bytes(&bytes);

        assert_eq!(decoded.flags, NodeSlot::FLAG_ACTIVE);
        assert_eq!(decoded.node_type, 42);
        assert_eq!(decoded.total_payload_len, 128);
        assert_eq!(decoded.parent_id, 10);
        assert_eq!(decoded.first_child_id, 20);
        assert_eq!(decoded.next_sibling_id, 30);
        assert_eq!(decoded.prev_sibling_id, 40);
        assert_eq!(decoded.inline_data[0], 0xAA);
        assert_eq!(decoded.inline_data[INLINE_PAYLOAD_CAP - 1], 0xBB);
    }
}
