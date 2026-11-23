use common::DbError;
use storage_slotted::{NodeId, NodeSlot, SLOT_SIZE};
use wal::WalRecord;

#[derive(Debug, Clone)]
pub enum SlotOp {
    WriteSlot {
        node_id: NodeId,
        slot: Box<NodeSlot>,
    },
    WriteHeap {
        offset: u64,
        data: Vec<u8>,
    },
}

pub fn encode_slot_op(op: &SlotOp) -> Vec<u8> {
    match op {
        SlotOp::WriteSlot { node_id, slot } => {
            let mut out = Vec::with_capacity(1 + 8 + SLOT_SIZE);
            out.push(0x01);
            out.extend_from_slice(&node_id.to_le_bytes());
            out.extend_from_slice(&slot.to_bytes());
            out
        }
        SlotOp::WriteHeap { offset, data } => {
            let mut out = Vec::with_capacity(1 + 8 + 4 + data.len());
            out.push(0x02);
            out.extend_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(&(data.len() as u32).to_le_bytes());
            out.extend_from_slice(data);
            out
        }
    }
}

pub fn decode_slot_op(record: &WalRecord) -> Result<SlotOp, DbError> {
    let b = &record.payload;
    if b.is_empty() {
        return Err(DbError::CorruptedData("Empty hierarchical WAL payload"));
    }
    match b[0] {
        0x01 => {
            if b.len() != 1 + 8 + SLOT_SIZE {
                return Err(DbError::CorruptedData("Invalid WriteSlot payload length"));
            }
            let node_id = u64::from_le_bytes(b[1..9].try_into().unwrap());
            let slot_bytes: [u8; SLOT_SIZE] = b[9..9 + SLOT_SIZE].try_into().unwrap();
            Ok(SlotOp::WriteSlot {
                node_id,
                slot: Box::new(NodeSlot::from_bytes(&slot_bytes)),
            })
        }
        0x02 => {
            if b.len() < 13 {
                return Err(DbError::CorruptedData("Truncated WriteHeap payload"));
            }
            let offset = u64::from_le_bytes(b[1..9].try_into().unwrap());
            let len = u32::from_le_bytes(b[9..13].try_into().unwrap()) as usize;
            if b.len() != 13 + len {
                return Err(DbError::CorruptedData("Invalid WriteHeap data length"));
            }
            Ok(SlotOp::WriteHeap {
                offset,
                data: b[13..].to_vec(),
            })
        }
        _ => Err(DbError::CorruptedData("Unknown hierarchical WAL tag")),
    }
}
