use crate::error::HdbError;
use crate::node::INLINE_PAYLOAD_CAP;
use crate::schema::{DataType, Decimal, SegmentSchema, Value};

pub trait HeapReader {
    fn read_bytes(&self, offset: u64, len: u32) -> Result<Vec<u8>, HdbError>;
}

pub trait HeapAppender {
    fn append_bytes(&mut self, data: &[u8]) -> Result<u64, HdbError>;
}

/// Buffers new heap appends during a transaction so they can be logged to WAL before disk write.
pub struct StagedHeapAppender {
    base_offset: u64,
    pub staged_data: Vec<u8>,
}

impl StagedHeapAppender {
    pub fn new(current_heap_len: u64) -> Self {
        Self {
            base_offset: current_heap_len,
            staged_data: Vec::new(),
        }
    }

    pub fn start_offset(&self) -> u64 {
        self.base_offset
    }
}

impl HeapAppender for StagedHeapAppender {
    fn append_bytes(&mut self, data: &[u8]) -> Result<u64, HdbError> {
        let offset = self.base_offset + (self.staged_data.len() as u64);
        self.staged_data.extend_from_slice(data);
        Ok(offset)
    }
}

pub fn encode_record<H: HeapAppender>(
    schema: &SegmentSchema,
    values: &[Value],
    heap: &mut H,
) -> Result<([u8; INLINE_PAYLOAD_CAP], u32), HdbError> {
    if values.len() != schema.columns.len() {
        return Err(HdbError::ColumnCountMismatch {
            expected: schema.columns.len(),
            actual: values.len(),
        });
    }

    let mut buf = [0u8; INLINE_PAYLOAD_CAP];
    let mut null_bitmap: u64 = 0;
    let mut cursor: usize = 8;

    for (i, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
        if matches!(val, Value::Null) {
            if !col.nullable {
                return Err(HdbError::NullConstraintViolation { col_index: i });
            }
            null_bitmap |= 1u64 << i;
            continue;
        }

        match (col.data_type, val) {
            (DataType::Bool, Value::Bool(b)) => {
                buf[cursor] = if *b { 1 } else { 0 };
                cursor += 1;
            }
            (DataType::I64, Value::I64(v)) => {
                buf[cursor..cursor + 8].copy_from_slice(&v.to_le_bytes());
                cursor += 8;
            }
            (DataType::F64, Value::F64(v)) => {
                buf[cursor..cursor + 8].copy_from_slice(&v.to_le_bytes());
                cursor += 8;
            }
            (DataType::FixedString(max_len), Value::String(s)) => {
                let bytes = s.as_bytes();
                let max = max_len as usize;
                if bytes.len() > max {
                    return Err(HdbError::FixedLengthExceeded {
                        col_index: i,
                        max,
                        actual: bytes.len(),
                    });
                }
                let actual_len = bytes.len() as u16;
                buf[cursor..cursor + 2].copy_from_slice(&actual_len.to_le_bytes());
                buf[cursor + 2..cursor + 2 + bytes.len()].copy_from_slice(bytes);
                cursor += 2 + max;
            }
            (DataType::FixedBytes(max_len), Value::Bytes(b)) => {
                let max = max_len as usize;
                if b.len() != max {
                    return Err(HdbError::FixedLengthExceeded {
                        col_index: i,
                        max,
                        actual: b.len(),
                    });
                }
                buf[cursor..cursor + max].copy_from_slice(b);
                cursor += max;
            }
            (DataType::VarString, Value::String(s)) => {
                let bytes = s.as_bytes();
                let offset = heap.append_bytes(bytes)?;
                let len = bytes.len() as u32;
                buf[cursor..cursor + 8].copy_from_slice(&offset.to_le_bytes());
                buf[cursor + 8..cursor + 12].copy_from_slice(&len.to_le_bytes());
                cursor += 12;
            }
            (DataType::VarBytes, Value::Bytes(b)) => {
                let offset = heap.append_bytes(b)?;
                let len = b.len() as u32;
                buf[cursor..cursor + 8].copy_from_slice(&offset.to_le_bytes());
                buf[cursor + 8..cursor + 12].copy_from_slice(&len.to_le_bytes());
                cursor += 12;
            }
            (DataType::Decimal, Value::Decimal(d)) => {
                buf[cursor] = d.scale;
                buf[cursor + 1..cursor + 17].copy_from_slice(&d.unscaled.to_le_bytes());
                cursor += 17;
            }
            _ => return Err(HdbError::TypeMismatch { col_index: i }),
        }
    }

    buf[0..8].copy_from_slice(&null_bitmap.to_le_bytes());
    Ok((buf, cursor as u32))
}

pub fn decode_record<H: HeapReader>(
    schema: &SegmentSchema,
    buf: &[u8; INLINE_PAYLOAD_CAP],
    heap: &H,
) -> Result<Vec<Value>, HdbError> {
    let bitmap_bytes: [u8; 8] = buf[0..8]
        .try_into()
        .map_err(|_| HdbError::CorruptedData("Invalid null bitmap slice"))?;
    let null_bitmap = u64::from_le_bytes(bitmap_bytes);
    let mut cursor: usize = 8;
    let mut out = Vec::with_capacity(schema.columns.len());

    for (i, col) in schema.columns.iter().enumerate() {
        if (null_bitmap & (1u64 << i)) != 0 {
            out.push(Value::Null);
            continue;
        }

        let val = match col.data_type {
            DataType::Bool => {
                let v = buf[cursor] != 0;
                cursor += 1;
                Value::Bool(v)
            }
            DataType::I64 => {
                let arr: [u8; 8] = buf[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid I64 slice"))?;
                cursor += 8;
                Value::I64(i64::from_le_bytes(arr))
            }
            DataType::F64 => {
                let arr: [u8; 8] = buf[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid F64 slice"))?;
                cursor += 8;
                Value::F64(f64::from_le_bytes(arr))
            }
            DataType::FixedString(max_len) => {
                let max = max_len as usize;
                let len_arr: [u8; 2] = buf[cursor..cursor + 2]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid FixedString length"))?;
                let actual_len = u16::from_le_bytes(len_arr) as usize;
                if actual_len > max {
                    return Err(HdbError::CorruptedData("FixedString actual_len > max"));
                }
                let slice = &buf[cursor + 2..cursor + 2 + actual_len];
                let s = std::str::from_utf8(slice)
                    .map_err(|_| HdbError::InvalidUtf8)?
                    .to_string();
                cursor += 2 + max;
                Value::String(s)
            }
            DataType::FixedBytes(max_len) => {
                let max = max_len as usize;
                let b = buf[cursor..cursor + max].to_vec();
                cursor += max;
                Value::Bytes(b)
            }
            DataType::VarString => {
                let off_arr: [u8; 8] = buf[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid VarString offset"))?;
                let len_arr: [u8; 4] = buf[cursor + 8..cursor + 12]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid VarString len"))?;
                cursor += 12;
                let raw =
                    heap.read_bytes(u64::from_le_bytes(off_arr), u32::from_le_bytes(len_arr))?;
                let s = std::str::from_utf8(&raw)
                    .map_err(|_| HdbError::InvalidUtf8)?
                    .to_string();
                Value::String(s)
            }
            DataType::VarBytes => {
                let off_arr: [u8; 8] = buf[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid VarBytes offset"))?;
                let len_arr: [u8; 4] = buf[cursor + 8..cursor + 12]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid VarBytes len"))?;
                cursor += 12;
                Value::Bytes(
                    heap.read_bytes(u64::from_le_bytes(off_arr), u32::from_le_bytes(len_arr))?,
                )
            }
            DataType::Decimal => {
                let scale = buf[cursor];
                let unscaled_arr: [u8; 16] = buf[cursor + 1..cursor + 17]
                    .try_into()
                    .map_err(|_| HdbError::CorruptedData("Invalid Decimal slice"))?;
                cursor += 17;
                Value::Decimal(Decimal {
                    unscaled: i128::from_le_bytes(unscaled_arr),
                    scale,
                })
            }
        };
        out.push(val);
    }

    Ok(out)
}
