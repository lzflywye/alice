use crate::error::HdbError;
use crate::node::INLINE_PAYLOAD_CAP;
use crate::schema::{DataType, SegmentSchema, Value};

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
    let null_bitmap = u64::from_le_bytes(buf[0..8].try_into().unwrap());
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
                let v = i64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                Value::I64(v)
            }
            DataType::F64 => {
                let v = f64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                Value::F64(v)
            }
            DataType::FixedString(max_len) => {
                let max = max_len as usize;
                let actual_len =
                    u16::from_le_bytes(buf[cursor..cursor + 2].try_into().unwrap()) as usize;
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
                let offset = u64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
                let len = u32::from_le_bytes(buf[cursor + 8..cursor + 12].try_into().unwrap());
                cursor += 12;
                let raw = heap.read_bytes(offset, len)?;
                let s = std::str::from_utf8(&raw)
                    .map_err(|_| HdbError::InvalidUtf8)?
                    .to_string();
                Value::String(s)
            }
            DataType::VarBytes => {
                let offset = u64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
                let len = u32::from_le_bytes(buf[cursor + 8..cursor + 12].try_into().unwrap());
                cursor += 12;
                Value::Bytes(heap.read_bytes(offset, len)?)
            }
        };
        out.push(val);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use core::f64;

    use super::*;
    use crate::schema::ColumnDef;

    /// Simple in-memory reader for isolated record unit tests.
    struct MemHeap(Vec<u8>);
    impl HeapReader for MemHeap {
        fn read_bytes(&self, offset: u64, len: u32) -> Result<Vec<u8>, HdbError> {
            let start = offset as usize;
            Ok(self.0[start..start + (len as usize)].to_vec())
        }
    }

    fn all_types_schema() -> SegmentSchema {
        SegmentSchema {
            node_type: 1,
            name: "AllTypes".into(),
            parent_type: None,
            columns: vec![
                ColumnDef {
                    name: "b".into(),
                    data_type: DataType::Bool,
                    nullable: false,
                },
                ColumnDef {
                    name: "i".into(),
                    data_type: DataType::I64,
                    nullable: false,
                },
                ColumnDef {
                    name: "f".into(),
                    data_type: DataType::F64,
                    nullable: true,
                },
                ColumnDef {
                    name: "fs".into(),
                    data_type: DataType::FixedString(10),
                    nullable: false,
                },
                ColumnDef {
                    name: "fb".into(),
                    data_type: DataType::FixedBytes(4),
                    nullable: false,
                },
                ColumnDef {
                    name: "vs".into(),
                    data_type: DataType::VarString,
                    nullable: true,
                },
                ColumnDef {
                    name: "vb".into(),
                    data_type: DataType::VarBytes,
                    nullable: true,
                },
            ],
        }
    }

    #[test]
    fn test_encode_decode_all_types() {
        let schema = all_types_schema();
        let mut appender = StagedHeapAppender::new(0);

        let input = vec![
            Value::Bool(true),
            Value::I64(-9876543210),
            Value::F64(f64::consts::PI),
            Value::String("hi\0null".into()), // Contains embedded null byte
            Value::Bytes(vec![1, 2, 3, 4]),
            Value::String("variable length text".into()),
            Value::Bytes(vec![0xFF; 50]),
        ];

        let (buf, _) = encode_record(&schema, &input, &mut appender).unwrap();
        let heap = MemHeap(appender.staged_data);
        let decoded = decode_record(&schema, &buf, &heap).unwrap();

        assert_eq!(input, decoded);
    }

    #[test]
    fn test_null_handling_and_constraints() {
        let schema = all_types_schema();
        let mut appender = StagedHeapAppender::new(0);

        // Nullable columns set to Null
        let valid_nulls = vec![
            Value::Bool(false),
            Value::I64(1),
            Value::Null,
            Value::String("ok".into()),
            Value::Bytes(vec![0; 4]),
            Value::Null,
            Value::Null,
        ];
        let (buf, _) = encode_record(&schema, &valid_nulls, &mut appender).unwrap();
        let decoded = decode_record(&schema, &buf, &MemHeap(appender.staged_data)).unwrap();
        assert_eq!(valid_nulls, decoded);

        // NOT NULL column (index 0) set to Null must fail
        let mut invalid_nulls = valid_nulls;
        invalid_nulls[0] = Value::Null;
        let mut appender2 = StagedHeapAppender::new(0);
        let err = encode_record(&schema, &invalid_nulls, &mut appender2).unwrap_err();
        assert!(matches!(
            err,
            HdbError::NullConstraintViolation { col_index: 0 }
        ));
    }

    #[test]
    fn test_fixed_length_and_type_errors() {
        let schema = all_types_schema();
        let mut appender = StagedHeapAppender::new(0);

        // FixedString(10) exceeded
        let too_long = vec![
            Value::Bool(true),
            Value::I64(1),
            Value::Null,
            Value::String("12345678901".into()), // 11 bytes > 10
            Value::Bytes(vec![0; 4]),
            Value::Null,
            Value::Null,
        ];
        let err = encode_record(&schema, &too_long, &mut appender).unwrap_err();
        assert!(matches!(
            err,
            HdbError::FixedLengthExceeded { col_index: 3, .. }
        ));
    }
}
