use crate::schema::{DataType, Decimal, SegmentSchema, Value};
use common::DbError;
use storage_slotted::INLINE_CAPACITY;

pub enum EncodedPayload {
    Inline([u8; INLINE_CAPACITY], u16),
    Heap(Vec<u8>),
}

pub fn encode_record(schema: &SegmentSchema, values: &[Value]) -> Result<EncodedPayload, DbError> {
    schema.validate_values(values)?;

    let col_count = schema.columns.len();
    let bitmap_bytes = col_count.div_ceil(8);
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&vec![0u8; bitmap_bytes]);

    for (idx, (col, val)) in schema.columns.iter().zip(values.iter()).enumerate() {
        if matches!(val, Value::Null) {
            buf[idx / 8] |= 1 << (idx % 8);
            continue;
        }

        match (&col.data_type, val) {
            (DataType::Bool, Value::Bool(b)) => buf.push(u8::from(*b)),
            (DataType::I64, Value::I64(n)) => buf.extend_from_slice(&n.to_le_bytes()),
            (DataType::F64, Value::F64(f)) => buf.extend_from_slice(&f.to_le_bytes()),
            (DataType::Decimal, Value::Decimal(d)) => {
                buf.extend_from_slice(&d.unscaled.to_le_bytes());
                buf.push(d.scale);
            }
            (DataType::FixedString(max), Value::String(s)) => {
                let raw = s.as_bytes();
                buf.extend_from_slice(&(raw.len() as u16).to_le_bytes());
                buf.extend_from_slice(raw);
                buf.resize(buf.len() + (*max as usize - raw.len()), 0);
            }
            (DataType::FixedBytes(max), Value::Bytes(b)) => {
                buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
                buf.extend_from_slice(b);
                buf.resize(buf.len() + (*max as usize - b.len()), 0);
            }
            (DataType::VarString, Value::String(s)) => {
                let raw = s.as_bytes();
                buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw);
            }
            (DataType::VarBytes, Value::Bytes(b)) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            _ => unreachable!(),
        }
    }

    if buf.len() <= INLINE_CAPACITY {
        let mut arr = [0u8; INLINE_CAPACITY];
        arr[..buf.len()].copy_from_slice(&buf);
        Ok(EncodedPayload::Inline(arr, buf.len() as u16))
    } else {
        Ok(EncodedPayload::Heap(buf))
    }
}

pub fn decode_record(schema: &SegmentSchema, bytes: &[u8]) -> Result<Vec<Value>, DbError> {
    let col_count = schema.columns.len();
    let bitmap_bytes = col_count.div_ceil(8);
    if bytes.len() < bitmap_bytes {
        return Err(DbError::CorruptedData("Record shorter than null bitmap"));
    }

    let bitmap = &bytes[..bitmap_bytes];
    let mut cur = bitmap_bytes;
    let mut out = Vec::with_capacity(col_count);

    for (idx, col) in schema.columns.iter().enumerate() {
        let is_null = (bitmap[idx / 8] & (1 << (idx % 8))) != 0;
        if is_null {
            out.push(Value::Null);
            continue;
        }

        match col.data_type {
            DataType::Bool => {
                if cur + 1 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated Bool"));
                }
                out.push(Value::Bool(bytes[cur] != 0));
                cur += 1;
            }
            DataType::I64 => {
                if cur + 8 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated I64"));
                }
                let n = i64::from_le_bytes(bytes[cur..cur + 8].try_into().unwrap());
                out.push(Value::I64(n));
                cur += 8;
            }
            DataType::F64 => {
                if cur + 8 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated F64"));
                }
                let f = f64::from_le_bytes(bytes[cur..cur + 8].try_into().unwrap());
                out.push(Value::F64(f));
                cur += 8;
            }
            DataType::Decimal => {
                if cur + 17 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated Decimal"));
                }
                let unscaled = i128::from_le_bytes(bytes[cur..cur + 16].try_into().unwrap());
                let scale = bytes[cur + 16];
                out.push(Value::Decimal(Decimal::new(unscaled, scale)));
                cur += 17;
            }
            DataType::FixedString(max) => {
                let total = 2 + max as usize;
                if cur + total > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated FixedString"));
                }
                let actual_len =
                    u16::from_le_bytes(bytes[cur..cur + 2].try_into().unwrap()) as usize;
                if actual_len > max as usize {
                    return Err(DbError::CorruptedData("Invalid FixedString length"));
                }
                let s = std::str::from_utf8(&bytes[cur + 2..cur + 2 + actual_len])
                    .map_err(|_| DbError::InvalidUtf8)?;
                out.push(Value::String(s.to_string()));
                cur += total;
            }
            DataType::FixedBytes(max) => {
                let total = 2 + max as usize;
                if cur + total > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated FixedBytes"));
                }
                let actual_len =
                    u16::from_le_bytes(bytes[cur..cur + 2].try_into().unwrap()) as usize;
                if actual_len > max as usize {
                    return Err(DbError::CorruptedData("Invalid FixedBytes length"));
                }
                out.push(Value::Bytes(bytes[cur + 2..cur + 2 + actual_len].to_vec()));
                cur += total;
            }
            DataType::VarString => {
                if cur + 4 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated VarString length"));
                }
                let len = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                if cur + len > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated VarString data"));
                }
                let s = std::str::from_utf8(&bytes[cur..cur + len])
                    .map_err(|_| DbError::InvalidUtf8)?;
                out.push(Value::String(s.to_string()));
                cur += len;
            }
            DataType::VarBytes => {
                if cur + 4 > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated VarBytes length"));
                }
                let len = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                if cur + len > bytes.len() {
                    return Err(DbError::CorruptedData("Truncated VarBytes data"));
                }
                out.push(Value::Bytes(bytes[cur..cur + len].to_vec()));
                cur += len;
            }
        }
    }

    Ok(out)
}
