use crate::{ColumnSchema, TableSchema, numeric::{Numeric, RoundingMode}};
use common::DbError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Bool,
    Int,
    Float,
    Numeric {
        precision: u8,
        scale: u8,
    },
    Text,
    Blob,
    /// Days since Unix epoch (1970-01-01)
    Date,
    /// Microseconds since Unix epoch (1970-01-01T00:00:00Z)
    Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Numeric(Numeric),
    Text(String),
    Blob(Vec<u8>),
    Date(i32),
    Timestamp(i64),
}

impl SqlValue {
    pub fn coerce_to(&self, target: SqlType, mode: RoundingMode) -> Result<Self, DbError> {
        if matches!(self, SqlValue::Null) {
            return Ok(SqlValue::Null);
        }
        match (target, self) {
            (SqlType::Bool, SqlValue::Bool(b)) => Ok(SqlValue::Bool(*b)),
            (SqlType::Int, SqlValue::Int(n)) => Ok(SqlValue::Int(*n)),
            (SqlType::Float, SqlValue::Float(f)) => Ok(SqlValue::Float(*f)),
            (SqlType::Float, SqlValue::Int(n)) => Ok(SqlValue::Float(*n as f64)),
            (SqlType::Numeric { precision, scale }, SqlValue::Numeric(num)) => Ok(
                SqlValue::Numeric(num.fit_to_schema(precision, scale, mode)?),
            ),
            (SqlType::Numeric { precision, scale }, SqlValue::Int(n)) => {
                let num = Numeric::new(*n as i128, 0)?.fit_to_schema(precision, scale, mode)?;
                Ok(SqlValue::Numeric(num))
            }
            (SqlType::Text, SqlValue::Text(s)) => Ok(SqlValue::Text(s.clone())),
            (SqlType::Blob, SqlValue::Blob(b)) => Ok(SqlValue::Blob(b.clone())),
            (SqlType::Date, SqlValue::Date(d)) => Ok(SqlValue::Date(*d)),
            (SqlType::Date, SqlValue::Text(s)) => Ok(SqlValue::Date(parse_iso_date(s)?)),
            (SqlType::Timestamp, SqlValue::Timestamp(ts)) => Ok(SqlValue::Timestamp(*ts)),
            (SqlType::Timestamp, SqlValue::Text(s)) => {
                Ok(SqlValue::Timestamp(parse_iso_timestamp(s)?))
            }
            _ => Err(DbError::TypeMismatch {
                column: "value".into(),
                expected: "matching SQL type",
            }),
        }
    }
}

/// Encodes a `SqlValue` into a memcomparable byte slice so byte order matches SQL logical order.
pub fn encode_memcomparable_key(val: &SqlValue) -> Vec<u8> {
    let mut out = Vec::new();
    match val {
        SqlValue::Null => out.push(0x00),
        SqlValue::Bool(b) => {
            out.push(0x01);
            out.push(u8::from(*b));
        }
        SqlValue::Int(n) => {
            out.push(0x02);
            let flipped = (*n as u64) ^ (1u64 << 63);
            out.extend_from_slice(&flipped.to_be_bytes());
        }
        SqlValue::Float(f) => {
            out.push(0x03);
            let bits = f.to_bits();
            let flipped = if (bits >> 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            out.extend_from_slice(&flipped.to_be_bytes());
        }
        SqlValue::Numeric(num) => {
            out.push(0x04);
            let flipped = (num.unscaled as u128) ^ (1u128 << 127);
            out.extend_from_slice(&flipped.to_be_bytes());
        }
        SqlValue::Text(s) => {
            out.push(0x05);
            for &b in s.as_bytes() {
                if b == 0x00 {
                    out.extend_from_slice(&[0x00, 0xFF]);
                } else {
                    out.push(b);
                }
            }
            out.extend_from_slice(&[0x00, 0x00]);
        }
        SqlValue::Blob(bytes) => {
            out.push(0x06);
            for &b in bytes {
                if b == 0x00 {
                    out.extend_from_slice(&[0x00, 0xFF]);
                } else {
                    out.push(b);
                }
            }
            out.extend_from_slice(&[0x00, 0x00]);
        }
        SqlValue::Date(days) => {
            out.push(0x07);
            let flipped = (*days as u32) ^ (1u32 << 31);
            out.extend_from_slice(&flipped.to_be_bytes());
        }
        SqlValue::Timestamp(micros) => {
            out.push(0x08);
            let flipped = (*micros as u64) ^ (1u64 << 63);
            out.extend_from_slice(&flipped.to_be_bytes());
        }
    }
    out
}

pub fn build_btree_key(table_id: u32, index_id: u16, parts: &[SqlValue]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(&table_id.to_be_bytes());
    buf.extend_from_slice(&index_id.to_be_bytes());
    for p in parts {
        buf.extend_from_slice(&encode_memcomparable_key(p));
    }
    buf
}

pub fn build_table_prefix(table_id: u32, index_id: u16) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::with_capacity(6);
    start.extend_from_slice(&table_id.to_be_bytes());
    start.extend_from_slice(&index_id.to_be_bytes());

    let mut end = start.clone();
    for b in end.iter_mut().rev() {
        if *b < 0xFF {
            *b += 1;
            break;
        }
        *b = 0;
    }
    (start, end)
}

/// Encodes a row tuple using compact variable-width Numeric packing (5B / 9B / 17B).
pub fn encode_row(values: &[SqlValue]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for v in values {
        match v {
            SqlValue::Null => buf.push(0),
            SqlValue::Bool(b) => {
                buf.push(1);
                buf.push(u8::from(*b));
            }
            SqlValue::Int(n) => {
                buf.push(2);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            SqlValue::Float(f) => {
                buf.push(3);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            SqlValue::Numeric(num) => {
                // Pack Numeric into 5B (i32 + scale), 9B (i64 + scale), or 17B (i128 + scale)
                if let Ok(v32) = i32::try_from(num.unscaled) {
                    buf.push(41);
                    buf.extend_from_slice(&v32.to_le_bytes());
                    buf.push(num.scale);
                } else if let Ok(v64) = i64::try_from(num.unscaled) {
                    buf.push(42);
                    buf.extend_from_slice(&v64.to_le_bytes());
                    buf.push(num.scale);
                } else {
                    buf.push(43);
                    buf.extend_from_slice(&num.unscaled.to_le_bytes());
                    buf.push(num.scale);
                }
            }
            SqlValue::Text(s) => {
                buf.push(5);
                let raw = s.as_bytes();
                buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
                buf.extend_from_slice(raw);
            }
            SqlValue::Blob(b) => {
                buf.push(6);
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            SqlValue::Date(d) => {
                buf.push(7);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            SqlValue::Timestamp(ts) => {
                buf.push(8);
                buf.extend_from_slice(&ts.to_le_bytes());
            }
        }
    }
    buf
}

pub fn decode_row(buf: &[u8]) -> Result<Vec<SqlValue>, DbError> {
    if buf.len() < 2 {
        return Err(DbError::CorruptedData("Truncated relational row"));
    }
    let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let mut cur = 2;
    let mut out = Vec::with_capacity(count);

    for _ in 0..count {
        if cur >= buf.len() {
            return Err(DbError::CorruptedData("Truncated row value tag"));
        }
        let tag = buf[cur];
        cur += 1;
        match tag {
            0 => out.push(SqlValue::Null),
            1 => {
                if cur + 1 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Bool"));
                }
                out.push(SqlValue::Bool(buf[cur] != 0));
                cur += 1;
            }
            2 => {
                if cur + 8 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Int"));
                }
                out.push(SqlValue::Int(i64::from_le_bytes(
                    buf[cur..cur + 8].try_into().unwrap(),
                )));
                cur += 8;
            }
            3 => {
                if cur + 8 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Float"));
                }
                out.push(SqlValue::Float(f64::from_le_bytes(
                    buf[cur..cur + 8].try_into().unwrap(),
                )));
                cur += 8;
            }
            41 => {
                if cur + 5 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated 5B Numeric"));
                }
                let unscaled = i32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as i128;
                let scale = buf[cur + 4];
                out.push(SqlValue::Numeric(Numeric::new(unscaled, scale)?));
                cur += 5;
            }
            42 => {
                if cur + 9 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated 9B Numeric"));
                }
                let unscaled = i64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap()) as i128;
                let scale = buf[cur + 8];
                out.push(SqlValue::Numeric(Numeric::new(unscaled, scale)?));
                cur += 9;
            }
            43 => {
                if cur + 17 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated 17B Numeric"));
                }
                let unscaled = i128::from_le_bytes(buf[cur..cur + 16].try_into().unwrap());
                let scale = buf[cur + 16];
                out.push(SqlValue::Numeric(Numeric::new(unscaled, scale)?));
                cur += 17;
            }
            5 => {
                if cur + 4 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Text len"));
                }
                let len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                if cur + len > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Text"));
                }
                let s =
                    std::str::from_utf8(&buf[cur..cur + len]).map_err(|_| DbError::InvalidUtf8)?;
                out.push(SqlValue::Text(s.to_string()));
                cur += len;
            }
            6 => {
                if cur + 4 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Blob len"));
                }
                let len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
                cur += 4;
                if cur + len > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Blob"));
                }
                out.push(SqlValue::Blob(buf[cur..cur + len].to_vec()));
                cur += len;
            }
            7 => {
                if cur + 4 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Date"));
                }
                out.push(SqlValue::Date(i32::from_le_bytes(
                    buf[cur..cur + 4].try_into().unwrap(),
                )));
                cur += 4;
            }
            8 => {
                if cur + 8 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated row Timestamp"));
                }
                out.push(SqlValue::Timestamp(i64::from_le_bytes(
                    buf[cur..cur + 8].try_into().unwrap(),
                )));
                cur += 8;
            }
            _ => return Err(DbError::CorruptedData("Unknown row value tag")),
        }
    }
    Ok(out)
}

/// Parses `"YYYY-MM-DD"` into days since 1970-01-01 (Howard Hinnant's civil-from-days algorithm).
pub fn parse_iso_date(s: &str) -> Result<i32, DbError> {
    let parts: Vec<&str> = s.trim().split('-').collect();
    if parts.len() != 3 {
        return Err(DbError::CorruptedData(
            "Invalid DATE format (expected YYYY-MM-DD)",
        ));
    }
    let y: i64 = parts[0]
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid year"))?;
    let m: u32 = parts[1]
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid month"))?;
    let d: u32 = parts[2]
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid day"))?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(DbError::CorruptedData("Date out of range"));
    }

    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = y_adj.div_euclid(400);
    let yoe = y_adj.rem_euclid(400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + (doe as i64) - 719_468;
    Ok(days as i32)
}

/// Parses `"YYYY-MM-DD HH:MM:SS"` or `"YYYY-MM-DDTHH:MM:SS"` into microseconds since Unix epoch.
pub fn parse_iso_timestamp(s: &str) -> Result<i64, DbError> {
    let s = s.trim().trim_end_matches('Z');
    let Some((date_part, time_part)) = s.split_once([' ', 'T']) else {
        let days = parse_iso_date(s)? as i64;
        return Ok(days * 86_400_000_000);
    };
    let days = parse_iso_date(date_part)? as i64;
    let tparts: Vec<&str> = time_part.split(':').collect();
    if tparts.len() != 3 {
        return Err(DbError::CorruptedData(
            "Invalid TIMESTAMP format (expected YYYY-MM-DD HH:MM:SS)",
        ));
    }
    let hh: i64 = tparts[0]
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid hour"))?;
    let mm: i64 = tparts[1]
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid minute"))?;
    let (sec_str, frac_str) = tparts[2].split_once('.').unwrap_or((tparts[2], "0"));
    let ss: i64 = sec_str
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid second"))?;
    let micros_str = format!("{:0<6}", &frac_str[..frac_str.len().min(6)]);
    let micros: i64 = micros_str
        .parse()
        .map_err(|_| DbError::CorruptedData("Invalid fractional second"))?;

    let total_secs = days * 86_400 + hh * 3600 + mm * 60 + ss;
    Ok(total_secs * 1_000_000 + micros)
}

pub fn encode_table_schema(s: &TableSchema) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&s.table_id.to_le_bytes());
    buf.extend_from_slice(&(s.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(s.name.as_bytes());
    buf.extend_from_slice(&(s.columns.len() as u16).to_le_bytes());
    for c in &s.columns {
        buf.extend_from_slice(&(c.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(c.name.as_bytes());
        match c.sql_type {
            SqlType::Bool => buf.extend_from_slice(&[1, 0, 0]),
            SqlType::Int => buf.extend_from_slice(&[2, 0, 0]),
            SqlType::Float => buf.extend_from_slice(&[3, 0, 0]),
            SqlType::Numeric { precision, scale } => buf.extend_from_slice(&[4, precision, scale]),
            SqlType::Text => buf.extend_from_slice(&[5, 0, 0]),
            SqlType::Blob => buf.extend_from_slice(&[6, 0, 0]),
            SqlType::Date => buf.extend_from_slice(&[7, 0, 0]),
            SqlType::Timestamp => buf.extend_from_slice(&[8, 0, 0]),
        }
        let flags = u8::from(c.nullable) | (u8::from(c.primary_key) << 1);
        buf.push(flags);
    }
    buf
}

pub fn decode_table_schema(buf: &[u8]) -> Result<TableSchema, DbError> {
    if buf.len() < 8 {
        return Err(DbError::CorruptedData("Truncated table schema"));
    }
    let table_id = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let nlen = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
    let mut cur = 6;
    let name = std::str::from_utf8(&buf[cur..cur + nlen])
        .map_err(|_| DbError::InvalidUtf8)?
        .to_string();
    cur += nlen;
    let col_count = u16::from_le_bytes(buf[cur..cur + 2].try_into().unwrap()) as usize;
    cur += 2;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let clen = u16::from_le_bytes(buf[cur..cur + 2].try_into().unwrap()) as usize;
        cur += 2;
        let cname = std::str::from_utf8(&buf[cur..cur + clen])
            .map_err(|_| DbError::InvalidUtf8)?
            .to_string();
        cur += clen;
        let sql_type = match buf[cur] {
            1 => SqlType::Bool,
            2 => SqlType::Int,
            3 => SqlType::Float,
            4 => SqlType::Numeric {
                precision: buf[cur + 1],
                scale: buf[cur + 2],
            },
            5 => SqlType::Text,
            6 => SqlType::Blob,
            7 => SqlType::Date,
            8 => SqlType::Timestamp,
            _ => return Err(DbError::CorruptedData("Unknown SqlType tag")),
        };
        let flags = buf[cur + 3];
        cur += 4;
        columns.push(ColumnSchema {
            name: cname,
            sql_type,
            nullable: (flags & 1) != 0,
            primary_key: (flags & 2) != 0,
        });
    }
    Ok(TableSchema {
        table_id,
        name,
        columns,
    })
}
