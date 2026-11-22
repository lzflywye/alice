use crate::error::HdbError;
use crate::schema::{ColumnDef, DataType, SegmentSchema};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

const CATALOG_MAGIC: &[u8; 8] = b"ALICECAT";

/// Persists segment schemas atomically using a temporary file + rename pattern.
pub struct CatalogStorage;

impl CatalogStorage {
    pub fn load(path: &Path) -> Result<HashMap<u16, SegmentSchema>, HdbError> {
        if !path.exists() {
            return Ok(HashMap::new());
        }

        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        if buf.len() < 12 || &buf[0..8] != CATALOG_MAGIC {
            return Err(HdbError::CorruptedData("Invalid catalog file header"));
        }

        let count_arr: [u8; 4] = buf[8..12]
            .try_into()
            .map_err(|_| HdbError::CorruptedData("Invalid catalog schema count"))?;
        let schema_count = u32::from_le_bytes(count_arr) as usize;
        let mut cursor = 12;
        let mut schemas = HashMap::with_capacity(schema_count);

        for _ in 0..schema_count {
            let node_type = Self::read_u16(&buf, &mut cursor)?;
            let name = Self::read_string(&buf, &mut cursor)?;
            let has_parent = Self::read_u8(&buf, &mut cursor)? != 0;
            let parent_raw = Self::read_u16(&buf, &mut cursor)?;
            let parent_type = if has_parent { Some(parent_raw) } else { None };

            let col_count = Self::read_u16(&buf, &mut cursor)? as usize;
            let mut columns = Vec::with_capacity(col_count);

            for _ in 0..col_count {
                let col_name = Self::read_string(&buf, &mut cursor)?;
                let tag = Self::read_u8(&buf, &mut cursor)?;
                let param = Self::read_u16(&buf, &mut cursor)?;
                let flags = Self::read_u8(&buf, &mut cursor)?;
                let nullable = (flags & (1 << 0)) != 0;
                let indexed = (flags & (1 << 1)) != 0;
                let unique = (flags & (1 << 2)) != 0;
                let data_type = DataType::from_raw(tag, param)?;

                columns.push(ColumnDef {
                    name: col_name,
                    data_type,
                    nullable,
                    indexed,
                    unique,
                });
            }

            schemas.insert(
                node_type,
                SegmentSchema {
                    node_type,
                    name,
                    parent_type,
                    columns,
                },
            );
        }

        Ok(schemas)
    }

    pub fn save(path: &Path, schemas: &HashMap<u16, SegmentSchema>) -> Result<(), HdbError> {
        let mut buf = Vec::new();
        buf.extend_from_slice(CATALOG_MAGIC);
        buf.extend_from_slice(&(schemas.len() as u32).to_le_bytes());

        let mut keys: Vec<u16> = schemas.keys().copied().collect();
        keys.sort_unstable();

        for key in keys {
            let s = &schemas[&key];
            buf.extend_from_slice(&s.node_type.to_le_bytes());
            Self::write_string(&mut buf, &s.name);
            match s.parent_type {
                Some(pid) => {
                    buf.push(1);
                    buf.extend_from_slice(&pid.to_le_bytes());
                }
                None => {
                    buf.push(0);
                    buf.extend_from_slice(&0u16.to_le_bytes());
                }
            }

            buf.extend_from_slice(&(s.columns.len() as u16).to_le_bytes());
            for col in &s.columns {
                Self::write_string(&mut buf, &col.name);
                buf.push(col.data_type.type_tag());
                buf.extend_from_slice(&col.data_type.type_param().to_le_bytes());
                let mut flags = 0u8;
                if col.nullable {
                    flags |= 1 << 0;
                }
                if col.indexed {
                    flags |= 1 << 1;
                }
                if col.unique {
                    flags |= 1 << 2;
                }
                buf.push(flags);
            }
        }

        let tmp_path = path.with_extension("db.tmp");
        {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?;
            tmp_file.write_all(&buf)?;
            tmp_file.sync_data()?;
        }
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    fn write_string(buf: &mut Vec<u8>, s: &str) {
        let bytes = s.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(bytes);
    }

    fn read_u8(buf: &[u8], cursor: &mut usize) -> Result<u8, HdbError> {
        if *cursor + 1 > buf.len() {
            return Err(HdbError::CorruptedData("Unexpected EOF in catalog"));
        }
        let v = buf[*cursor];
        *cursor += 1;
        Ok(v)
    }

    fn read_u16(buf: &[u8], cursor: &mut usize) -> Result<u16, HdbError> {
        if *cursor + 2 > buf.len() {
            return Err(HdbError::CorruptedData("Unexpected EOF in catalog"));
        }
        let arr: [u8; 2] = buf[*cursor..*cursor + 2]
            .try_into()
            .map_err(|_| HdbError::CorruptedData("Invalid u16 slice in catalog"))?;
        *cursor += 2;
        Ok(u16::from_le_bytes(arr))
    }

    fn read_string(buf: &[u8], cursor: &mut usize) -> Result<String, HdbError> {
        let len = Self::read_u16(buf, cursor)? as usize;
        if *cursor + len > buf.len() {
            return Err(HdbError::CorruptedData("Unexpected EOF in catalog string"));
        }
        let s = std::str::from_utf8(&buf[*cursor..*cursor + len])
            .map_err(|_| HdbError::InvalidUtf8)?
            .to_string();
        *cursor += len;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_save_and_load() {
        let path = std::env::temp_dir().join(format!("alice_cat_{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let mut map = HashMap::new();
        map.insert(
            1,
            SegmentSchema {
                node_type: 1,
                name: "Company".into(),
                parent_type: None,
                columns: vec![
                    ColumnDef::new("code", DataType::FixedString(8), false).with_index(true),
                    ColumnDef::new("hash", DataType::FixedBytes(32), true).with_index(false),
                    ColumnDef::new("balance", DataType::Decimal, false),
                ],
            },
        );

        CatalogStorage::save(&path, &map).unwrap();
        let loaded = CatalogStorage::load(&path).unwrap();
        assert_eq!(map, loaded);

        let _ = std::fs::remove_file(&path);
    }
}
