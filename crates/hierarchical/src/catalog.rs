use crate::schema::{ColumnDef, DataType, SegmentSchema};
use common::{DbError, crc32};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

const CATALOG_MAGIC: &[u8; 4] = b"ACAT";
const CATALOG_VERSION: u16 = 1;

pub struct Catalog {
    path: PathBuf,
    schemas: HashMap<u16, SegmentSchema>,
}

impl Catalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let mut schemas = HashMap::new();
        if path.exists() {
            let bytes = fs::read(&path)?;
            if !bytes.is_empty() {
                for s in decode_catalog(&bytes)? {
                    schemas.insert(s.node_type, s);
                }
            }
        }
        Ok(Self { path, schemas })
    }

    pub fn get(&self, node_type: u16) -> Option<&SegmentSchema> {
        self.schemas.get(&node_type)
    }

    pub fn all(&self) -> Vec<SegmentSchema> {
        let mut list: Vec<SegmentSchema> = self.schemas.values().cloned().collect();
        list.sort_by_key(|s| s.node_type);
        list
    }

    pub fn register(&mut self, schema: SegmentSchema) -> Result<(), DbError> {
        if self.schemas.contains_key(&schema.node_type) {
            return Err(DbError::SchemaAlreadyExists(schema.node_type));
        }
        if let Some(parent_t) = schema.parent_type
            && !self.schemas.contains_key(&parent_t)
        {
            return Err(DbError::SchemaNotFound(parent_t));
        }
        self.schemas.insert(schema.node_type, schema);
        self.persist()
    }

    pub fn replace_all(&mut self, list: Vec<SegmentSchema>) -> Result<(), DbError> {
        self.schemas.clear();
        for s in list {
            self.schemas.insert(s.node_type, s);
        }
        self.persist()
    }

    fn persist(&self) -> Result<(), DbError> {
        let encoded = encode_catalog(&self.all());
        let tmp = self.path.with_extension("db.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&encoded)?;
            f.sync_data()?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

pub fn encode_catalog(schemas: &[SegmentSchema]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(CATALOG_MAGIC);
    buf.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(schemas.len() as u16).to_le_bytes());

    for s in schemas {
        buf.extend_from_slice(&s.node_type.to_le_bytes());
        let name_bytes = s.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        match s.parent_type {
            Some(pt) => {
                buf.push(1);
                buf.extend_from_slice(&pt.to_le_bytes());
            }
            None => {
                buf.push(0);
                buf.extend_from_slice(&0u16.to_le_bytes());
            }
        }
        buf.extend_from_slice(&(s.columns.len() as u16).to_le_bytes());
        for col in &s.columns {
            let cname = col.name.as_bytes();
            buf.extend_from_slice(&(cname.len() as u16).to_le_bytes());
            buf.extend_from_slice(cname);
            buf.push(col.data_type.type_tag());
            buf.extend_from_slice(&col.data_type.param().to_le_bytes());
            let mut flags = 0u8;
            if col.nullable {
                flags |= 0x01;
            }
            if col.indexed {
                flags |= 0x02;
            }
            if col.unique {
                flags |= 0x04;
            }
            buf.push(flags);
        }
    }

    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

pub fn decode_catalog(bytes: &[u8]) -> Result<Vec<SegmentSchema>, DbError> {
    if bytes.len() < 12 {
        return Err(DbError::CorruptedData("Catalog file too short"));
    }
    if &bytes[0..4] != CATALOG_MAGIC {
        return Err(DbError::InvalidMagic);
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver != CATALOG_VERSION {
        return Err(DbError::UnsupportedVersion(ver));
    }
    let crc_offset = bytes.len() - 4;
    let expected_crc = u32::from_le_bytes(bytes[crc_offset..].try_into().unwrap());
    let actual_crc = crc32(&bytes[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(DbError::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let count = u16::from_le_bytes([bytes[6], bytes[7]]) as usize;
    let mut cur = 8;
    let mut out = Vec::with_capacity(count);

    for _ in 0..count {
        if cur + 4 > crc_offset {
            return Err(DbError::CorruptedData("Truncated schema header"));
        }
        let node_type = u16::from_le_bytes(bytes[cur..cur + 2].try_into().unwrap());
        let name_len = u16::from_le_bytes(bytes[cur + 2..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        if cur + name_len + 5 > crc_offset {
            return Err(DbError::CorruptedData("Truncated schema name/parent"));
        }
        let name =
            std::str::from_utf8(&bytes[cur..cur + name_len]).map_err(|_| DbError::InvalidUtf8)?;
        cur += name_len;
        let has_parent = bytes[cur];
        let parent_val = u16::from_le_bytes(bytes[cur + 1..cur + 3].try_into().unwrap());
        let parent_type = if has_parent != 0 {
            Some(parent_val)
        } else {
            None
        };
        let col_count = u16::from_le_bytes(bytes[cur + 3..cur + 5].try_into().unwrap()) as usize;
        cur += 5;

        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            if cur + 2 > crc_offset {
                return Err(DbError::CorruptedData("Truncated column header"));
            }
            let cname_len = u16::from_le_bytes(bytes[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            if cur + cname_len + 4 > crc_offset {
                return Err(DbError::CorruptedData("Truncated column body"));
            }
            let cname = std::str::from_utf8(&bytes[cur..cur + cname_len])
                .map_err(|_| DbError::InvalidUtf8)?;
            cur += cname_len;
            let tag = bytes[cur];
            let param = u16::from_le_bytes(bytes[cur + 1..cur + 3].try_into().unwrap());
            let flags = bytes[cur + 3];
            cur += 4;

            columns.push(ColumnDef {
                name: cname.to_string(),
                data_type: DataType::from_tag(tag, param)?,
                nullable: (flags & 0x01) != 0,
                indexed: (flags & 0x02) != 0,
                unique: (flags & 0x04) != 0,
            });
        }

        out.push(SegmentSchema {
            node_type,
            name: name.to_string(),
            parent_type,
            columns,
        });
    }

    Ok(out)
}
