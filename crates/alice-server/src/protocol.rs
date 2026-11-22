use alice_core::error::HdbError;
use alice_core::node::NODE_SLOT_SIZE;
use alice_core::schema::{ColumnDef, DataType, SegmentSchema, Value};
use alice_core::wal::{WalEntry, WalOp};
use std::io::{Read, Write};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgTag {
    Ping = 1,
    DefineSegment = 2,
    Insert = 3,
    Update = 4,
    Delete = 5,
    Get = 6,
    ChildrenIds = 7,
    FetchWal = 8,
    SyncCatalog = 9,
    Promote = 10,

    OkEmpty = 100,
    OkNodeId = 101,
    OkRecord = 102,
    OkNodeIds = 103,
    OkWalBatch = 104,
    OkCatalog = 105,
    ErrResponse = 255,
}

impl TryFrom<u8> for MsgTag {
    type Error = HdbError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(MsgTag::Ping),
            2 => Ok(MsgTag::DefineSegment),
            3 => Ok(MsgTag::Insert),
            4 => Ok(MsgTag::Update),
            5 => Ok(MsgTag::Delete),
            6 => Ok(MsgTag::Get),
            7 => Ok(MsgTag::ChildrenIds),
            8 => Ok(MsgTag::FetchWal),
            9 => Ok(MsgTag::SyncCatalog),
            10 => Ok(MsgTag::Promote),
            100 => Ok(MsgTag::OkEmpty),
            101 => Ok(MsgTag::OkNodeId),
            102 => Ok(MsgTag::OkRecord),
            103 => Ok(MsgTag::OkNodeIds),
            104 => Ok(MsgTag::OkWalBatch),
            105 => Ok(MsgTag::OkCatalog),
            255 => Ok(MsgTag::ErrResponse),
            _ => Err(HdbError::CorruptedData("Unknown RPC message tag")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Request {
    Ping,
    DefineSegment(SegmentSchema),
    Insert {
        parent_id: u64,
        node_type: u16,
        values: Vec<Value>,
    },
    Update {
        node_id: u64,
        values: Vec<Value>,
    },
    Delete {
        node_id: u64,
    },
    Get {
        node_id: u64,
    },
    ChildrenIds {
        parent_id: u64,
    },
    FetchWal {
        after_lsn: u64,
    },
    SyncCatalog,
    Promote,
}

#[derive(Debug, Clone)]
pub enum Response {
    OkEmpty,
    OkNodeId(u64),
    OkRecord { node_type: u16, values: Vec<Value> },
    OkNodeIds(Vec<u64>),
    OkWalBatch(Vec<WalEntry>),
    OkCatalog(Vec<SegmentSchema>),
    Err(String),
}

/// Writes a length-prefixed binary frame: [frame_len: u32][tag: u8][body...]
pub fn write_frame<W: Write>(w: &mut W, tag: MsgTag, body: &[u8]) -> Result<(), HdbError> {
    let frame_len = (1 + body.len()) as u32;
    w.write_all(&frame_len.to_le_bytes())?;
    w.write_all(&[tag as u8])?;
    w.write_all(body)?;
    w.flush()?;
    Ok(())
}

pub fn read_frame<R: Read>(r: &mut R) -> Result<(MsgTag, Vec<u8>), HdbError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len == 0 {
        return Err(HdbError::CorruptedData("Empty RPC frame"));
    }
    let mut buf = vec![0u8; frame_len];
    r.read_exact(&mut buf)?;
    let tag = MsgTag::try_from(buf[0])?;
    Ok((tag, buf[1..].to_vec()))
}

// --- Binary Serialization Helpers ---

pub fn encode_request(req: &Request) -> (MsgTag, Vec<u8>) {
    let mut b = Vec::new();
    match req {
        Request::Ping => (MsgTag::Ping, b),
        Request::DefineSegment(schema) => {
            write_schema(&mut b, schema);
            (MsgTag::DefineSegment, b)
        }
        Request::Insert {
            parent_id,
            node_type,
            values,
        } => {
            b.extend_from_slice(&parent_id.to_le_bytes());
            b.extend_from_slice(&node_type.to_le_bytes());
            write_values(&mut b, values);
            (MsgTag::Insert, b)
        }
        Request::Update { node_id, values } => {
            b.extend_from_slice(&node_id.to_le_bytes());
            write_values(&mut b, values);
            (MsgTag::Update, b)
        }
        Request::Delete { node_id } => {
            b.extend_from_slice(&node_id.to_le_bytes());
            (MsgTag::Delete, b)
        }
        Request::Get { node_id } => {
            b.extend_from_slice(&node_id.to_le_bytes());
            (MsgTag::Get, b)
        }
        Request::ChildrenIds { parent_id } => {
            b.extend_from_slice(&parent_id.to_le_bytes());
            (MsgTag::ChildrenIds, b)
        }
        Request::FetchWal { after_lsn } => {
            b.extend_from_slice(&after_lsn.to_le_bytes());
            (MsgTag::FetchWal, b)
        }
        Request::SyncCatalog => (MsgTag::SyncCatalog, b),
        Request::Promote => (MsgTag::Promote, b),
    }
}

pub fn decode_request(tag: MsgTag, buf: &[u8]) -> Result<Request, HdbError> {
    let mut cur = 0;
    match tag {
        MsgTag::Ping => Ok(Request::Ping),
        MsgTag::DefineSegment => Ok(Request::DefineSegment(read_schema(buf, &mut cur)?)),
        MsgTag::Insert => {
            let parent_id = read_u64(buf, &mut cur)?;
            let node_type = read_u16(buf, &mut cur)?;
            let values = read_values(buf, &mut cur)?;
            Ok(Request::Insert {
                parent_id,
                node_type,
                values,
            })
        }
        MsgTag::Update => {
            let node_id = read_u64(buf, &mut cur)?;
            let values = read_values(buf, &mut cur)?;
            Ok(Request::Update { node_id, values })
        }
        MsgTag::Delete => Ok(Request::Delete {
            node_id: read_u64(buf, &mut cur)?,
        }),
        MsgTag::Get => Ok(Request::Get {
            node_id: read_u64(buf, &mut cur)?,
        }),
        MsgTag::ChildrenIds => Ok(Request::ChildrenIds {
            parent_id: read_u64(buf, &mut cur)?,
        }),
        MsgTag::FetchWal => Ok(Request::FetchWal {
            after_lsn: read_u64(buf, &mut cur)?,
        }),
        MsgTag::SyncCatalog => Ok(Request::SyncCatalog),
        MsgTag::Promote => Ok(Request::Promote),
        _ => Err(HdbError::CorruptedData(
            "Unexpected response tag as request",
        )),
    }
}

pub fn encode_response(resp: &Response) -> (MsgTag, Vec<u8>) {
    let mut b = Vec::new();
    match resp {
        Response::OkEmpty => (MsgTag::OkEmpty, b),
        Response::OkNodeId(id) => {
            b.extend_from_slice(&id.to_le_bytes());
            (MsgTag::OkNodeId, b)
        }
        Response::OkRecord { node_type, values } => {
            b.extend_from_slice(&node_type.to_le_bytes());
            write_values(&mut b, values);
            (MsgTag::OkRecord, b)
        }
        Response::OkNodeIds(ids) => {
            b.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            for id in ids {
                b.extend_from_slice(&id.to_le_bytes());
            }
            (MsgTag::OkNodeIds, b)
        }
        Response::OkWalBatch(entries) => {
            b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for e in entries {
                b.extend_from_slice(&e.lsn.to_le_bytes());
                match &e.op {
                    WalOp::WriteSlot {
                        node_id,
                        slot_bytes,
                    } => {
                        b.push(1);
                        b.extend_from_slice(&node_id.to_le_bytes());
                        b.extend_from_slice(slot_bytes.as_slice());
                    }
                    WalOp::WriteHeap { offset, data } => {
                        b.push(2);
                        b.extend_from_slice(&offset.to_le_bytes());
                        b.extend_from_slice(&(data.len() as u32).to_le_bytes());
                        b.extend_from_slice(data);
                    }
                }
            }
            (MsgTag::OkWalBatch, b)
        }
        Response::OkCatalog(schemas) => {
            b.extend_from_slice(&(schemas.len() as u32).to_le_bytes());
            for s in schemas {
                write_schema(&mut b, s);
            }
            (MsgTag::OkCatalog, b)
        }
        Response::Err(msg) => {
            write_str(&mut b, msg);
            (MsgTag::ErrResponse, b)
        }
    }
}

pub fn decode_response(tag: MsgTag, buf: &[u8]) -> Result<Response, HdbError> {
    let mut cur = 0;
    match tag {
        MsgTag::OkEmpty => Ok(Response::OkEmpty),
        MsgTag::OkNodeId => Ok(Response::OkNodeId(read_u64(buf, &mut cur)?)),
        MsgTag::OkRecord => {
            let node_type = read_u16(buf, &mut cur)?;
            let values = read_values(buf, &mut cur)?;
            Ok(Response::OkRecord { node_type, values })
        }
        MsgTag::OkNodeIds => {
            let count = read_u32(buf, &mut cur)? as usize;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(read_u64(buf, &mut cur)?);
            }
            Ok(Response::OkNodeIds(ids))
        }
        MsgTag::OkWalBatch => {
            let count = read_u32(buf, &mut cur)? as usize;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let lsn = read_u64(buf, &mut cur)?;
                let op_type = read_u8(buf, &mut cur)?;
                let op = match op_type {
                    1 => {
                        let node_id = read_u64(buf, &mut cur)?;
                        let raw = read_bytes(buf, &mut cur, NODE_SLOT_SIZE)?;
                        let mut slot_bytes = Box::new([0u8; NODE_SLOT_SIZE]);
                        slot_bytes.copy_from_slice(&raw);
                        WalOp::WriteSlot {
                            node_id,
                            slot_bytes,
                        }
                    }
                    2 => {
                        let offset = read_u64(buf, &mut cur)?;
                        let len = read_u32(buf, &mut cur)? as usize;
                        let data = read_bytes(buf, &mut cur, len)?;
                        WalOp::WriteHeap { offset, data }
                    }
                    _ => return Err(HdbError::CorruptedData("Invalid WalOp tag in RPC")),
                };
                entries.push(WalEntry { lsn, txn_id: 0, op });
            }
            Ok(Response::OkWalBatch(entries))
        }
        MsgTag::OkCatalog => {
            let count = read_u32(buf, &mut cur)? as usize;
            let mut schemas = Vec::with_capacity(count);
            for _ in 0..count {
                schemas.push(read_schema(buf, &mut cur)?);
            }
            Ok(Response::OkCatalog(schemas))
        }
        MsgTag::ErrResponse => Ok(Response::Err(read_str(buf, &mut cur)?)),
        _ => Err(HdbError::CorruptedData(
            "Unexpected request tag as response",
        )),
    }
}

// --- Low-level field coders ---

fn write_str(b: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    b.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    b.extend_from_slice(bytes);
}

fn read_str(buf: &[u8], cur: &mut usize) -> Result<String, HdbError> {
    let len = read_u32(buf, cur)? as usize;
    let bytes = read_bytes(buf, cur, len)?;
    String::from_utf8(bytes).map_err(|_| HdbError::InvalidUtf8)
}

fn write_schema(b: &mut Vec<u8>, s: &SegmentSchema) {
    b.extend_from_slice(&s.node_type.to_le_bytes());
    write_str(b, &s.name);
    match s.parent_type {
        Some(p) => {
            b.push(1);
            b.extend_from_slice(&p.to_le_bytes());
        }
        None => {
            b.push(0);
            b.extend_from_slice(&0u16.to_le_bytes());
        }
    }
    b.extend_from_slice(&(s.columns.len() as u16).to_le_bytes());
    for c in &s.columns {
        write_str(b, &c.name);
        b.push(c.data_type.type_tag());
        b.extend_from_slice(&c.data_type.type_param().to_le_bytes());
        b.push(if c.nullable { 1 } else { 0 });
    }
}

fn read_schema(buf: &[u8], cur: &mut usize) -> Result<SegmentSchema, HdbError> {
    let node_type = read_u16(buf, cur)?;
    let name = read_str(buf, cur)?;
    let has_parent = read_u8(buf, cur)? != 0;
    let p_raw = read_u16(buf, cur)?;
    let parent_type = if has_parent { Some(p_raw) } else { None };
    let col_count = read_u16(buf, cur)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let col_name = read_str(buf, cur)?;
        let tag = read_u8(buf, cur)?;
        let param = read_u16(buf, cur)?;
        let nullable = read_u8(buf, cur)? != 0;
        columns.push(ColumnDef {
            name: col_name,
            data_type: DataType::from_raw(tag, param)?,
            nullable,
        });
    }
    Ok(SegmentSchema {
        node_type,
        name,
        parent_type,
        columns,
    })
}

fn write_values(b: &mut Vec<u8>, values: &[Value]) {
    b.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for v in values {
        match v {
            Value::Null => b.push(0),
            Value::Bool(v) => {
                b.push(1);
                b.push(if *v { 1 } else { 0 });
            }
            Value::I64(v) => {
                b.push(2);
                b.extend_from_slice(&v.to_le_bytes());
            }
            Value::F64(v) => {
                b.push(3);
                b.extend_from_slice(&v.to_le_bytes());
            }
            Value::String(s) => {
                b.push(4);
                write_str(b, s);
            }
            Value::Bytes(raw) => {
                b.push(5);
                b.extend_from_slice(&(raw.len() as u32).to_le_bytes());
                b.extend_from_slice(raw);
            }
        }
    }
}

fn read_values(buf: &[u8], cur: &mut usize) -> Result<Vec<Value>, HdbError> {
    let count = read_u16(buf, cur)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let tag = read_u8(buf, cur)?;
        let v = match tag {
            0 => Value::Null,
            1 => Value::Bool(read_u8(buf, cur)? != 0),
            2 => Value::I64(i64::from_le_bytes(
                read_bytes(buf, cur, 8)?.try_into().unwrap(),
            )),
            3 => Value::F64(f64::from_le_bytes(
                read_bytes(buf, cur, 8)?.try_into().unwrap(),
            )),
            4 => Value::String(read_str(buf, cur)?),
            5 => {
                let len = read_u32(buf, cur)? as usize;
                Value::Bytes(read_bytes(buf, cur, len)?)
            }
            _ => return Err(HdbError::CorruptedData("Invalid Value tag in RPC")),
        };
        out.push(v);
    }
    Ok(out)
}

fn read_u8(buf: &[u8], cur: &mut usize) -> Result<u8, HdbError> {
    if *cur + 1 > buf.len() {
        return Err(HdbError::CorruptedData("Unexpected EOF"));
    }
    let v = buf[*cur];
    *cur += 1;
    Ok(v)
}

fn read_u16(buf: &[u8], cur: &mut usize) -> Result<u16, HdbError> {
    let b = read_bytes(buf, cur, 2)?;
    Ok(u16::from_le_bytes(b.try_into().unwrap()))
}

fn read_u32(buf: &[u8], cur: &mut usize) -> Result<u32, HdbError> {
    let b = read_bytes(buf, cur, 4)?;
    Ok(u32::from_le_bytes(b.try_into().unwrap()))
}

fn read_u64(buf: &[u8], cur: &mut usize) -> Result<u64, HdbError> {
    let b = read_bytes(buf, cur, 8)?;
    Ok(u64::from_le_bytes(b.try_into().unwrap()))
}

fn read_bytes(buf: &[u8], cur: &mut usize, len: usize) -> Result<Vec<u8>, HdbError> {
    if *cur + len > buf.len() {
        return Err(HdbError::CorruptedData("Unexpected EOF"));
    }
    let slice = buf[*cur..*cur + len].to_vec();
    *cur += len;
    Ok(slice)
}
