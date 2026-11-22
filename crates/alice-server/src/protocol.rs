use alice_core::db::{BaseSnapshot, NodeView, VacuumStats};
use alice_core::error::HdbError;
use alice_core::node::NODE_SLOT_SIZE;
use alice_core::schema::{ColumnDef, DataType, Decimal, SegmentSchema, Value};
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
    FetchSnapshot = 11,
    FindByIndex = 12,
    WalkSubtree = 13,
    Vacuum = 14,

    // SCRAM-SHA-256 Handshake Requests
    AuthHello = 20,
    AuthProof = 21,

    OkEmpty = 100,
    OkNodeId = 101,
    OkRecord = 102,
    OkNodeIds = 103,
    OkWalBatch = 104,
    OkCatalog = 105,
    OkSnapshot = 106,
    OkNodeViews = 107,
    OkVacuum = 108,

    // SCRAM-SHA-256 Handshake Responses
    AuthChallenge = 120,
    AuthSuccess = 121,

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
            11 => Ok(MsgTag::FetchSnapshot),
            12 => Ok(MsgTag::FindByIndex),
            13 => Ok(MsgTag::WalkSubtree),
            14 => Ok(MsgTag::Vacuum),
            20 => Ok(MsgTag::AuthHello),
            21 => Ok(MsgTag::AuthProof),
            100 => Ok(MsgTag::OkEmpty),
            101 => Ok(MsgTag::OkNodeId),
            102 => Ok(MsgTag::OkRecord),
            103 => Ok(MsgTag::OkNodeIds),
            104 => Ok(MsgTag::OkWalBatch),
            105 => Ok(MsgTag::OkCatalog),
            106 => Ok(MsgTag::OkSnapshot),
            107 => Ok(MsgTag::OkNodeViews),
            108 => Ok(MsgTag::OkVacuum),
            120 => Ok(MsgTag::AuthChallenge),
            121 => Ok(MsgTag::AuthSuccess),
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
    FetchSnapshot,
    FindByIndex {
        node_type: u16,
        col_name: String,
        value: Value,
    },
    WalkSubtree {
        root_node_id: u64,
        max_depth: Option<usize>,
    },
    Vacuum,
    AuthHello {
        username: String,
        client_nonce: Vec<u8>,
    },
    AuthProof {
        client_proof: [u8; 32],
    },
}

#[derive(Debug, Clone)]
pub enum Response {
    OkEmpty,
    OkNodeId(u64),
    OkRecord {
        node_type: u16,
        values: Vec<Value>,
    },
    OkNodeIds(Vec<u64>),
    OkWalBatch(Vec<WalEntry>),
    OkCatalog(Vec<SegmentSchema>),
    OkSnapshot(BaseSnapshot),
    OkNodeViews(Vec<NodeView>),
    OkVacuum(VacuumStats),
    AuthChallenge {
        salt: [u8; 16],
        iterations: u32,
        combined_nonce: Vec<u8>,
    },
    AuthSuccess {
        server_signature: [u8; 32],
    },
    Err(String),
}

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
        Request::FetchSnapshot => (MsgTag::FetchSnapshot, b),
        Request::FindByIndex {
            node_type,
            col_name,
            value,
        } => {
            b.extend_from_slice(&node_type.to_le_bytes());
            write_str(&mut b, col_name);
            write_single_value(&mut b, value);
            (MsgTag::FindByIndex, b)
        }
        Request::WalkSubtree {
            root_node_id,
            max_depth,
        } => {
            b.extend_from_slice(&root_node_id.to_le_bytes());
            match max_depth {
                Some(d) => {
                    b.push(1);
                    b.extend_from_slice(&(*d as u32).to_le_bytes());
                }
                None => {
                    b.push(0);
                    b.extend_from_slice(&0u32.to_le_bytes());
                }
            }
            (MsgTag::WalkSubtree, b)
        }
        Request::Vacuum => (MsgTag::Vacuum, b),
        Request::AuthHello {
            username,
            client_nonce,
        } => {
            write_str(&mut b, username);
            b.extend_from_slice(&(client_nonce.len() as u32).to_le_bytes());
            b.extend_from_slice(client_nonce);
            (MsgTag::AuthHello, b)
        }
        Request::AuthProof { client_proof } => {
            b.extend_from_slice(client_proof);
            (MsgTag::AuthProof, b)
        }
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
        MsgTag::FetchSnapshot => Ok(Request::FetchSnapshot),
        MsgTag::FindByIndex => {
            let node_type = read_u16(buf, &mut cur)?;
            let col_name = read_str(buf, &mut cur)?;
            let value = read_single_value(buf, &mut cur)?;
            Ok(Request::FindByIndex {
                node_type,
                col_name,
                value,
            })
        }
        MsgTag::WalkSubtree => {
            let root_node_id = read_u64(buf, &mut cur)?;
            let has_depth = read_u8(buf, &mut cur)? != 0;
            let raw_depth = read_u32(buf, &mut cur)? as usize;
            let max_depth = if has_depth { Some(raw_depth) } else { None };
            Ok(Request::WalkSubtree {
                root_node_id,
                max_depth,
            })
        }
        MsgTag::Vacuum => Ok(Request::Vacuum),
        MsgTag::AuthHello => {
            let username = read_str(buf, &mut cur)?;
            let nonce_len = read_u32(buf, &mut cur)? as usize;
            let client_nonce = read_bytes(buf, &mut cur, nonce_len)?;
            Ok(Request::AuthHello {
                username,
                client_nonce,
            })
        }
        MsgTag::AuthProof => {
            let raw = read_bytes(buf, &mut cur, 32)?;
            let client_proof: [u8; 32] = raw
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid AuthProof length"))?;
            Ok(Request::AuthProof { client_proof })
        }
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
        Response::OkSnapshot(snap) => {
            b.extend_from_slice(&snap.snapshot_lsn.to_le_bytes());
            b.extend_from_slice(&(snap.schemas.len() as u32).to_le_bytes());
            for s in &snap.schemas {
                write_schema(&mut b, s);
            }
            b.extend_from_slice(&(snap.nodes_bytes.len() as u64).to_le_bytes());
            b.extend_from_slice(&snap.nodes_bytes);
            b.extend_from_slice(&(snap.heap_bytes.len() as u64).to_le_bytes());
            b.extend_from_slice(&snap.heap_bytes);
            (MsgTag::OkSnapshot, b)
        }
        Response::OkNodeViews(views) => {
            b.extend_from_slice(&(views.len() as u32).to_le_bytes());
            for v in views {
                b.extend_from_slice(&v.node_id.to_le_bytes());
                b.extend_from_slice(&v.parent_id.to_le_bytes());
                b.extend_from_slice(&(v.depth as u32).to_le_bytes());
                b.extend_from_slice(&v.node_type.to_le_bytes());
                write_values(&mut b, &v.values);
            }
            (MsgTag::OkNodeViews, b)
        }
        Response::OkVacuum(stats) => {
            b.extend_from_slice(&stats.active_nodes.to_le_bytes());
            b.extend_from_slice(&stats.old_slot_capacity.to_le_bytes());
            b.extend_from_slice(&stats.old_heap_bytes.to_le_bytes());
            b.extend_from_slice(&stats.new_heap_bytes.to_le_bytes());
            (MsgTag::OkVacuum, b)
        }
        Response::AuthChallenge {
            salt,
            iterations,
            combined_nonce,
        } => {
            b.extend_from_slice(salt);
            b.extend_from_slice(&iterations.to_le_bytes());
            b.extend_from_slice(&(combined_nonce.len() as u32).to_le_bytes());
            b.extend_from_slice(combined_nonce);
            (MsgTag::AuthChallenge, b)
        }
        Response::AuthSuccess { server_signature } => {
            b.extend_from_slice(server_signature);
            (MsgTag::AuthSuccess, b)
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
        MsgTag::OkSnapshot => {
            let snapshot_lsn = read_u64(buf, &mut cur)?;
            let schema_count = read_u32(buf, &mut cur)? as usize;
            let mut schemas = Vec::with_capacity(schema_count);
            for _ in 0..schema_count {
                schemas.push(read_schema(buf, &mut cur)?);
            }
            let nodes_len = read_u64(buf, &mut cur)? as usize;
            let nodes_bytes = read_bytes(buf, &mut cur, nodes_len)?;
            let heap_len = read_u64(buf, &mut cur)? as usize;
            let heap_bytes = read_bytes(buf, &mut cur, heap_len)?;
            Ok(Response::OkSnapshot(BaseSnapshot {
                snapshot_lsn,
                schemas,
                nodes_bytes,
                heap_bytes,
            }))
        }
        MsgTag::OkNodeViews => {
            let count = read_u32(buf, &mut cur)? as usize;
            let mut views = Vec::with_capacity(count);
            for _ in 0..count {
                let node_id = read_u64(buf, &mut cur)?;
                let parent_id = read_u64(buf, &mut cur)?;
                let depth = read_u32(buf, &mut cur)? as usize;
                let node_type = read_u16(buf, &mut cur)?;
                let values = read_values(buf, &mut cur)?;
                views.push(NodeView {
                    node_id,
                    parent_id,
                    depth,
                    node_type,
                    values,
                });
            }
            Ok(Response::OkNodeViews(views))
        }
        MsgTag::OkVacuum => {
            let active_nodes = read_u64(buf, &mut cur)?;
            let old_slot_capacity = read_u64(buf, &mut cur)?;
            let old_heap_bytes = read_u64(buf, &mut cur)?;
            let new_heap_bytes = read_u64(buf, &mut cur)?;
            Ok(Response::OkVacuum(VacuumStats {
                active_nodes,
                old_slot_capacity,
                old_heap_bytes,
                new_heap_bytes,
            }))
        }
        MsgTag::AuthChallenge => {
            let raw_salt = read_bytes(buf, &mut cur, 16)?;
            let salt: [u8; 16] = raw_salt
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid AuthChallenge salt"))?;
            let iterations = read_u32(buf, &mut cur)?;
            let nonce_len = read_u32(buf, &mut cur)? as usize;
            let combined_nonce = read_bytes(buf, &mut cur, nonce_len)?;
            Ok(Response::AuthChallenge {
                salt,
                iterations,
                combined_nonce,
            })
        }
        MsgTag::AuthSuccess => {
            let raw_sig = read_bytes(buf, &mut cur, 32)?;
            let server_signature: [u8; 32] = raw_sig
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid AuthSuccess signature"))?;
            Ok(Response::AuthSuccess { server_signature })
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
        let mut flags = 0u8;
        if c.nullable {
            flags |= 1 << 0;
        }
        if c.indexed {
            flags |= 1 << 1;
        }
        if c.unique {
            flags |= 1 << 2;
        }
        b.push(flags);
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
        let flags = read_u8(buf, cur)?;
        columns.push(ColumnDef {
            name: col_name,
            data_type: DataType::from_raw(tag, param)?,
            nullable: (flags & (1 << 0)) != 0,
            indexed: (flags & (1 << 1)) != 0,
            unique: (flags & (1 << 2)) != 0,
        });
    }
    Ok(SegmentSchema {
        node_type,
        name,
        parent_type,
        columns,
    })
}

fn write_single_value(b: &mut Vec<u8>, v: &Value) {
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
        Value::Decimal(d) => {
            b.push(6);
            b.push(d.scale);
            b.extend_from_slice(&d.unscaled.to_le_bytes());
        }
    }
}

fn read_single_value(buf: &[u8], cur: &mut usize) -> Result<Value, HdbError> {
    let tag = read_u8(buf, cur)?;
    match tag {
        0 => Ok(Value::Null),
        1 => Ok(Value::Bool(read_u8(buf, cur)? != 0)),
        2 => {
            let arr: [u8; 8] = read_bytes(buf, cur, 8)?
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid I64 in RPC"))?;
            Ok(Value::I64(i64::from_le_bytes(arr)))
        }
        3 => {
            let arr: [u8; 8] = read_bytes(buf, cur, 8)?
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid F64 in RPC"))?;
            Ok(Value::F64(f64::from_le_bytes(arr)))
        }
        4 => Ok(Value::String(read_str(buf, cur)?)),
        5 => {
            let len = read_u32(buf, cur)? as usize;
            Ok(Value::Bytes(read_bytes(buf, cur, len)?))
        }
        6 => {
            let scale = read_u8(buf, cur)?;
            let arr: [u8; 16] = read_bytes(buf, cur, 16)?
                .try_into()
                .map_err(|_| HdbError::CorruptedData("Invalid Decimal in RPC"))?;
            Ok(Value::Decimal(Decimal {
                unscaled: i128::from_le_bytes(arr),
                scale,
            }))
        }
        _ => Err(HdbError::CorruptedData("Invalid Value tag in RPC")),
    }
}

fn write_values(b: &mut Vec<u8>, values: &[Value]) {
    b.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for v in values {
        write_single_value(b, v);
    }
}

fn read_values(buf: &[u8], cur: &mut usize) -> Result<Vec<Value>, HdbError> {
    let count = read_u16(buf, cur)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_single_value(buf, cur)?);
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
    let arr: [u8; 2] = b
        .try_into()
        .map_err(|_| HdbError::CorruptedData("Invalid u16"))?;
    Ok(u16::from_le_bytes(arr))
}

fn read_u32(buf: &[u8], cur: &mut usize) -> Result<u32, HdbError> {
    let b = read_bytes(buf, cur, 4)?;
    let arr: [u8; 4] = b
        .try_into()
        .map_err(|_| HdbError::CorruptedData("Invalid u32"))?;
    Ok(u32::from_le_bytes(arr))
}

fn read_u64(buf: &[u8], cur: &mut usize) -> Result<u64, HdbError> {
    let b = read_bytes(buf, cur, 8)?;
    let arr: [u8; 8] = b
        .try_into()
        .map_err(|_| HdbError::CorruptedData("Invalid u64"))?;
    Ok(u64::from_le_bytes(arr))
}

fn read_bytes(buf: &[u8], cur: &mut usize, len: usize) -> Result<Vec<u8>, HdbError> {
    if *cur + len > buf.len() {
        return Err(HdbError::CorruptedData("Unexpected EOF"));
    }
    let slice = buf[*cur..*cur + len].to_vec();
    *cur += len;
    Ok(slice)
}
