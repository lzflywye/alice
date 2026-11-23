use common::DbError;
use hierarchical::catalog::{decode_catalog, encode_catalog};
use hierarchical::{
    Decimal, NodeId, NodeView, SegmentSchema, SnapshotBundle, VacuumStats, Value, WalRecord,
};

#[derive(Debug, Clone)]
pub enum Request {
    Ping,
    DefineSegment(SegmentSchema),
    Insert {
        parent_id: NodeId,
        node_type: u16,
        values: Vec<Value>,
    },
    Update {
        node_id: NodeId,
        values: Vec<Value>,
    },
    Delete {
        node_id: NodeId,
    },
    Get {
        node_id: NodeId,
    },
    ChildrenIds {
        parent_id: NodeId,
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
        root_node_id: NodeId,
        max_depth: Option<usize>,
    },
    Vacuum,
    AuthHello {
        username: String,
        client_nonce: Vec<u8>,
    },
    AuthProof {
        client_proof: Vec<u8>,
    },
}

#[derive(Debug, Clone)]
pub enum Response {
    OkEmpty,
    OkNodeId(NodeId),
    OkNodeIds(Vec<NodeId>),
    OkRecord {
        node_type: u16,
        values: Vec<Value>,
    },
    OkWalBatch(Vec<WalRecord>),
    OkCatalog(Vec<SegmentSchema>),
    OkSnapshot(SnapshotBundle),
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

pub fn encode_request(req: &Request) -> (u8, Vec<u8>) {
    let mut buf = Vec::new();
    let tag = match req {
        Request::Ping => 1,
        Request::DefineSegment(schema) => {
            buf.extend_from_slice(&encode_catalog(std::slice::from_ref(schema)));
            2
        }
        Request::Insert {
            parent_id,
            node_type,
            values,
        } => {
            buf.extend_from_slice(&parent_id.to_le_bytes());
            buf.extend_from_slice(&node_type.to_le_bytes());
            encode_values(&mut buf, values);
            3
        }
        Request::Update { node_id, values } => {
            buf.extend_from_slice(&node_id.to_le_bytes());
            encode_values(&mut buf, values);
            4
        }
        Request::Delete { node_id } => {
            buf.extend_from_slice(&node_id.to_le_bytes());
            5
        }
        Request::Get { node_id } => {
            buf.extend_from_slice(&node_id.to_le_bytes());
            6
        }
        Request::ChildrenIds { parent_id } => {
            buf.extend_from_slice(&parent_id.to_le_bytes());
            7
        }
        Request::FetchWal { after_lsn } => {
            buf.extend_from_slice(&after_lsn.to_le_bytes());
            8
        }
        Request::SyncCatalog => 9,
        Request::Promote => 10,
        Request::FetchSnapshot => 11,
        Request::FindByIndex {
            node_type,
            col_name,
            value,
        } => {
            buf.extend_from_slice(&node_type.to_le_bytes());
            let cb = col_name.as_bytes();
            buf.extend_from_slice(&(cb.len() as u16).to_le_bytes());
            buf.extend_from_slice(cb);
            encode_single_value(&mut buf, value);
            12
        }
        Request::WalkSubtree {
            root_node_id,
            max_depth,
        } => {
            buf.extend_from_slice(&root_node_id.to_le_bytes());
            match max_depth {
                Some(d) => {
                    buf.push(1);
                    buf.extend_from_slice(&(*d as u32).to_le_bytes());
                }
                None => buf.push(0),
            }
            13
        }
        Request::Vacuum => 14,
        Request::AuthHello {
            username,
            client_nonce,
        } => {
            let ub = username.as_bytes();
            buf.extend_from_slice(&(ub.len() as u16).to_le_bytes());
            buf.extend_from_slice(ub);
            buf.extend_from_slice(&(client_nonce.len() as u16).to_le_bytes());
            buf.extend_from_slice(client_nonce);
            15
        }
        Request::AuthProof { client_proof } => {
            buf.extend_from_slice(&(client_proof.len() as u16).to_le_bytes());
            buf.extend_from_slice(client_proof);
            16
        }
    };
    (tag, buf)
}

pub fn decode_request(tag: u8, buf: &[u8]) -> Result<Request, DbError> {
    match tag {
        1 => Ok(Request::Ping),
        2 => {
            let mut list = decode_catalog(buf)?;
            let schema = list
                .pop()
                .ok_or(DbError::CorruptedData("Empty DefineSegment"))?;
            Ok(Request::DefineSegment(schema))
        }
        3 => {
            if buf.len() < 10 {
                return Err(DbError::CorruptedData("Truncated Insert"));
            }
            let parent_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let node_type = u16::from_le_bytes(buf[8..10].try_into().unwrap());
            let (values, _) = decode_values(buf, 10)?;
            Ok(Request::Insert {
                parent_id,
                node_type,
                values,
            })
        }
        4 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated Update"));
            }
            let node_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let (values, _) = decode_values(buf, 8)?;
            Ok(Request::Update { node_id, values })
        }
        5 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated Delete"));
            }
            Ok(Request::Delete {
                node_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            })
        }
        6 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated Get"));
            }
            Ok(Request::Get {
                node_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            })
        }
        7 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated ChildrenIds"));
            }
            Ok(Request::ChildrenIds {
                parent_id: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            })
        }
        8 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated FetchWal"));
            }
            Ok(Request::FetchWal {
                after_lsn: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            })
        }
        9 => Ok(Request::SyncCatalog),
        10 => Ok(Request::Promote),
        11 => Ok(Request::FetchSnapshot),
        12 => {
            if buf.len() < 4 {
                return Err(DbError::CorruptedData("Truncated FindByIndex"));
            }
            let node_type = u16::from_le_bytes(buf[0..2].try_into().unwrap());
            let clen = u16::from_le_bytes(buf[2..4].try_into().unwrap()) as usize;
            if buf.len() < 4 + clen {
                return Err(DbError::CorruptedData("Truncated FindByIndex col"));
            }
            let col_name =
                std::str::from_utf8(&buf[4..4 + clen]).map_err(|_| DbError::InvalidUtf8)?;
            let (value, _) = decode_single_value(buf, 4 + clen)?;
            Ok(Request::FindByIndex {
                node_type,
                col_name: col_name.to_string(),
                value,
            })
        }
        13 => {
            if buf.len() < 9 {
                return Err(DbError::CorruptedData("Truncated WalkSubtree"));
            }
            let root_node_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let max_depth = if buf[8] != 0 {
                if buf.len() < 13 {
                    return Err(DbError::CorruptedData("Truncated WalkSubtree depth"));
                }
                Some(u32::from_le_bytes(buf[9..13].try_into().unwrap()) as usize)
            } else {
                None
            };
            Ok(Request::WalkSubtree {
                root_node_id,
                max_depth,
            })
        }
        14 => Ok(Request::Vacuum),
        15 => {
            if buf.len() < 2 {
                return Err(DbError::CorruptedData("Truncated AuthHello"));
            }
            let ulen = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
            if buf.len() < 4 + ulen {
                return Err(DbError::CorruptedData("Truncated AuthHello user"));
            }
            let username =
                std::str::from_utf8(&buf[2..2 + ulen]).map_err(|_| DbError::InvalidUtf8)?;
            let nlen = u16::from_le_bytes(buf[2 + ulen..4 + ulen].try_into().unwrap()) as usize;
            if buf.len() < 4 + ulen + nlen {
                return Err(DbError::CorruptedData("Truncated AuthHello nonce"));
            }
            Ok(Request::AuthHello {
                username: username.to_string(),
                client_nonce: buf[4 + ulen..4 + ulen + nlen].to_vec(),
            })
        }
        16 => {
            if buf.len() < 2 {
                return Err(DbError::CorruptedData("Truncated AuthProof"));
            }
            let plen = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
            if buf.len() < 2 + plen {
                return Err(DbError::CorruptedData("Truncated AuthProof body"));
            }
            Ok(Request::AuthProof {
                client_proof: buf[2..2 + plen].to_vec(),
            })
        }
        _ => Err(DbError::CorruptedData("Unknown request tag")),
    }
}

pub fn encode_response(resp: &Response) -> (u8, Vec<u8>) {
    let mut buf = Vec::new();
    let tag = match resp {
        Response::OkEmpty => 100,
        Response::OkNodeId(id) => {
            buf.extend_from_slice(&id.to_le_bytes());
            101
        }
        Response::OkNodeIds(ids) => {
            buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
            for id in ids {
                buf.extend_from_slice(&id.to_le_bytes());
            }
            102
        }
        Response::OkRecord { node_type, values } => {
            buf.extend_from_slice(&node_type.to_le_bytes());
            encode_values(&mut buf, values);
            103
        }
        Response::OkWalBatch(entries) => {
            buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
            for e in entries {
                buf.extend_from_slice(&e.lsn.to_le_bytes());
                buf.extend_from_slice(&(e.payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(&e.payload);
            }
            104
        }
        Response::OkCatalog(schemas) => {
            buf.extend_from_slice(&encode_catalog(schemas));
            105
        }
        Response::OkSnapshot(snap) => {
            buf.extend_from_slice(&snap.base_lsn.to_le_bytes());
            buf.extend_from_slice(&(snap.nodes_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&snap.nodes_bytes);
            buf.extend_from_slice(&(snap.heap_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&snap.heap_bytes);
            let cat = encode_catalog(&snap.schemas);
            buf.extend_from_slice(&(cat.len() as u32).to_le_bytes());
            buf.extend_from_slice(&cat);
            106
        }
        Response::OkNodeViews(views) => {
            buf.extend_from_slice(&(views.len() as u32).to_le_bytes());
            for v in views {
                buf.extend_from_slice(&v.node_id.to_le_bytes());
                buf.extend_from_slice(&v.parent_id.to_le_bytes());
                buf.extend_from_slice(&v.node_type.to_le_bytes());
                buf.extend_from_slice(&(v.depth as u32).to_le_bytes());
                encode_values(&mut buf, &v.values);
            }
            107
        }
        Response::OkVacuum(stats) => {
            buf.extend_from_slice(&(stats.live_nodes as u64).to_le_bytes());
            buf.extend_from_slice(&(stats.reclaimed_slots as u64).to_le_bytes());
            buf.extend_from_slice(&stats.old_heap_bytes.to_le_bytes());
            buf.extend_from_slice(&stats.new_heap_bytes.to_le_bytes());
            108
        }
        Response::AuthChallenge {
            salt,
            iterations,
            combined_nonce,
        } => {
            buf.extend_from_slice(salt);
            buf.extend_from_slice(&iterations.to_le_bytes());
            buf.extend_from_slice(&(combined_nonce.len() as u16).to_le_bytes());
            buf.extend_from_slice(combined_nonce);
            109
        }
        Response::AuthSuccess { server_signature } => {
            buf.extend_from_slice(server_signature);
            110
        }
        Response::Err(msg) => {
            buf.extend_from_slice(msg.as_bytes());
            255
        }
    };
    (tag, buf)
}

pub fn decode_response(tag: u8, buf: &[u8]) -> Result<Response, DbError> {
    match tag {
        100 => Ok(Response::OkEmpty),
        101 => {
            if buf.len() < 8 {
                return Err(DbError::CorruptedData("Truncated OkNodeId"));
            }
            Ok(Response::OkNodeId(u64::from_le_bytes(
                buf[0..8].try_into().unwrap(),
            )))
        }
        102 => {
            if buf.len() < 4 {
                return Err(DbError::CorruptedData("Truncated OkNodeIds"));
            }
            let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            let mut cur = 4;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                if cur + 8 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated OkNodeIds item"));
                }
                ids.push(u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap()));
                cur += 8;
            }
            Ok(Response::OkNodeIds(ids))
        }
        103 => {
            if buf.len() < 2 {
                return Err(DbError::CorruptedData("Truncated OkRecord"));
            }
            let node_type = u16::from_le_bytes(buf[0..2].try_into().unwrap());
            let (values, _) = decode_values(buf, 2)?;
            Ok(Response::OkRecord { node_type, values })
        }
        104 => {
            if buf.len() < 4 {
                return Err(DbError::CorruptedData("Truncated OkWalBatch"));
            }
            let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            let mut cur = 4;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                if cur + 12 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated OkWalBatch entry"));
                }
                let lsn = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap());
                let plen = u32::from_le_bytes(buf[cur + 8..cur + 12].try_into().unwrap()) as usize;
                cur += 12;
                if cur + plen > buf.len() {
                    return Err(DbError::CorruptedData("Truncated OkWalBatch payload"));
                }
                entries.push(WalRecord {
                    lsn,
                    payload: buf[cur..cur + plen].to_vec(),
                });
                cur += plen;
            }
            Ok(Response::OkWalBatch(entries))
        }
        105 => Ok(Response::OkCatalog(decode_catalog(buf)?)),
        106 => {
            if buf.len() < 12 {
                return Err(DbError::CorruptedData("Truncated OkSnapshot"));
            }
            let base_lsn = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            let nlen = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
            let mut cur = 12;
            if cur + nlen + 4 > buf.len() {
                return Err(DbError::CorruptedData("Truncated OkSnapshot nodes"));
            }
            let nodes_bytes = buf[cur..cur + nlen].to_vec();
            cur += nlen;
            let hlen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;
            if cur + hlen + 4 > buf.len() {
                return Err(DbError::CorruptedData("Truncated OkSnapshot heap"));
            }
            let heap_bytes = buf[cur..cur + hlen].to_vec();
            cur += hlen;
            let clen = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;
            if cur + clen > buf.len() {
                return Err(DbError::CorruptedData("Truncated OkSnapshot catalog"));
            }
            let schemas = decode_catalog(&buf[cur..cur + clen])?;
            Ok(Response::OkSnapshot(SnapshotBundle {
                base_lsn,
                nodes_bytes,
                heap_bytes,
                schemas,
            }))
        }
        107 => {
            if buf.len() < 4 {
                return Err(DbError::CorruptedData("Truncated OkNodeViews"));
            }
            let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            let mut cur = 4;
            let mut views = Vec::with_capacity(count);
            for _ in 0..count {
                if cur + 22 > buf.len() {
                    return Err(DbError::CorruptedData("Truncated NodeView"));
                }
                let node_id = u64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap());
                let parent_id = u64::from_le_bytes(buf[cur + 8..cur + 16].try_into().unwrap());
                let node_type = u16::from_le_bytes(buf[cur + 16..cur + 18].try_into().unwrap());
                let depth =
                    u32::from_le_bytes(buf[cur + 18..cur + 22].try_into().unwrap()) as usize;
                cur += 22;
                let (values, next_cur) = decode_values(buf, cur)?;
                cur = next_cur;
                views.push(NodeView {
                    node_id,
                    parent_id,
                    node_type,
                    depth,
                    values,
                });
            }
            Ok(Response::OkNodeViews(views))
        }
        108 => {
            if buf.len() < 32 {
                return Err(DbError::CorruptedData("Truncated OkVacuum"));
            }
            Ok(Response::OkVacuum(VacuumStats {
                live_nodes: u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize,
                reclaimed_slots: u64::from_le_bytes(buf[8..16].try_into().unwrap()) as usize,
                old_heap_bytes: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
                new_heap_bytes: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
            }))
        }
        109 => {
            if buf.len() < 22 {
                return Err(DbError::CorruptedData("Truncated AuthChallenge"));
            }
            let mut salt = [0u8; 16];
            salt.copy_from_slice(&buf[0..16]);
            let iterations = u32::from_le_bytes(buf[16..20].try_into().unwrap());
            let nlen = u16::from_le_bytes(buf[20..22].try_into().unwrap()) as usize;
            if buf.len() < 22 + nlen {
                return Err(DbError::CorruptedData("Truncated AuthChallenge nonce"));
            }
            Ok(Response::AuthChallenge {
                salt,
                iterations,
                combined_nonce: buf[22..22 + nlen].to_vec(),
            })
        }
        110 => {
            if buf.len() < 32 {
                return Err(DbError::CorruptedData("Truncated AuthSuccess"));
            }
            let mut server_signature = [0u8; 32];
            server_signature.copy_from_slice(&buf[0..32]);
            Ok(Response::AuthSuccess { server_signature })
        }
        255 => {
            let msg = String::from_utf8_lossy(buf).into_owned();
            Ok(Response::Err(msg))
        }
        _ => Err(DbError::CorruptedData("Unknown response tag")),
    }
}

fn encode_values(buf: &mut Vec<u8>, values: &[Value]) {
    buf.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for v in values {
        encode_single_value(buf, v);
    }
}

fn encode_single_value(buf: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => buf.push(0),
        Value::Bool(b) => {
            buf.push(1);
            buf.push(u8::from(*b));
        }
        Value::I64(n) => {
            buf.push(2);
            buf.extend_from_slice(&n.to_le_bytes());
        }
        Value::F64(f) => {
            buf.push(3);
            buf.extend_from_slice(&f.to_le_bytes());
        }
        Value::Decimal(d) => {
            buf.push(4);
            buf.extend_from_slice(&d.unscaled.to_le_bytes());
            buf.push(d.scale);
        }
        Value::String(s) => {
            buf.push(5);
            let raw = s.as_bytes();
            buf.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            buf.extend_from_slice(raw);
        }
        Value::Bytes(b) => {
            buf.push(6);
            buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
            buf.extend_from_slice(b);
        }
    }
}

fn decode_values(buf: &[u8], start: usize) -> Result<(Vec<Value>, usize), DbError> {
    if start + 2 > buf.len() {
        return Err(DbError::CorruptedData("Truncated values count"));
    }
    let count = u16::from_le_bytes(buf[start..start + 2].try_into().unwrap()) as usize;
    let mut cur = start + 2;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let (v, next) = decode_single_value(buf, cur)?;
        out.push(v);
        cur = next;
    }
    Ok((out, cur))
}

fn decode_single_value(buf: &[u8], offset: usize) -> Result<(Value, usize), DbError> {
    if offset >= buf.len() {
        return Err(DbError::CorruptedData("Truncated value tag"));
    }
    let tag = buf[offset];
    let cur = offset + 1;
    match tag {
        0 => Ok((Value::Null, cur)),
        1 => {
            if cur + 1 > buf.len() {
                return Err(DbError::CorruptedData("Truncated Bool"));
            }
            Ok((Value::Bool(buf[cur] != 0), cur + 1))
        }
        2 => {
            if cur + 8 > buf.len() {
                return Err(DbError::CorruptedData("Truncated I64"));
            }
            Ok((
                Value::I64(i64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap())),
                cur + 8,
            ))
        }
        3 => {
            if cur + 8 > buf.len() {
                return Err(DbError::CorruptedData("Truncated F64"));
            }
            Ok((
                Value::F64(f64::from_le_bytes(buf[cur..cur + 8].try_into().unwrap())),
                cur + 8,
            ))
        }
        4 => {
            if cur + 17 > buf.len() {
                return Err(DbError::CorruptedData("Truncated Decimal"));
            }
            let unscaled = i128::from_le_bytes(buf[cur..cur + 16].try_into().unwrap());
            let scale = buf[cur + 16];
            Ok((Value::Decimal(Decimal::new(unscaled, scale)), cur + 17))
        }
        5 => {
            if cur + 4 > buf.len() {
                return Err(DbError::CorruptedData("Truncated String len"));
            }
            let len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            let body = cur + 4;
            if body + len > buf.len() {
                return Err(DbError::CorruptedData("Truncated String body"));
            }
            let s =
                std::str::from_utf8(&buf[body..body + len]).map_err(|_| DbError::InvalidUtf8)?;
            Ok((Value::String(s.to_string()), body + len))
        }
        6 => {
            if cur + 4 > buf.len() {
                return Err(DbError::CorruptedData("Truncated Bytes len"));
            }
            let len = u32::from_le_bytes(buf[cur..cur + 4].try_into().unwrap()) as usize;
            let body = cur + 4;
            if body + len > buf.len() {
                return Err(DbError::CorruptedData("Truncated Bytes body"));
            }
            Ok((Value::Bytes(buf[body..body + len].to_vec()), body + len))
        }
        _ => Err(DbError::CorruptedData("Unknown value tag")),
    }
}
