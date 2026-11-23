use crate::auth::{
    AuthRegistry, build_auth_message, compute_client_proof, generate_nonce, verify_client_proof,
};
use crate::config::ServerConfig;
use crate::framing::{read_frame, write_frame};
use crate::hierarchical_proto::{
    Request, Response, decode_request, decode_response, encode_request, encode_response,
};
use common::DbError;
use hierarchical::Database;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

struct ActiveConnGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for ActiveConnGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct HierarchicalClient {
    stream: TcpStream,
}

impl HierarchicalClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, DbError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    pub fn connect_with_auth<A: ToSocketAddrs>(
        addr: A,
        username: &str,
        password: &str,
    ) -> Result<Self, DbError> {
        let mut client = Self::connect(addr)?;
        client.authenticate(username, password)?;
        Ok(client)
    }

    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<(), DbError> {
        let client_nonce = generate_nonce()?;
        let resp = self.call(Request::AuthHello {
            username: username.to_string(),
            client_nonce: client_nonce.to_vec(),
        })?;

        let (salt, iterations, combined_nonce) = match resp {
            Response::AuthChallenge {
                salt,
                iterations,
                combined_nonce,
            } => (salt, iterations, combined_nonce),
            Response::Err(_) => return Err(DbError::CorruptedData("Authentication rejected")),
            _ => return Err(DbError::CorruptedData("Unexpected response to AuthHello")),
        };

        if !combined_nonce.starts_with(&client_nonce) || combined_nonce.len() <= client_nonce.len()
        {
            return Err(DbError::CorruptedData("Invalid combined nonce from server"));
        }

        let auth_msg = build_auth_message(username, &combined_nonce, &salt);
        let (client_proof, expected_server_sig) =
            compute_client_proof(password, &salt, iterations, &auth_msg)?;

        match self.call(Request::AuthProof { client_proof: client_proof.to_vec() })? {
            Response::AuthSuccess { server_signature } => {
                let mut diff: u8 = 0;
                for (a, b) in server_signature.iter().zip(expected_server_sig.iter()) {
                    diff |= a ^ b;
                }
                if diff != 0 {
                    return Err(DbError::CorruptedData("Server signature mismatch"));
                }
                Ok(())
            }
            Response::Err(_) => Err(DbError::CorruptedData("Invalid credentials")),
            _ => Err(DbError::CorruptedData("Unexpected response to AuthProof")),
        }
    }

    pub fn call(&mut self, req: Request) -> Result<Response, DbError> {
        let (tag, body) = encode_request(&req);
        write_frame(&mut self.stream, tag, &body)?;
        let (resp_tag, resp_body) = read_frame(&mut self.stream)?;
        decode_response(resp_tag, &resp_body)
    }
}

pub struct HierarchicalServer {
    db: Arc<Database>,
    auth: Arc<AuthRegistry>,
    server_cfg: ServerConfig,
    active_conns: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl HierarchicalServer {
    pub fn new(db: Arc<Database>) -> Self {
        Self::with_config(db, Arc::new(AuthRegistry::new()), ServerConfig::default())
    }

    pub fn with_auth(db: Arc<Database>, auth: Arc<AuthRegistry>) -> Self {
        Self::with_config(db, auth, ServerConfig::default())
    }

    pub fn with_config(
        db: Arc<Database>,
        auth: Arc<AuthRegistry>,
        server_cfg: ServerConfig,
    ) -> Self {
        Self {
            db,
            auth,
            server_cfg,
            active_conns: Arc::new(AtomicUsize::new(0)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn auth_registry(&self) -> Arc<AuthRegistry> {
        Arc::clone(&self.auth)
    }

    pub fn active_connections(&self) -> usize {
        self.active_conns.load(Ordering::SeqCst)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn start_listener<A: ToSocketAddrs>(
        &self,
        bind_addr: A,
    ) -> Result<std::net::SocketAddr, DbError> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let db = Arc::clone(&self.db);
        let auth = Arc::clone(&self.auth);
        let active_conns = Arc::clone(&self.active_conns);
        let shutdown = Arc::clone(&self.shutdown);
        let max_connections = self.server_cfg.max_connections;
        let read_timeout = Duration::from_millis(self.server_cfg.read_timeout_ms);

        thread::spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let current = active_conns.load(Ordering::SeqCst);
                        if current >= max_connections {
                            let (t, b) = encode_response(&Response::Err(
                                "Max connections limit reached".into(),
                            ));
                            let _ = write_frame(&mut stream, t, &b);
                            continue;
                        }

                        active_conns.fetch_add(1, Ordering::SeqCst);
                        let guard = ActiveConnGuard {
                            counter: Arc::clone(&active_conns),
                        };

                        let db_conn = Arc::clone(&db);
                        let auth_conn = Arc::clone(&auth);
                        let shutdown_conn = Arc::clone(&shutdown);
                        thread::spawn(move || {
                            let _guard = guard;
                            let _ = handle_connection(
                                stream,
                                db_conn,
                                auth_conn,
                                shutdown_conn,
                                read_timeout,
                            );
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(local_addr)
    }

    pub fn start_standby_replication(&self, primary_addr: String, poll_interval: Duration) {
        self.start_standby_replication_with_auth(primary_addr, poll_interval, None);
    }

    pub fn start_standby_replication_with_auth(
        &self,
        primary_addr: String,
        poll_interval: Duration,
        credentials: Option<(String, String)>,
    ) {
        if self.db.set_replica_mode(true).is_err() {
            return;
        }
        let db = Arc::clone(&self.db);
        let shutdown = Arc::clone(&self.shutdown);

        thread::spawn(move || {
            let mut client: Option<HierarchicalClient> = None;

            while !shutdown.load(Ordering::SeqCst) {
                if client.is_none() {
                    let conn_res = match &credentials {
                        Some((user, pass)) => {
                            HierarchicalClient::connect_with_auth(&primary_addr, user, pass)
                        }
                        None => HierarchicalClient::connect(&primary_addr),
                    };

                    if let Ok(mut c) = conn_res {
                        if let Ok(Response::OkCatalog(schemas)) = c.call(Request::SyncCatalog) {
                            let _ = db.import_schemas(schemas);
                            client = Some(c);
                        }
                    } else {
                        thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                }

                if let Some(ref mut c) = client {
                    let Ok(after_lsn) = db.current_lsn() else {
                        break;
                    };
                    match c.call(Request::FetchWal { after_lsn }) {
                        Ok(Response::OkWalBatch(entries)) => {
                            if !entries.is_empty() {
                                if let Ok(Response::OkCatalog(schemas)) =
                                    c.call(Request::SyncCatalog)
                                {
                                    let _ = db.import_schemas(schemas);
                                }
                                let _ = db.apply_replicated_entries(&entries);
                            } else {
                                thread::sleep(poll_interval);
                            }
                        }
                        Ok(Response::Err(msg)) if msg.contains("ReplicationLag") => {
                            if let Ok(Response::OkSnapshot(snap)) = c.call(Request::FetchSnapshot) {
                                let _ = db.install_snapshot(snap);
                            }
                        }
                        _ => {
                            client = None;
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
        });
    }
}

fn handle_connection(
    mut stream: TcpStream,
    db: Arc<Database>,
    auth: Arc<AuthRegistry>,
    shutdown: Arc<AtomicBool>,
    read_timeout: Duration,
) -> Result<(), DbError> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(read_timeout))?;

    let auth_required = auth.is_enabled().unwrap_or(true);
    if auth_required && !perform_server_handshake(&mut stream, &auth, &shutdown)? {
        return Ok(());
    }

    while !shutdown.load(Ordering::SeqCst) {
        let (tag, body) = match read_frame(&mut stream) {
            Ok(frame) => frame,
            Err(DbError::Io(ref e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => break,
        };

        let resp = match decode_request(tag, &body) {
            Ok(req) => dispatch_request(&db, req),
            Err(e) => Response::Err(format!("{:?}", e)),
        };

        let (resp_tag, resp_body) = encode_response(&resp);
        if write_frame(&mut stream, resp_tag, &resp_body).is_err() {
            break;
        }
    }
    Ok(())
}

fn perform_server_handshake(
    stream: &mut TcpStream,
    auth: &AuthRegistry,
    shutdown: &AtomicBool,
) -> Result<bool, DbError> {
    let (tag1, body1) = loop {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match read_frame(stream) {
            Ok(frame) => break frame,
            Err(DbError::Io(ref e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => return Ok(false),
        }
    };

    let (username, client_nonce) = match decode_request(tag1, &body1) {
        Ok(Request::AuthHello {
            username,
            client_nonce,
        }) if !client_nonce.is_empty() => (username, client_nonce),
        _ => {
            let (t, b) = encode_response(&Response::Err("Authentication required".into()));
            let _ = write_frame(stream, t, &b);
            return Ok(false);
        }
    };

    let Some(cred) = auth.get_user(&username)? else {
        let (t, b) = encode_response(&Response::Err("Authentication failed".into()));
        let _ = write_frame(stream, t, &b);
        return Ok(false);
    };

    let server_nonce = generate_nonce()?;
    let mut combined_nonce = Vec::with_capacity(client_nonce.len() + server_nonce.len());
    combined_nonce.extend_from_slice(&client_nonce);
    combined_nonce.extend_from_slice(&server_nonce);

    let (ct, cb) = encode_response(&Response::AuthChallenge {
        salt: cred.salt,
        iterations: cred.iterations,
        combined_nonce: combined_nonce.clone(),
    });
    write_frame(stream, ct, &cb)?;

    let (tag2, body2) = loop {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(false);
        }
        match read_frame(stream) {
            Ok(frame) => break frame,
            Err(DbError::Io(ref e))
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(_) => return Ok(false),
        }
    };

    let client_proof = match decode_request(tag2, &body2) {
        Ok(Request::AuthProof { client_proof }) => client_proof,
        _ => {
            let (t, b) = encode_response(&Response::Err("Expected AuthProof".into()));
            let _ = write_frame(stream, t, &b);
            return Ok(false);
        }
    };

    let auth_msg = build_auth_message(&username, &combined_nonce, &cred.salt);
    match verify_client_proof(&cred, &auth_msg, &client_proof) {
        Some(server_signature) => {
            let (st, sb) = encode_response(&Response::AuthSuccess { server_signature });
            write_frame(stream, st, &sb)?;
            Ok(true)
        }
        None => {
            let (et, eb) = encode_response(&Response::Err("Authentication failed".into()));
            let _ = write_frame(stream, et, &eb);
            Ok(false)
        }
    }
}

fn dispatch_request(db: &Database, req: Request) -> Response {
    match req {
        Request::Ping => Response::OkEmpty,
        Request::DefineSegment(schema) => match db.define_segment(schema) {
            Ok(()) => Response::OkEmpty,
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Insert {
            parent_id,
            node_type,
            values,
        } => match db.insert(parent_id, node_type, &values) {
            Ok(id) => Response::OkNodeId(id),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Update { node_id, values } => match db.update(node_id, &values) {
            Ok(()) => Response::OkEmpty,
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Delete { node_id } => match db.delete(node_id) {
            Ok(()) => Response::OkEmpty,
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Get { node_id } => match db.get(node_id) {
            Ok((node_type, values)) => Response::OkRecord { node_type, values },
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::ChildrenIds { parent_id } => match db.children_ids(parent_id) {
            Ok(ids) => Response::OkNodeIds(ids),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::FetchWal { after_lsn } => match db.fetch_wal_since(after_lsn) {
            Ok(entries) => Response::OkWalBatch(entries),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::SyncCatalog => match db.export_schemas() {
            Ok(schemas) => Response::OkCatalog(schemas),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Promote => match db.set_replica_mode(false) {
            Ok(()) => Response::OkEmpty,
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::FetchSnapshot => match db.create_snapshot() {
            Ok(snap) => Response::OkSnapshot(snap),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::FindByIndex {
            node_type,
            col_name,
            value,
        } => match db.find_by_index(node_type, &col_name, &value) {
            Ok(ids) => Response::OkNodeIds(ids),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::WalkSubtree {
            root_node_id,
            max_depth,
        } => match db.walk_subtree(root_node_id, max_depth) {
            Ok(views) => Response::OkNodeViews(views),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::Vacuum => match db.vacuum() {
            Ok(stats) => Response::OkVacuum(stats),
            Err(e) => Response::Err(format!("{:?}", e)),
        },
        Request::AuthHello { .. } | Request::AuthProof { .. } => {
            Response::Err("Already authenticated".into())
        }
    }
}
