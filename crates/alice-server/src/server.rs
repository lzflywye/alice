use crate::protocol::{
    Request, Response, decode_request, decode_response, encode_request, encode_response,
    read_frame, write_frame,
};
use alice_core::db::Database;
use alice_core::error::HdbError;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

/// Simple synchronous TCP client for Alice Hierarchical Database.
pub struct AliceClient {
    stream: TcpStream,
}

impl AliceClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, HdbError> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    pub fn call(&mut self, req: Request) -> Result<Response, HdbError> {
        let (tag, body) = encode_request(&req);
        write_frame(&mut self.stream, tag, &body)?;
        let (resp_tag, resp_body) = read_frame(&mut self.stream)?;
        decode_response(resp_tag, &resp_body)
    }
}

/// TCP Server wrapper around `Database` supporting both Active and Standby modes.
pub struct AliceServer {
    db: Arc<Database>,
    shutdown: Arc<AtomicBool>,
}

impl AliceServer {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Starts accepting client & replication connections in background threads.
    pub fn start_listener<A: ToSocketAddrs>(
        &self,
        bind_addr: A,
    ) -> Result<std::net::SocketAddr, HdbError> {
        let listener = TcpListener::bind(bind_addr)?;
        listener.set_nonblocking(true)?;
        let local_addr = listener.local_addr()?;

        let db = Arc::clone(&self.db);
        let shutdown = Arc::clone(&self.shutdown);

        thread::spawn(move || {
            while !shutdown.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let db_conn = Arc::clone(&db);
                        let shutdown_conn = Arc::clone(&shutdown);
                        thread::spawn(move || {
                            let _ = handle_connection(stream, db_conn, shutdown_conn);
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

    /// Starts background WAL replication loop pulling changes from an Active primary.
    pub fn start_standby_replication(&self, primary_addr: String, poll_interval: Duration) {
        if self.db.set_replica_mode(true).is_err() {
            return;
        }
        let db = Arc::clone(&self.db);
        let shutdown = Arc::clone(&self.shutdown);

        thread::spawn(move || {
            let mut client: Option<AliceClient> = None;

            while !shutdown.load(Ordering::SeqCst) {
                // Establish or reconnect to primary
                if client.is_none() {
                    if let Ok(mut c) = AliceClient::connect(&primary_addr) {
                        // Sync segment schemas first
                        if let Ok(Response::OkCatalog(schemas)) = c.call(Request::SyncCatalog) {
                            let _ = db.import_schemas(schemas);
                            client = Some(c);
                        }
                    } else {
                        thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                }

                if let Some(ref mut c) = client
                    && let Ok(after_lsn) = db.current_lsn()
                {
                    match c.call(Request::FetchWal { after_lsn }) {
                        Ok(Response::OkWalBatch(entries)) => {
                            if !entries.is_empty() {
                                let _ = db.apply_replicated_entries(&entries);
                            } else {
                                thread::sleep(poll_interval);
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
    shutdown: Arc<AtomicBool>,
) -> Result<(), HdbError> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;

    while !shutdown.load(Ordering::SeqCst) {
        let (tag, body) = match read_frame(&mut stream) {
            Ok(frame) => frame,
            Err(HdbError::Io(ref e))
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
    }
}
