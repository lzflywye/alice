use hierarchical::{ColumnDef, DataType, Database, SegmentSchema, Value};
use net::{AuthRegistry, HierarchicalClient, HierarchicalServer, Request, Response, ServerConfig};
use std::fs;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_scram_authentication_success_and_rejection() {
    let dir = std::env::temp_dir().join(format!("net_auth_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    let db = Arc::new(Database::open(&dir).unwrap());
    let auth = Arc::new(AuthRegistry::new());
    auth.add_user("admin", "s3cr3t_pass").unwrap();

    let srv = HierarchicalServer::with_auth(db, auth);
    let addr = srv.start_listener("127.0.0.1:0").unwrap();

    // Reject connection when password is invalid
    assert!(HierarchicalClient::connect_with_auth(addr, "admin", "wrong").is_err());

    // Succeed when SCRAM-SHA-256 credentials match
    let mut client = HierarchicalClient::connect_with_auth(addr, "admin", "s3cr3t_pass").unwrap();
    assert!(matches!(
        client.call(Request::Ping).unwrap(),
        Response::OkEmpty
    ));

    srv.shutdown();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_max_connections_limit() {
    let dir = std::env::temp_dir().join(format!("net_maxconn_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    let db = Arc::new(Database::open(&dir).unwrap());
    let cfg = ServerConfig {
        max_connections: 1,
        ..ServerConfig::default()
    };
    let srv = HierarchicalServer::with_config(db, Arc::new(AuthRegistry::new()), cfg);
    let addr = srv.start_listener("127.0.0.1:0").unwrap();

    // First connection occupies the single available slot
    let mut c1 = HierarchicalClient::connect(addr).unwrap();
    assert!(matches!(c1.call(Request::Ping).unwrap(), Response::OkEmpty));

    // Second concurrent connection must receive a max-connections error response
    let mut c2 = HierarchicalClient::connect(addr).unwrap();
    let resp = c2.call(Request::Ping).unwrap();
    assert!(matches!(resp, Response::Err(msg) if msg.contains("Max connections")));

    srv.shutdown();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_active_standby_replication_and_promote() {
    let base = std::env::temp_dir().join(format!("net_repl_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);

    let primary_db = Arc::new(Database::open(base.join("primary")).unwrap());
    let primary_srv = HierarchicalServer::new(Arc::clone(&primary_db));
    let primary_addr = primary_srv.start_listener("127.0.0.1:0").unwrap();

    let mut client = HierarchicalClient::connect(primary_addr).unwrap();
    client
        .call(Request::DefineSegment(SegmentSchema {
            node_type: 1,
            name: "Org".into(),
            parent_type: None,
            columns: vec![ColumnDef::new("code", DataType::VarString, false)],
        }))
        .unwrap();
    let node_id = match client
        .call(Request::Insert {
            parent_id: 0,
            node_type: 1,
            values: vec![Value::String("HQ".into())],
        })
        .unwrap()
    {
        Response::OkNodeId(id) => id,
        other => panic!("Unexpected insert response: {other:?}"),
    };

    // Start standby server replicating WAL entries from primary
    let standby_db = Arc::new(Database::open(base.join("standby")).unwrap());
    let standby_srv = HierarchicalServer::new(Arc::clone(&standby_db));
    let standby_addr = standby_srv.start_listener("127.0.0.1:0").unwrap();
    standby_srv.start_standby_replication(primary_addr.to_string(), Duration::from_millis(20));

    std::thread::sleep(Duration::from_millis(120));

    let mut standby_client = HierarchicalClient::connect(standby_addr).unwrap();
    assert!(matches!(
        standby_client.call(Request::Get { node_id }).unwrap(),
        Response::OkRecord { .. }
    ));

    // Direct writes must be rejected while operating in read-only replica mode
    let rejected = standby_client
        .call(Request::Insert {
            parent_id: 0,
            node_type: 1,
            values: vec![Value::String("FAIL".into())],
        })
        .unwrap();
    assert!(matches!(rejected, Response::Err(_)));

    // Promoting standby to primary enables direct write operations
    standby_client.call(Request::Promote).unwrap();
    let promoted = standby_client
        .call(Request::Insert {
            parent_id: 0,
            node_type: 1,
            values: vec![Value::String("BRANCH".into())],
        })
        .unwrap();
    assert!(matches!(promoted, Response::OkNodeId(_)));

    primary_srv.shutdown();
    standby_srv.shutdown();
    let _ = fs::remove_dir_all(&base);
}
