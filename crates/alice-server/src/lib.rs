pub mod auth;
pub mod config;
pub mod protocol;
pub mod server;

#[cfg(test)]
mod tests {
    use crate::auth::AuthRegistry;
    use crate::protocol::{Request, Response};
    use crate::server::{AliceClient, AliceServer};
    use alice_core::db::Database;
    use alice_core::schema::{ColumnDef, DataType, SegmentSchema, Value};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_scram_authentication_and_authenticated_replication() {
        let base = std::env::temp_dir().join(format!("alice_auth_ha_{}", std::process::id()));
        let active_dir = base.join("active");
        let standby_dir = base.join("standby");
        let _ = std::fs::remove_dir_all(&base);

        let auth = Arc::new(AuthRegistry::new());
        auth.add_user("admin", "s3cr3t-p@ssw0rd!").unwrap();
        auth.add_user("replicator", "repl-t0k3n-99").unwrap();

        let active_db = Arc::new(Database::open(&active_dir).unwrap());
        let active_srv = AliceServer::with_auth(Arc::clone(&active_db), Arc::clone(&auth));
        let active_addr = active_srv.start_listener("127.0.0.1:0").unwrap();

        // 1. Unauthenticated client must be rejected on first RPC
        let mut unauth_client = AliceClient::connect(active_addr).unwrap();
        let unauth_res = unauth_client.call(Request::Ping).unwrap();
        assert!(matches!(unauth_res, Response::Err(_)));

        // 2. Wrong password must fail authentication
        assert!(AliceClient::connect_with_auth(active_addr, "admin", "wrong-pass").is_err());

        // 3. Valid credentials succeed
        let mut admin_client =
            AliceClient::connect_with_auth(active_addr, "admin", "s3cr3t-p@ssw0rd!").unwrap();
        assert!(matches!(
            admin_client.call(Request::Ping).unwrap(),
            Response::OkEmpty
        ));

        admin_client
            .call(Request::DefineSegment(SegmentSchema {
                node_type: 1,
                name: "Account".into(),
                parent_type: None,
                columns: vec![
                    ColumnDef::new("acct_id", DataType::FixedString(16), false).with_index(true),
                ],
            }))
            .unwrap();

        admin_client
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![Value::String("ACC-777".into())],
            })
            .unwrap();

        // 4. Standby authenticates with `replicator` credentials and syncs data
        let standby_db = Arc::new(Database::open(&standby_dir).unwrap());
        let standby_srv = AliceServer::new(Arc::clone(&standby_db));
        standby_srv.start_standby_replication_with_auth(
            active_addr.to_string(),
            Duration::from_millis(10),
            Some(("replicator".into(), "repl-t0k3n-99".into())),
        );

        thread::sleep(Duration::from_millis(80));

        assert_eq!(
            standby_db
                .find_by_index(1, "acct_id", &Value::String("ACC-777".into()))
                .unwrap(),
            vec![1]
        );

        active_srv.shutdown();
        standby_srv.shutdown();
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_active_standby_replication_and_failover() {
        let base = std::env::temp_dir().join(format!("alice_ha_{}", std::process::id()));
        let active_dir = base.join("active");
        let standby_dir = base.join("standby");
        let _ = std::fs::remove_dir_all(&base);

        let active_db = Arc::new(Database::open(&active_dir).unwrap());
        let active_srv = AliceServer::new(Arc::clone(&active_db));
        let active_addr = active_srv.start_listener("127.0.0.1:0").unwrap();

        let standby_db = Arc::new(Database::open(&standby_dir).unwrap());
        let standby_srv = AliceServer::new(Arc::clone(&standby_db));
        let standby_addr = standby_srv.start_listener("127.0.0.1:0").unwrap();
        standby_srv.start_standby_replication(active_addr.to_string(), Duration::from_millis(10));

        let mut client_active = AliceClient::connect(active_addr).unwrap();
        let resp = client_active
            .call(Request::DefineSegment(SegmentSchema {
                node_type: 1,
                name: "Dept".into(),
                parent_type: None,
                columns: vec![
                    ColumnDef::new("code", DataType::FixedString(8), false).with_index(true),
                    ColumnDef::new("desc", DataType::VarString, false),
                ],
            }))
            .unwrap();
        assert!(matches!(resp, Response::OkEmpty));

        let node_id = match client_active
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![
                    Value::String("ENG-01".into()),
                    Value::String("Core Engineering".into()),
                ],
            })
            .unwrap()
        {
            Response::OkNodeId(id) => id,
            other => panic!("Unexpected response: {:?}", other),
        };

        thread::sleep(Duration::from_millis(80));

        let mut client_standby = AliceClient::connect(standby_addr).unwrap();
        match client_standby.call(Request::Get { node_id }).unwrap() {
            Response::OkRecord { node_type, values } => {
                assert_eq!(node_type, 1);
                assert_eq!(values[0], Value::String("ENG-01".into()));
                assert_eq!(values[1], Value::String("Core Engineering".into()));
            }
            other => panic!("Standby read failed: {:?}", other),
        }

        assert_eq!(
            standby_db
                .find_by_index(1, "code", &Value::String("ENG-01".into()))
                .unwrap(),
            vec![node_id]
        );

        let write_attempt = client_standby
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![
                    Value::String("FAIL".into()),
                    Value::String("Should fail".into()),
                ],
            })
            .unwrap();
        assert!(matches!(write_attempt, Response::Err(_)));

        active_srv.shutdown();
        assert!(matches!(
            client_standby.call(Request::Promote).unwrap(),
            Response::OkEmpty
        ));

        let new_id = match client_standby
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![
                    Value::String("OPS-02".into()),
                    Value::String("Promoted Primary".into()),
                ],
            })
            .unwrap()
        {
            Response::OkNodeId(id) => id,
            other => panic!("Write after promote failed: {:?}", other),
        };
        assert_eq!(new_id, 2);

        standby_srv.shutdown();
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_extended_rpcs_and_late_standby_snapshot_sync() {
        let base = std::env::temp_dir().join(format!("alice_snap_ha_{}", std::process::id()));
        let active_dir = base.join("active");
        let standby_dir = base.join("standby");
        let _ = std::fs::remove_dir_all(&base);

        let active_db = Arc::new(Database::open(&active_dir).unwrap());
        let active_srv = AliceServer::new(Arc::clone(&active_db));
        let active_addr = active_srv.start_listener("127.0.0.1:0").unwrap();

        let mut client = AliceClient::connect(active_addr).unwrap();
        client
            .call(Request::DefineSegment(SegmentSchema {
                node_type: 1,
                name: "Item".into(),
                parent_type: None,
                columns: vec![
                    ColumnDef::new("key", DataType::FixedString(16), false).with_index(true),
                ],
            }))
            .unwrap();

        let root_id = match client
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![Value::String("ROOT-KEEP".into())],
            })
            .unwrap()
        {
            Response::OkNodeId(id) => id,
            other => panic!("Unexpected: {:?}", other),
        };

        let temp_id = match client
            .call(Request::Insert {
                parent_id: 0,
                node_type: 1,
                values: vec![Value::String("TEMP-0".into())],
            })
            .unwrap()
        {
            Response::OkNodeId(id) => id,
            other => panic!("Unexpected: {:?}", other),
        };

        for i in 1..=10_050 {
            active_db
                .update(temp_id, &[Value::String(format!("TEMP-{i}"))])
                .unwrap();
        }
        client.call(Request::Delete { node_id: temp_id }).unwrap();

        match client.call(Request::Vacuum).unwrap() {
            Response::OkVacuum(stats) => assert_eq!(stats.active_nodes, 1),
            other => panic!("Vacuum failed: {:?}", other),
        }

        match client
            .call(Request::FindByIndex {
                node_type: 1,
                col_name: "key".into(),
                value: Value::String("ROOT-KEEP".into()),
            })
            .unwrap()
        {
            Response::OkNodeIds(ids) => assert_eq!(ids, vec![root_id]),
            other => panic!("FindByIndex failed: {:?}", other),
        }

        match client
            .call(Request::WalkSubtree {
                root_node_id: root_id,
                max_depth: None,
            })
            .unwrap()
        {
            Response::OkNodeViews(views) => assert_eq!(views.len(), 1),
            other => panic!("WalkSubtree failed: {:?}", other),
        }

        let standby_db = Arc::new(Database::open(&standby_dir).unwrap());
        let standby_srv = AliceServer::new(Arc::clone(&standby_db));
        standby_srv.start_standby_replication(active_addr.to_string(), Duration::from_millis(10));

        thread::sleep(Duration::from_millis(120));

        assert_eq!(
            standby_db
                .find_by_index(1, "key", &Value::String("ROOT-KEEP".into()))
                .unwrap(),
            vec![root_id]
        );

        active_srv.shutdown();
        standby_srv.shutdown();
        let _ = std::fs::remove_dir_all(&base);
    }
}
