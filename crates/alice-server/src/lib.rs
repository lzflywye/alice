pub mod protocol;
pub mod server;

#[cfg(test)]
mod tests {
    use crate::protocol::{Request, Response};
    use crate::server::{AliceClient, AliceServer};
    use alice_core::db::Database;
    use alice_core::schema::{ColumnDef, DataType, SegmentSchema, Value};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_active_standby_replication_and_failover() {
        let base = std::env::temp_dir().join(format!("alice_ha_{}", std::process::id()));
        let active_dir = base.join("active");
        let standby_dir = base.join("standby");
        let _ = std::fs::remove_dir_all(&base);

        // 1. Start Active Server
        let active_db = Arc::new(Database::open(&active_dir).unwrap());
        let active_srv = AliceServer::new(Arc::clone(&active_db));
        let active_addr = active_srv.start_listener("127.0.0.1:0").unwrap();

        // 2. Start Standby Server and attach to Active
        let standby_db = Arc::new(Database::open(&standby_dir).unwrap());
        let standby_srv = AliceServer::new(Arc::clone(&standby_db));
        let standby_addr = standby_srv.start_listener("127.0.0.1:0").unwrap();
        standby_srv.start_standby_replication(active_addr.to_string(), Duration::from_millis(10));

        // 3. Client writes to Active
        let mut client_active = AliceClient::connect(active_addr).unwrap();
        let resp = client_active
            .call(Request::DefineSegment(SegmentSchema {
                node_type: 1,
                name: "Dept".into(),
                parent_type: None,
                columns: vec![
                    ColumnDef {
                        name: "code".into(),
                        data_type: DataType::FixedString(8),
                        nullable: false,
                    },
                    ColumnDef {
                        name: "desc".into(),
                        data_type: DataType::VarString,
                        nullable: false,
                    },
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

        // Wait briefly for replication poll
        thread::sleep(Duration::from_millis(80));

        // 4. Verify Standby has replicated both schema & data, and rejects writes
        let mut client_standby = AliceClient::connect(standby_addr).unwrap();
        match client_standby.call(Request::Get { node_id }).unwrap() {
            Response::OkRecord { node_type, values } => {
                assert_eq!(node_type, 1);
                assert_eq!(values[0], Value::String("ENG-01".into()));
                assert_eq!(values[1], Value::String("Core Engineering".into()));
            }
            other => panic!("Standby read failed: {:?}", other),
        }

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

        // 5. Simulate Active crash & Promote Standby to Active (Failover)
        active_srv.shutdown();
        assert!(matches!(
            client_standby.call(Request::Promote).unwrap(),
            Response::OkEmpty
        ));

        // Promoted Standby now accepts writes!
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
}
