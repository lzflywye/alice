use hierarchical::*;
use std::fs;

fn setup_demo_schemas(db: &Database) {
    db.define_segment(SegmentSchema {
        node_type: 1,
        name: "Dept".into(),
        parent_type: None,
        columns: vec![
            ColumnDef::new("code", DataType::FixedString(16), false).with_index(true),
            ColumnDef::new("name", DataType::VarString, false),
        ],
    })
    .unwrap();

    db.define_segment(SegmentSchema {
        node_type: 2,
        name: "Emp".into(),
        parent_type: Some(1),
        columns: vec![
            ColumnDef::new("emp_no", DataType::I64, false).with_index(true),
            ColumnDef::new("bio", DataType::VarString, true),
            ColumnDef::new("salary", DataType::Decimal, false),
        ],
    })
    .unwrap();
}

#[test]
fn test_hierarchical_crud_heap_index_and_vacuum() {
    let dir = std::env::temp_dir().join(format!("hier_crud_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        let db = Database::open(&dir).unwrap();
        setup_demo_schemas(&db);

        // Root insert
        let dept_id = db
            .insert(
                0,
                1,
                &[
                    Value::String("ENG".into()),
                    Value::String("Engineering".into()),
                ],
            )
            .unwrap();

        // Unique constraint check
        assert!(matches!(
            db.insert(
                0,
                1,
                &[Value::String("ENG".into()), Value::String("Dup".into())]
            ),
            Err(common::DbError::UniqueConstraintViolation { .. })
        ));

        // Invalid parent type check (Emp cannot be placed under root 0)
        assert!(matches!(
            db.insert(
                0,
                2,
                &[
                    Value::I64(100),
                    Value::Null,
                    Value::Decimal(Decimal::new(500_000, 2)),
                ]
            ),
            Err(common::DbError::InvalidParentType { .. })
        ));

        // Heap overflow payload (> 204 bytes)
        let long_bio = "x".repeat(400);
        let emp1 = db
            .insert(
                dept_id,
                2,
                &[
                    Value::I64(1001),
                    Value::String(long_bio.clone()),
                    Value::Decimal(Decimal::new(850_000, 2)),
                ],
            )
            .unwrap();

        let emp2 = db
            .insert(
                dept_id,
                2,
                &[
                    Value::I64(1002),
                    Value::Null,
                    Value::Decimal(Decimal::new(920_000, 2)),
                ],
            )
            .unwrap();

        // Secondary index lookup
        let found = db.find_by_index(2, "emp_no", &Value::I64(1001)).unwrap();
        assert_eq!(found, vec![emp1]);

        // Walk subtree
        let tree = db.walk_subtree(dept_id, None).unwrap();
        assert_eq!(tree.len(), 3);
        assert_eq!(tree[0].depth, 0);
        assert_eq!(tree[1].depth, 1);

        // Delete leaf and run vacuum
        db.delete(emp2).unwrap();
        let stats = db.vacuum().unwrap();
        assert_eq!(stats.live_nodes, 2);
        assert_eq!(stats.reclaimed_slots, 1);

        let (_, vals) = db.get(emp1).unwrap();
        assert_eq!(vals[1], Value::String(long_bio));
    }

    // Reopen and verify persistence + index rebuild
    {
        let db = Database::open(&dir).unwrap();
        let found = db
            .find_by_index(1, "code", &Value::String("ENG".into()))
            .unwrap();
        assert_eq!(found.len(), 1);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_hierarchical_with_tiny_buffer_pool_eviction_and_wal_recovery() {
    let dir = std::env::temp_dir().join(format!("hier_bp_evict_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);

    {
        // Only 2 pages (32 slots) allowed in RAM at once
        let db = Database::open_with_options(&dir, 65536, 2).unwrap();
        setup_demo_schemas(&db);

        let dept_id = db
            .insert(
                0,
                1,
                &[
                    Value::String("OPS".into()),
                    Value::String("Operations".into()),
                ],
            )
            .unwrap();

        // Insert 80 child nodes across 5+ pages -> forces multiple LRU page evictions
        for i in 0..80i64 {
            db.insert(
                dept_id,
                2,
                &[
                    Value::I64(2000 + i),
                    Value::String(format!("bio_{i}")),
                    Value::Decimal(Decimal::new((600_000 + i).into(), 2)),
                ],
            )
            .unwrap();
        }

        let stats = db.buffer_pool_stats().unwrap();
        assert_eq!(stats.cached_pages, 2);
        assert!(stats.evictions >= 3);

        let tree = db.walk_subtree(dept_id, None).unwrap();
        assert_eq!(tree.len(), 81); // 1 Dept + 80 Emps
    }

    // Reopen without flushing explicitly beforehand -> WAL must replay cleanly into Buffer Pool
    {
        let db = Database::open_with_options(&dir, 65536, 4).unwrap();
        let found = db.find_by_index(2, "emp_no", &Value::I64(2079)).unwrap();
        assert_eq!(found.len(), 1);
    }

    let _ = fs::remove_dir_all(&dir);
}
