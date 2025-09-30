//! Comprehensive tests for the storage_new module

#[cfg(test)]
mod integration_tests {
    use crate::storage::{Storage, StorageConfig};
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table as TableSchema};
    use crate::types::value::Value;
    use proven_hlc::{HlcTimestamp, NodeId};
    use std::path::Path;

    fn create_test_engine() -> Storage {
        // Use a unique path for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let path = Path::new("/tmp").join(format!("test_storage_new_{}", test_id));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();

        let config = StorageConfig::for_testing();
        Storage::open_at_path(&path, config).unwrap()
    }

    fn create_test_schema() -> TableSchema {
        TableSchema::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("name".to_string(), DataType::Str),
                Column::new("email".to_string(), DataType::Str).unique(),
                Column::new("age".to_string(), DataType::I64),
            ],
        )
        .unwrap()
    }

    fn create_tx_id(ts: u64) -> HlcTimestamp {
        HlcTimestamp::new(ts, 0, NodeId::new(1))
    }

    #[test]
    fn test_table_operations() {
        let engine = create_test_engine();

        // Create table
        let schema = create_test_schema();
        engine
            .create_table("users".to_string(), schema.clone())
            .unwrap();

        // Verify table exists
        let tables = engine.list_tables();
        assert!(tables.contains(&"users".to_string()));

        // Get schema
        let retrieved_schema = engine.get_table_schema("users").unwrap();
        assert_eq!(retrieved_schema.name, schema.name);
        assert_eq!(retrieved_schema.columns.len(), schema.columns.len());

        // Drop table
        engine.drop_table("users").unwrap();
        let tables = engine.list_tables();
        assert!(!tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_insert_and_read() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_tx_id(100);
        let tx1 = create_tx_id(1);

        // Insert row
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_id = engine.insert(tx1, "users", values.clone()).unwrap();

        // Read uncommitted (should see own writes)
        let row = engine.read(tx1, "users", row_id).unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row.values, values);

        // Another transaction shouldn't see uncommitted
        // let tx2 = create_tx_id(200);
        let tx2 = create_tx_id(2);
        let row = engine.read(tx2, "users", row_id).unwrap();
        assert!(row.is_none());

        // Commit tx1
        engine.commit_transaction(tx1).unwrap();

        // Now tx2 should see it
        let row = engine.read(tx2, "users", row_id).unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().values, values);
    }

    #[test]
    fn test_update() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_tx_id(100);
        let tx1 = create_tx_id(1);

        // Insert and commit
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_id = engine.insert(tx1, "users", values).unwrap();
        engine.commit_transaction(tx1).unwrap();

        // Update in new transaction
        // let tx2 = create_tx_id(200);
        let tx2 = create_tx_id(2);

        let new_values = vec![
            Value::I64(1),
            Value::Str("Alice Smith".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(31),
        ];
        engine
            .update(tx2, "users", row_id, new_values.clone())
            .unwrap();

        // tx2 sees new value
        let row = engine.read(tx2, "users", row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice Smith".to_string()));
        assert_eq!(row.values[3], Value::I64(31));

        // Other transactions still see old value
        // let tx3 = create_tx_id(300);
        let tx3 = create_tx_id(3);
        let row = engine.read(tx3, "users", row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice".to_string()));
        assert_eq!(row.values[3], Value::I64(30));

        // After commit, new value is visible
        engine.commit_transaction(tx2).unwrap();
        let row = engine.read(tx3, "users", row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice Smith".to_string()));
        assert_eq!(row.values[3], Value::I64(31));
    }

    #[test]
    fn test_delete() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_tx_id(100);
        let tx1 = create_tx_id(1);

        // Insert and commit
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_id = engine.insert(tx1, "users", values).unwrap();
        engine.commit_transaction(tx1).unwrap();

        // Delete in new transaction
        // let tx2 = create_tx_id(200);
        let tx2 = create_tx_id(2);
        engine.delete(tx2, "users", row_id).unwrap();

        // tx2 doesn't see deleted row
        let row = engine.read(tx2, "users", row_id).unwrap();
        assert!(row.is_none());

        // Other transactions still see it
        // let tx3 = create_tx_id(300);
        let tx3 = create_tx_id(3);
        let row = engine.read(tx3, "users", row_id).unwrap();
        assert!(row.is_some());

        // After commit, row is deleted
        engine.commit_transaction(tx2).unwrap();
        let row = engine.read(tx3, "users", row_id).unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn test_scan() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_tx_id(100);
        let tx1 = create_tx_id(1);

        // Insert multiple rows
        for i in 1..=5 {
            let values = vec![
                Value::I64(i),
                Value::Str(format!("User{}", i)),
                Value::Str(format!("user{}@example.com", i)),
                Value::I64(20 + i),
            ];
            engine.insert(tx1, "users", values).unwrap();
        }

        // Scan should see all uncommitted rows
        let rows = engine.scan(tx1, "users").unwrap();
        assert_eq!(rows.len(), 5);

        // Other transaction sees nothing
        // let tx2 = create_tx_id(200);
        let tx2 = create_tx_id(2);
        let rows = engine.scan(tx2, "users").unwrap();
        assert_eq!(rows.len(), 0);

        // After commit, all visible
        engine.commit_transaction(tx1).unwrap();
        let rows = engine.scan(tx2, "users").unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_abort_transaction() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_tx_id(100);
        let tx1 = create_tx_id(1);

        // Insert row
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_id = engine.insert(tx1, "users", values).unwrap();

        // Abort transaction
        engine.abort_transaction(tx1).unwrap();

        // Row should not be visible even to same transaction
        // let tx2 = create_tx_id(200);
        let tx2 = create_tx_id(2);
        let row = engine.read(tx2, "users", row_id).unwrap();
        assert!(row.is_none());

        let rows = engine.scan(tx2, "users").unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_persistence() {
        let path = Path::new("/tmp/test_storage_persist");
        let _ = std::fs::remove_dir_all(path);
        std::fs::create_dir_all(path).unwrap();

        // Create engine and add data
        {
            let mut config = StorageConfig::for_testing();
            // For persistence test, we need to actually sync to disk
            config.persist_mode = fjall::PersistMode::SyncData;
            let engine = Storage::open_at_path(path, config).unwrap();
            engine
                .create_table("users".to_string(), create_test_schema())
                .unwrap();

            let tx = create_tx_id(100);

            let values = vec![
                Value::I64(1),
                Value::Str("Alice".to_string()),
                Value::Str("alice@example.com".to_string()),
                Value::I64(30),
            ];
            engine.insert(tx, "users", values).unwrap();
            engine.commit_transaction(tx).unwrap();
        }

        // Reopen and verify data persisted
        {
            let mut config = StorageConfig::for_testing();
            config.persist_mode = fjall::PersistMode::SyncData;
            let engine = Storage::open_at_path(path, config).unwrap();

            // Check if tables are loaded
            let tables = engine.list_tables();
            assert!(!tables.is_empty(), "No tables found after restart");
            assert!(
                tables.contains(&"users".to_string()),
                "users table not found"
            );

            // Use a later transaction ID to read the committed data
            let tx = create_tx_id(200);

            let rows = engine.scan(tx, "users").unwrap();
            assert_eq!(rows.len(), 1, "Expected 1 row, found {}", rows.len());
            assert_eq!(rows[0].values[1], Value::Str("Alice".to_string()));
        }
    }

    #[test]
    fn test_concurrent_transactions() {
        let engine = create_test_engine();
        engine
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // Start multiple transactions
        // let tx1 = create_tx_id(100);
        // let tx2 = create_tx_id(200);
        // let tx3 = create_tx_id(300);

        let tx1 = create_tx_id(1);
        let tx2 = create_tx_id(2);
        let tx3 = create_tx_id(3);

        // Each inserts different rows
        let values1 = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        engine.insert(tx1, "users", values1).unwrap();

        let values2 = vec![
            Value::I64(2),
            Value::Str("Bob".to_string()),
            Value::Str("bob@example.com".to_string()),
            Value::I64(25),
        ];
        engine.insert(tx2, "users", values2).unwrap();

        // tx1 and tx2 only see their own writes
        assert_eq!(engine.scan(tx1, "users").unwrap().len(), 1);
        assert_eq!(engine.scan(tx2, "users").unwrap().len(), 1);
        assert_eq!(engine.scan(tx3, "users").unwrap().len(), 0);

        // Commit tx1
        engine.commit_transaction(tx1).unwrap();

        // tx3 now sees tx1's data but not tx2's
        assert_eq!(engine.scan(tx3, "users").unwrap().len(), 1);

        // Commit tx2
        engine.commit_transaction(tx2).unwrap();

        // tx3 sees all data
        assert_eq!(engine.scan(tx3, "users").unwrap().len(), 2);
    }

    #[test]
    fn test_index_ddl_operations() {
        let engine = create_test_engine();

        // Create table
        let schema = create_test_schema();
        engine.create_table("users".to_string(), schema).unwrap();

        // Insert some data
        let tx1 = create_tx_id(1);
        engine
            .insert(
                tx1,
                "users",
                vec![
                    Value::I64(1),
                    Value::Str("Alice".to_string()),
                    Value::Str("alice@example.com".to_string()),
                    Value::I64(30),
                ],
            )
            .unwrap();
        engine
            .insert(
                tx1,
                "users",
                vec![
                    Value::I64(2),
                    Value::Str("Bob".to_string()),
                    Value::Str("bob@example.com".to_string()),
                    Value::I64(25),
                ],
            )
            .unwrap();
        engine.commit_transaction(tx1).unwrap();

        // Create non-unique index on age
        let idx_tx1 = create_tx_id(10);
        engine
            .create_index(
                idx_tx1,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        engine.commit_transaction(idx_tx1).unwrap();

        // Create unique index on email
        let idx_tx2 = create_tx_id(11);
        engine
            .create_index(
                idx_tx2,
                "idx_email".to_string(),
                "users".to_string(),
                vec!["email".to_string()],
                true,
            )
            .unwrap();
        engine.commit_transaction(idx_tx2).unwrap();

        // Verify index works with lookup
        let tx2 = create_tx_id(20);
        let results = engine
            .index_lookup("idx_age", vec![Value::I64(30)], tx2)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[1], Value::Str("Alice".to_string()));

        // Test unique constraint check
        assert!(
            engine
                .check_unique_constraint(
                    "idx_email",
                    &[Value::Str("alice@example.com".to_string())],
                    tx2,
                )
                .unwrap()
        );

        // Drop index
        engine.drop_index("idx_age").unwrap();

        // Verify index is gone (should get error)
        let result = engine.index_lookup("idx_age", vec![Value::I64(30)], tx2);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_lookup_operations() {
        let engine = create_test_engine();

        // Create table
        let schema = create_test_schema();
        engine.create_table("users".to_string(), schema).unwrap();

        // Insert test data
        let tx1 = create_tx_id(1);
        for i in 1..=5 {
            engine
                .insert(
                    tx1,
                    "users",
                    vec![
                        Value::I64(i),
                        Value::Str(format!("User{}", i)),
                        Value::Str(format!("user{}@example.com", i)),
                        Value::I64(20 + i),
                    ],
                )
                .unwrap();
        }
        engine.commit_transaction(tx1).unwrap();

        // Create index on age
        let idx_tx = create_tx_id(15);
        engine
            .create_index(
                idx_tx,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        engine.commit_transaction(idx_tx).unwrap();

        let tx2 = create_tx_id(20);

        // Test exact lookup
        let results = engine
            .index_lookup("idx_age", vec![Value::I64(23)], tx2)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[1], Value::Str("User3".to_string()));

        // Test range scan - ages 22 to 24
        let results = engine
            .index_range_scan(
                "idx_age",
                Some(vec![Value::I64(22)]),
                Some(vec![Value::I64(24)]),
                tx2,
            )
            .unwrap();
        assert_eq!(results.len(), 3); // Users 2, 3, and 4

        // Test open-ended range scan - ages >= 24
        let results = engine
            .index_range_scan("idx_age", Some(vec![Value::I64(24)]), None, tx2)
            .unwrap();
        assert_eq!(results.len(), 2); // Users 4 and 5

        // Test open-ended range scan - ages <= 22
        let results = engine
            .index_range_scan("idx_age", None, Some(vec![Value::I64(22)]), tx2)
            .unwrap();
        assert_eq!(results.len(), 2); // Users 1 and 2
    }

    #[test]
    fn test_unique_index_constraints() {
        let engine = create_test_engine();

        // Create table
        let schema = create_test_schema();
        engine.create_table("users".to_string(), schema).unwrap();

        // Create unique index on email
        let idx_tx = create_tx_id(20);
        engine
            .create_index(
                idx_tx,
                "idx_email".to_string(),
                "users".to_string(),
                vec!["email".to_string()],
                true,
            )
            .unwrap();
        engine.commit_transaction(idx_tx).unwrap();

        // Insert first user
        let tx1 = create_tx_id(1);
        engine
            .insert(
                tx1,
                "users",
                vec![
                    Value::I64(1),
                    Value::Str("Alice".to_string()),
                    Value::Str("alice@example.com".to_string()),
                    Value::I64(30),
                ],
            )
            .unwrap();
        engine.commit_transaction(tx1).unwrap();

        // Check that email already exists
        let tx2 = create_tx_id(2);
        assert!(
            engine
                .check_unique_constraint(
                    "idx_email",
                    &[Value::Str("alice@example.com".to_string())],
                    tx2,
                )
                .unwrap()
        );
        engine.abort_transaction(tx2).unwrap();

        // Check that different email doesn't exist
        let tx3 = create_tx_id(3);
        assert!(
            !engine
                .check_unique_constraint(
                    "idx_email",
                    &[Value::Str("bob@example.com".to_string())],
                    tx3
                )
                .unwrap()
        );
        engine.abort_transaction(tx3).unwrap();
    }

    #[test]
    fn test_index_performance_streaming() {
        let engine = create_test_engine();

        // Create table
        let schema = create_test_schema();
        engine.create_table("users".to_string(), schema).unwrap();

        // Insert a large number of rows
        let tx1 = create_tx_id(1);
        for i in 1..=1000 {
            engine
                .insert(
                    tx1,
                    "users",
                    vec![
                        Value::I64(i),
                        Value::Str(format!("User{}", i)),
                        Value::Str(format!("user{}@example.com", i)),
                        Value::I64(20 + (i % 50)), // Ages from 20-69
                    ],
                )
                .unwrap();
        }
        engine.commit_transaction(tx1).unwrap();

        // Create index on age
        let idx_tx = create_tx_id(25);
        engine
            .create_index(
                idx_tx,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        engine.commit_transaction(idx_tx).unwrap();

        // Test streaming iterator (doesn't collect all results into memory)
        let mut count = 0;
        let iter = engine
            .index_range_scan_iter(
                "idx_age",
                Some(vec![Value::I64(30)]),
                Some(vec![Value::I64(40)]),
            )
            .unwrap();

        for row_id_result in iter {
            row_id_result.unwrap();
            count += 1;
            if count >= 10 {
                break; // Early termination - a key benefit of streaming
            }
        }

        // With streaming, we only processed 10 items even though there might be hundreds
        assert_eq!(count, 10);

        // Compare with Vec-based approach that loads everything
        let tx2 = create_tx_id(30);
        let all_results = engine
            .index_range_scan(
                "idx_age",
                Some(vec![Value::I64(30)]),
                Some(vec![Value::I64(40)]),
                tx2,
            )
            .unwrap();

        // This loaded all matching rows into memory
        assert!(all_results.len() >= 200); // ~220 rows should match (11 ages * 20 rows per age)
    }

    #[test]
    fn test_composite_index() {
        let engine = create_test_engine();

        // Create table with more columns
        let schema = TableSchema::new(
            "orders".to_string(),
            vec![
                Column::new("id".to_string(), DataType::I64).primary_key(),
                Column::new("customer_id".to_string(), DataType::I64),
                Column::new("product_id".to_string(), DataType::I64),
                Column::new("quantity".to_string(), DataType::I64),
                Column::new("price".to_string(), DataType::F64),
            ],
        )
        .unwrap();

        engine.create_table("orders".to_string(), schema).unwrap();

        // Insert test data
        let tx1 = create_tx_id(1);
        engine
            .insert(
                tx1,
                "orders",
                vec![
                    Value::I64(1),
                    Value::I64(100),
                    Value::I64(1),
                    Value::I64(2),
                    Value::F64(19.99),
                ],
            )
            .unwrap();
        engine
            .insert(
                tx1,
                "orders",
                vec![
                    Value::I64(2),
                    Value::I64(100),
                    Value::I64(2),
                    Value::I64(1),
                    Value::F64(29.99),
                ],
            )
            .unwrap();
        engine
            .insert(
                tx1,
                "orders",
                vec![
                    Value::I64(3),
                    Value::I64(101),
                    Value::I64(1),
                    Value::I64(3),
                    Value::F64(19.99),
                ],
            )
            .unwrap();
        engine.commit_transaction(tx1).unwrap();

        // Create composite index on (customer_id, product_id)
        let idx_tx = create_tx_id(30);
        engine
            .create_index(
                idx_tx,
                "idx_customer_product".to_string(),
                "orders".to_string(),
                vec!["customer_id".to_string(), "product_id".to_string()],
                false,
            )
            .unwrap();
        engine.commit_transaction(idx_tx).unwrap();

        // Lookup by composite key
        let tx2 = create_tx_id(40);
        let results = engine
            .index_lookup(
                "idx_customer_product",
                vec![Value::I64(100), Value::I64(1)],
                tx2,
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::I64(1));
    }
}
