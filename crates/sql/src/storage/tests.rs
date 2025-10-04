//! Comprehensive tests for the storage_new module

#[cfg(test)]
mod integration_tests {
    use crate::error::Result;
    use crate::storage::{Storage, StorageConfig};
    use crate::stream::transaction::TransactionContext;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table as TableSchema};
    use crate::types::value::Value;
    use proven_hlc::{HlcTimestamp, NodeId};
    use std::path::Path;

    fn create_test_storage() -> Storage {
        // Use a unique path for each test run to avoid conflicts
        let test_id = uuid::Uuid::new_v4().to_string();
        let path = Path::new("/tmp").join(format!("test_storage_new_{}", test_id));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();

        let config = StorageConfig::for_testing();
        Storage::open_at_path(&path, config).unwrap()
    }

    fn create_test_tx_ctx(txn_id: HlcTimestamp, log_index: u64) -> TransactionContext {
        let mut ctx = TransactionContext::new(txn_id);
        ctx.log_index = log_index;
        ctx
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

    fn create_txn_id(ts: u64) -> HlcTimestamp {
        HlcTimestamp::new(ts, 0, NodeId::new(1))
    }

    #[test]
    fn test_table_operations() {
        let mut storage = create_test_storage();

        // Create table
        let schema = create_test_schema();
        storage
            .create_table("users".to_string(), schema.clone())
            .unwrap();

        // Verify table exists
        let tables = storage.get_schemas().keys().cloned().collect::<Vec<_>>();
        assert!(tables.contains(&"users".to_string()));

        // Get schema
        let retrieved_schema = storage.get_schemas().get("users").unwrap().clone();
        assert_eq!(retrieved_schema.name, schema.name);
        assert_eq!(retrieved_schema.columns.len(), schema.columns.len());

        // Drop table
        storage.drop_table("users").unwrap();
        let tables = storage.get_schemas().keys().cloned().collect::<Vec<_>>();
        assert!(!tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_insert_and_read() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_txn_id(100);
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);

        // Insert row
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_ids = storage
            .insert_batch(&mut tx1_ctx, "users", vec![values.clone()])
            .unwrap();

        let row_id = row_ids.first().unwrap();

        // Read uncommitted (should see own writes)
        let row = storage.read(tx1, "users", *row_id).unwrap();
        assert!(row.is_some());
        let row = row.unwrap();
        assert_eq!(row.values, values);

        // Another transaction shouldn't see uncommitted
        // let tx2 = create_txn_id(200);
        let tx2 = create_txn_id(2);
        let row = storage.read(tx2, "users", *row_id).unwrap();
        assert!(row.is_none());

        // Commit tx1
        storage.commit_transaction(tx1, 1).unwrap();

        // Now tx2 should see it
        let row = storage.read(tx2, "users", *row_id).unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().values, values);
    }

    #[test]
    fn test_update() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_txn_id(100);
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);

        // Insert and commit
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_ids = storage
            .insert_batch(&mut tx1_ctx, "users", vec![values.clone()])
            .unwrap();
        let row_id = row_ids.first().unwrap();
        storage.commit_transaction(tx1, 1).unwrap();

        // Update in new transaction
        // let tx2 = create_txn_id(200);
        let tx2 = create_txn_id(2);
        let mut tx2_ctx = create_test_tx_ctx(tx2, 0);

        let new_values = vec![
            Value::I64(1),
            Value::Str("Alice Smith".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(31),
        ];
        storage
            .update(&mut tx2_ctx, "users", *row_id, new_values.clone())
            .unwrap();

        // tx2 sees new value
        let row = storage.read(tx2, "users", *row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice Smith".to_string()));
        assert_eq!(row.values[3], Value::I64(31));

        // Other transactions still see old value
        // let tx3 = create_txn_id(300);
        let tx3 = create_txn_id(3);
        let row = storage.read(tx3, "users", *row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice".to_string()));
        assert_eq!(row.values[3], Value::I64(30));

        // After commit, new value is visible
        storage.commit_transaction(tx2, 1).unwrap();
        let row = storage.read(tx3, "users", *row_id).unwrap().unwrap();
        assert_eq!(row.values[1], Value::Str("Alice Smith".to_string()));
        assert_eq!(row.values[3], Value::I64(31));
    }

    #[test]
    fn test_delete() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_txn_id(100);
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);

        // Insert and commit
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_ids = storage
            .insert_batch(&mut tx1_ctx, "users", vec![values.clone()])
            .unwrap();
        let row_id = row_ids.first().unwrap();
        storage.commit_transaction(tx1, 1).unwrap();

        // Delete in new transaction
        // let tx2 = create_txn_id(200);
        let tx2 = create_txn_id(2);
        let mut tx2_ctx = create_test_tx_ctx(tx2, 0);
        storage.delete(&mut tx2_ctx, "users", *row_id).unwrap();

        // tx2 doesn't see deleted row
        let row = storage.read(tx2, "users", *row_id).unwrap();
        assert!(row.is_none());

        // Other transactions still see it
        // let tx3 = create_txn_id(300);
        let tx3 = create_txn_id(3);
        let row = storage.read(tx3, "users", *row_id).unwrap();
        assert!(row.is_some());

        // After commit, row is deleted
        storage.commit_transaction(tx2, 1).unwrap();
        let row = storage.read(tx3, "users", *row_id).unwrap();
        assert!(row.is_none());
    }

    #[test]
    fn test_scan() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_txn_id(100);
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);

        // Insert multiple rows
        for i in 1..=5 {
            let values = vec![
                Value::I64(i),
                Value::Str(format!("User{}", i)),
                Value::Str(format!("user{}@example.com", i)),
                Value::I64(20 + i),
            ];
            storage
                .insert_batch(&mut tx1_ctx, "users", vec![values.clone()])
                .unwrap();
        }

        // Scan should see all uncommitted rows
        let rows = storage
            .iter(tx1, "users")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 5);

        // Other transaction sees nothing
        // let tx2 = create_txn_id(200);
        let tx2 = create_txn_id(2);
        let rows = storage
            .iter(tx2, "users")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 0);

        // After commit, all visible
        storage.commit_transaction(tx1, 1).unwrap();
        let rows = storage
            .iter(tx2, "users")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn test_abort_transaction() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // let tx1 = create_txn_id(100);
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);

        // Insert row
        let values = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        let row_ids = storage
            .insert_batch(&mut tx1_ctx, "users", vec![values.clone()])
            .unwrap();
        let row_id = row_ids.first().unwrap();

        // Abort transaction
        storage.abort_transaction(tx1, 1).unwrap();

        // Row should not be visible even to same transaction
        // let tx2 = create_txn_id(200);
        let tx2 = create_txn_id(2);
        let row = storage.read(tx2, "users", *row_id).unwrap();
        assert!(row.is_none());

        let rows = storage
            .iter(tx2, "users")
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_persistence() {
        let path = Path::new("/tmp/test_storage_persist");
        let _ = std::fs::remove_dir_all(path);
        std::fs::create_dir_all(path).unwrap();

        // Create storage and add data
        {
            let mut config = StorageConfig::for_testing();
            // For persistence test, we need to actually sync to disk
            config.persist_mode = fjall::PersistMode::SyncData;
            let mut storage = Storage::open_at_path(path, config).unwrap();
            storage
                .create_table("users".to_string(), create_test_schema())
                .unwrap();

            let tx = create_txn_id(100);
            let mut tx_ctx = create_test_tx_ctx(tx, 0);

            let values = vec![
                Value::I64(1),
                Value::Str("Alice".to_string()),
                Value::Str("alice@example.com".to_string()),
                Value::I64(30),
            ];
            storage
                .insert_batch(&mut tx_ctx, "users", vec![values.clone()])
                .unwrap();
            storage.commit_transaction(tx, 1).unwrap();
        }

        // Reopen and verify data persisted
        {
            let mut config = StorageConfig::for_testing();
            config.persist_mode = fjall::PersistMode::SyncData;
            let storage = Storage::open_at_path(path, config).unwrap();

            // Check if tables are loaded
            let tables = storage.get_schemas().keys().cloned().collect::<Vec<_>>();
            assert!(!tables.is_empty(), "No tables found after restart");
            assert!(
                tables.contains(&"users".to_string()),
                "users table not found"
            );

            // Use a later transaction ID to read the committed data
            let tx = create_txn_id(200);

            let rows = storage
                .iter(tx, "users")
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_eq!(rows.len(), 1, "Expected 1 row, found {}", rows.len());
            assert_eq!(rows[0].values[1], Value::Str("Alice".to_string()));
        }
    }

    #[test]
    fn test_concurrent_transactions() {
        let mut storage = create_test_storage();
        storage
            .create_table("users".to_string(), create_test_schema())
            .unwrap();

        // Start multiple transactions
        // let tx1 = create_txn_id(100);
        // let tx2 = create_txn_id(200);
        // let tx3 = create_txn_id(300);

        let tx1 = create_txn_id(1);
        let tx2 = create_txn_id(2);
        let tx3 = create_txn_id(3);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);
        let mut tx2_ctx = create_test_tx_ctx(tx2, 0);

        // Each inserts different rows
        let values1 = vec![
            Value::I64(1),
            Value::Str("Alice".to_string()),
            Value::Str("alice@example.com".to_string()),
            Value::I64(30),
        ];
        storage
            .insert_batch(&mut tx1_ctx, "users", vec![values1.clone()])
            .unwrap();

        let values2 = vec![
            Value::I64(2),
            Value::Str("Bob".to_string()),
            Value::Str("bob@example.com".to_string()),
            Value::I64(25),
        ];
        storage
            .insert_batch(&mut tx2_ctx, "users", vec![values2.clone()])
            .unwrap();

        // tx1 and tx2 only see their own writes
        assert_eq!(storage.iter(tx1, "users").unwrap().count(), 1);
        assert_eq!(storage.iter(tx2, "users").unwrap().count(), 1);
        assert_eq!(storage.iter(tx3, "users").unwrap().count(), 0);

        // Commit tx1
        storage.commit_transaction(tx1, 1).unwrap();

        // tx3 now sees tx1's data but not tx2's
        assert_eq!(storage.iter(tx3, "users").unwrap().count(), 1);

        // Commit tx2
        storage.commit_transaction(tx2, 1).unwrap();

        // tx3 sees all data
        assert_eq!(storage.iter(tx3, "users").unwrap().count(), 2);
    }

    #[test]
    fn test_index_ddl_operations() {
        let mut storage = create_test_storage();

        // Create table
        let schema = create_test_schema();
        storage.create_table("users".to_string(), schema).unwrap();

        // Insert some data
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);
        storage
            .insert_batch(
                &mut tx1_ctx,
                "users",
                vec![vec![
                    Value::I64(1),
                    Value::Str("Alice".to_string()),
                    Value::Str("alice@example.com".to_string()),
                    Value::I64(30),
                ]],
            )
            .unwrap();
        storage
            .insert_batch(
                &mut tx1_ctx,
                "users",
                vec![vec![
                    Value::I64(2),
                    Value::Str("Bob".to_string()),
                    Value::Str("bob@example.com".to_string()),
                    Value::I64(25),
                ]],
            )
            .unwrap();
        storage.commit_transaction(tx1, 1).unwrap();

        // Create non-unique index on age
        let idx_tx1 = create_txn_id(10);
        storage
            .create_index(
                idx_tx1,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        storage.commit_transaction(idx_tx1, 1).unwrap();

        // Create unique index on email
        let idx_tx2 = create_txn_id(11);
        storage
            .create_index(
                idx_tx2,
                "idx_email".to_string(),
                "users".to_string(),
                vec!["email".to_string()],
                true,
            )
            .unwrap();
        storage.commit_transaction(idx_tx2, 1).unwrap();

        // Verify index works with lookup
        let tx2 = create_txn_id(20);
        let results: Vec<_> = storage
            .index_lookup_rows_streaming("idx_age", vec![Value::I64(30)], tx2)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[1], Value::Str("Alice".to_string()));

        // Drop index
        storage.drop_index("idx_age").unwrap();

        // Verify index is gone (should get error)
        let result = storage.index_lookup_rows_streaming("idx_age", vec![Value::I64(30)], tx2);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_lookup_operations() {
        let mut storage = create_test_storage();

        // Create table
        let schema = create_test_schema();
        storage.create_table("users".to_string(), schema).unwrap();

        // Insert test data
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);
        for i in 1..=5 {
            storage
                .insert_batch(
                    &mut tx1_ctx,
                    "users",
                    vec![vec![
                        Value::I64(i),
                        Value::Str(format!("User{}", i)),
                        Value::Str(format!("user{}@example.com", i)),
                        Value::I64(20 + i),
                    ]],
                )
                .unwrap();
        }
        storage.commit_transaction(tx1, 1).unwrap();

        // Create index on age
        let idx_tx = create_txn_id(15);
        storage
            .create_index(
                idx_tx,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        storage.commit_transaction(idx_tx, 1).unwrap();

        let tx2 = create_txn_id(20);

        // Test exact lookup
        let results: Vec<_> = storage
            .index_lookup_rows_streaming("idx_age", vec![Value::I64(23)], tx2)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[1], Value::Str("User3".to_string()));

        // Test range scan - ages 22 to 24 (using streaming API)
        let results: Vec<_> = storage
            .index_range_lookup_rows_streaming(
                "idx_age",
                Some(vec![Value::I64(22)]),
                Some(vec![Value::I64(24)]),
                false,
                tx2,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 3); // Users 2, 3, and 4

        // Test open-ended range scan - ages >= 24
        let results: Vec<_> = storage
            .index_range_lookup_rows_streaming(
                "idx_age",
                Some(vec![Value::I64(24)]),
                None,
                false,
                tx2,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 2); // Users 4 and 5

        // Test open-ended range scan - ages <= 22
        let results: Vec<_> = storage
            .index_range_lookup_rows_streaming(
                "idx_age",
                None,
                Some(vec![Value::I64(22)]),
                false,
                tx2,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 2); // Users 1 and 2
    }

    #[test]
    fn test_index_performance_streaming() {
        let mut storage = create_test_storage();

        // Create table
        let schema = create_test_schema();
        storage.create_table("users".to_string(), schema).unwrap();

        // Insert a large number of rows
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);
        for i in 1..=1000 {
            storage
                .insert_batch(
                    &mut tx1_ctx,
                    "users",
                    vec![vec![
                        Value::I64(i),
                        Value::Str(format!("User{}", i)),
                        Value::Str(format!("user{}@example.com", i)),
                        Value::I64(20 + (i % 50)), // Ages from 20-69
                    ]],
                )
                .unwrap();
        }
        storage.commit_transaction(tx1, 1).unwrap();

        // Create index on age
        let idx_tx = create_txn_id(25);
        storage
            .create_index(
                idx_tx,
                "idx_age".to_string(),
                "users".to_string(),
                vec!["age".to_string()],
                false,
            )
            .unwrap();
        storage.commit_transaction(idx_tx, 1).unwrap();

        // Test streaming iterator (doesn't collect all results into memory)
        // Now using MVCC-aware streaming API
        let mut count = 0;
        let scan_tx = create_txn_id(30);
        let iter = storage
            .index_range_lookup_rows_streaming(
                "idx_age",
                Some(vec![Value::I64(30)]),
                Some(vec![Value::I64(40)]),
                false,
                scan_tx,
            )
            .unwrap();

        for row_result in iter {
            row_result.unwrap();
            count += 1;
            if count >= 10 {
                break; // Early termination - a key benefit of streaming
            }
        }

        // With streaming, we only processed 10 items even though there might be hundreds
        assert_eq!(count, 10);

        // Verify streaming can be collected into Vec if needed
        let tx2 = create_txn_id(30);
        let all_results: Vec<_> = storage
            .index_range_lookup_rows_streaming(
                "idx_age",
                Some(vec![Value::I64(30)]),
                Some(vec![Value::I64(40)]),
                false,
                tx2,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        // This collected all matching rows (but streamed during iteration)
        assert!(all_results.len() >= 200); // ~220 rows should match (11 ages * 20 rows per age)
    }

    #[test]
    fn test_composite_index() {
        let mut storage = create_test_storage();

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

        storage.create_table("orders".to_string(), schema).unwrap();

        // Insert test data
        let tx1 = create_txn_id(1);
        let mut tx1_ctx = create_test_tx_ctx(tx1, 0);
        storage
            .insert_batch(
                &mut tx1_ctx,
                "orders",
                vec![vec![
                    Value::I64(1),
                    Value::I64(100),
                    Value::I64(1),
                    Value::I64(2),
                    Value::F64(19.99),
                ]],
            )
            .unwrap();
        storage
            .insert_batch(
                &mut tx1_ctx,
                "orders",
                vec![vec![
                    Value::I64(2),
                    Value::I64(100),
                    Value::I64(2),
                    Value::I64(1),
                    Value::F64(29.99),
                ]],
            )
            .unwrap();
        storage
            .insert_batch(
                &mut tx1_ctx,
                "orders",
                vec![vec![
                    Value::I64(3),
                    Value::I64(101),
                    Value::I64(1),
                    Value::I64(3),
                    Value::F64(19.99),
                ]],
            )
            .unwrap();
        storage.commit_transaction(tx1, 1).unwrap();

        // Create composite index on (customer_id, product_id)
        let idx_tx = create_txn_id(30);
        storage
            .create_index(
                idx_tx,
                "idx_customer_product".to_string(),
                "orders".to_string(),
                vec!["customer_id".to_string(), "product_id".to_string()],
                false,
            )
            .unwrap();
        storage.commit_transaction(idx_tx, 1).unwrap();

        // Lookup by composite key
        let tx2 = create_txn_id(40);
        let results: Vec<_> = storage
            .index_lookup_rows_streaming(
                "idx_customer_product",
                vec![Value::I64(100), Value::I64(1)],
                tx2,
            )
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], Value::I64(1));
    }
}
