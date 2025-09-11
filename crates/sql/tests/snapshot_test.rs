//! Tests for SQL engine snapshot functionality

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::engine::SqlTransactionEngine;
use proven_sql::stream::operation::SqlOperation;
use proven_sql::types::value::Value;
use proven_stream::{OperationResult, TransactionEngine};

#[test]
fn test_sql_snapshot_and_restore() {
    // Create engine and set up tables with data
    let mut engine1 = SqlTransactionEngine::new();

    // Create a table
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine1.begin_transaction(txn1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT, email VARCHAR)"
            .to_string(),
        params: None,
    };
    let result = engine1.apply_operation(create_table, txn1);
    assert!(matches!(result, OperationResult::Success(_)));
    engine1.commit(txn1).unwrap();

    // Insert some data
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine1.begin_transaction(txn2);

    for i in 1..=5 {
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO users (id, name, age, email) VALUES (?, ?, ?, ?)".to_string(),
            params: Some(vec![
                Value::Integer(i),
                Value::String(format!("User{}", i)),
                Value::Integer(20 + i),
                Value::String(format!("user{}@example.com", i)),
            ]),
        };
        let result = engine1.apply_operation(insert, txn2);
        assert!(matches!(result, OperationResult::Success(_)));
    }
    engine1.commit(txn2).unwrap();

    // Create another table with indexes
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine1.begin_transaction(txn3);

    let create_orders = SqlOperation::Execute {
        sql:
            "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL, status VARCHAR)"
                .to_string(),
        params: None,
    };
    engine1.apply_operation(create_orders, txn3);

    let insert_order = SqlOperation::Execute {
        sql: "INSERT INTO orders (id, user_id, amount, status) VALUES (?, ?, ?, ?)".to_string(),
        params: Some(vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Decimal("99.99".parse().unwrap()),
            Value::String("pending".to_string()),
        ]),
    };
    engine1.apply_operation(insert_order, txn3);
    engine1.commit(txn3).unwrap();

    // Take a snapshot
    let snapshot = engine1.snapshot().unwrap();
    assert!(!snapshot.is_empty());

    // Create a new engine and restore
    let mut engine2 = SqlTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify data was restored by querying
    let txn4 = HlcTimestamp::new(4, 0, NodeId::new(1));
    engine2.begin_transaction(txn4);

    // Query users table
    let query_users = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM users".to_string(),
        params: None,
    };
    let result = engine2.apply_operation(query_users, txn4);
    assert!(matches!(result, OperationResult::Success(_)));

    // Query specific user
    let query_user = SqlOperation::Query {
        sql: "SELECT name, age FROM users WHERE id = ?".to_string(),
        params: Some(vec![Value::Integer(3)]),
    };
    let result = engine2.apply_operation(query_user, txn4);
    if let OperationResult::Success(response) = result {
        // Response should contain User3 with age 23
        let response_str = format!("{:?}", response);
        // Check if it's a query result with rows
        assert!(response_str.contains("String(\"User3\")") || response_str.contains("User3"));
    }

    // Query orders table
    let query_orders = SqlOperation::Query {
        sql: "SELECT * FROM orders WHERE user_id = ?".to_string(),
        params: Some(vec![Value::Integer(1)]),
    };
    let result = engine2.apply_operation(query_orders, txn4);
    assert!(matches!(result, OperationResult::Success(_)));
}

#[test]
fn test_snapshot_with_active_transaction_fails() {
    let mut engine = SqlTransactionEngine::new();

    // Begin a transaction
    let txn = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin_transaction(txn);

    // Try to snapshot with active transaction
    let result = engine.snapshot();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("active transactions"));

    // Commit the transaction
    engine.commit(txn).unwrap();

    // Now snapshot should succeed
    let result = engine.snapshot();
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_with_deleted_rows() {
    let mut engine = SqlTransactionEngine::new();

    // Create table and insert data
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin_transaction(txn1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE items (id INT PRIMARY KEY, name VARCHAR)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, txn1);

    for i in 1..=10 {
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO items (id, name) VALUES (?, ?)".to_string(),
            params: Some(vec![Value::Integer(i), Value::String(format!("Item{}", i))]),
        };
        engine.apply_operation(insert, txn1);
    }
    engine.commit(txn1).unwrap();

    // Delete some rows
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin_transaction(txn2);

    let delete = SqlOperation::Execute {
        sql: "DELETE FROM items WHERE id > ?".to_string(),
        params: Some(vec![Value::Integer(5)]),
    };
    engine.apply_operation(delete, txn2);
    engine.commit(txn2).unwrap();

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // Restore to new engine
    let mut engine2 = SqlTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify only items 1-5 exist
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin_transaction(txn3);

    let count_query = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM items".to_string(),
        params: None,
    };
    let result = engine2.apply_operation(count_query, txn3);
    if let OperationResult::Success(response) = result {
        // Should have 5 items
        let response_str = format!("{:?}", response);
        assert!(response_str.contains("5") || response_str.contains("[[Integer(5)]]"));
    }

    // Verify item 6 doesn't exist
    let query_deleted = SqlOperation::Query {
        sql: "SELECT * FROM items WHERE id = ?".to_string(),
        params: Some(vec![Value::Integer(6)]),
    };
    let result = engine2.apply_operation(query_deleted, txn3);
    if let OperationResult::Success(response) = result {
        let response_str = format!("{:?}", response);
        assert!(response_str.contains("[]") || response_str.contains("empty"));
    }
}

#[test]
fn test_snapshot_compression() {
    let mut engine = SqlTransactionEngine::new();

    // Create a large table
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin_transaction(txn1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE large_table (id INT PRIMARY KEY, data VARCHAR, value INT)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, txn1);
    engine.commit(txn1).unwrap();

    // Insert many rows with repetitive data (good for compression)
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin_transaction(txn2);

    let repetitive_data = "A".repeat(100);
    for i in 1..=1000 {
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO large_table (id, data, value) VALUES (?, ?, ?)".to_string(),
            params: Some(vec![
                Value::Integer(i),
                Value::String(repetitive_data.clone()),
                Value::Integer(42), // Same value for all rows
            ]),
        };
        engine.apply_operation(insert, txn2);
    }
    engine.commit(txn2).unwrap();

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // The compressed snapshot should be much smaller than the raw data
    // 1000 rows * ~100 bytes per row = ~100KB uncompressed
    // With compression, should be much smaller
    assert!(snapshot.len() < 50000, "Snapshot should be compressed");

    // Verify it can be restored
    let mut engine2 = SqlTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify data integrity
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin_transaction(txn3);

    let count_query = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM large_table".to_string(),
        params: None,
    };
    let result = engine2.apply_operation(count_query, txn3);
    assert!(matches!(result, OperationResult::Success(_)));
}

#[test]
fn test_snapshot_preserves_indexes() {
    let mut engine = SqlTransactionEngine::new();

    // Create table with indexed column
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin_transaction(txn1);

    let create_table = SqlOperation::Execute {
        sql:
            "CREATE TABLE indexed_table (id INT PRIMARY KEY, email VARCHAR UNIQUE, status VARCHAR)"
                .to_string(),
        params: None,
    };
    engine.apply_operation(create_table, txn1);

    // Insert data
    for i in 1..=5 {
        let insert = SqlOperation::Execute {
            sql: "INSERT INTO indexed_table (id, email, status) VALUES (?, ?, ?)".to_string(),
            params: Some(vec![
                Value::Integer(i),
                Value::String(format!("user{}@test.com", i)),
                Value::String("active".to_string()),
            ]),
        };
        engine.apply_operation(insert, txn1);
    }
    engine.commit(txn1).unwrap();

    // Snapshot and restore
    let snapshot = engine.snapshot().unwrap();
    let mut engine2 = SqlTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify index works by querying with indexed column
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin_transaction(txn2);

    let query_by_email = SqlOperation::Query {
        sql: "SELECT id FROM indexed_table WHERE email = ?".to_string(),
        params: Some(vec![Value::String("user3@test.com".to_string())]),
    };
    let result = engine2.apply_operation(query_by_email, txn2);
    if let OperationResult::Success(response) = result {
        let response_str = format!("{:?}", response);
        assert!(response_str.contains("3"));
    }

    // Verify unique constraint is preserved
    let duplicate_insert = SqlOperation::Execute {
        sql: "INSERT INTO indexed_table (id, email, status) VALUES (?, ?, ?)".to_string(),
        params: Some(vec![
            Value::Integer(6),
            Value::String("user3@test.com".to_string()), // Duplicate email
            Value::String("active".to_string()),
        ]),
    };
    let result = engine2.apply_operation(duplicate_insert, txn2);
    // Should fail due to unique constraint
    assert!(matches!(result, OperationResult::Error(_)));

    println!("Indexes and constraints preserved across snapshot/restore");
}
