//! Simple test to debug SQL snapshot issues

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::engine::SqlTransactionEngine;
use proven_sql::stream::operation::SqlOperation;
use proven_stream::{OperationResult, TransactionEngine};

#[test]
fn test_simple_snapshot() {
    // Create engine and set up a simple table
    let mut engine1 = SqlTransactionEngine::new();

    // Create a table
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine1.begin_transaction(txn1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR)".to_string(),
        params: None,
    };
    let result = engine1.apply_operation(create_table, txn1);
    println!("Create table result: {:?}", result);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine1.commit(txn1).unwrap();

    // Insert one row
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine1.begin_transaction(txn2);

    let insert = SqlOperation::Execute {
        sql: "INSERT INTO test (id, name) VALUES (1, 'TestName')".to_string(),
        params: None,
    };
    let result = engine1.apply_operation(insert, txn2);
    println!("Insert result: {:?}", result);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine1.commit(txn2).unwrap();

    // Query before snapshot
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine1.begin_transaction(txn3);
    let query = SqlOperation::Query {
        sql: "SELECT * FROM test".to_string(),
        params: None,
    };
    let result = engine1.apply_operation(query, txn3);
    println!("Query before snapshot: {:?}", result);
    engine1.commit(txn3).unwrap();

    // Take a snapshot
    let snapshot = engine1.snapshot().unwrap();
    println!("Snapshot size: {} bytes", snapshot.len());

    // Create a new engine and restore
    let mut engine2 = SqlTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();
    println!("Snapshot restored");

    // Query after restore
    let txn4 = HlcTimestamp::new(4, 0, NodeId::new(1));
    engine2.begin_transaction(txn4);

    // First check if table exists
    let query_tables = SqlOperation::Query {
        sql: "SELECT * FROM test".to_string(),
        params: None,
    };
    let result = engine2.apply_operation(query_tables, txn4);
    println!("Query after restore: {:?}", result);

    if let OperationResult::Complete(response) = result {
        let response_str = format!("{:?}", response);
        println!("Response string: {}", response_str);
        assert!(response_str.contains("TestName") || response_str.contains("String(\"TestName\")"));
    } else {
        panic!("Query failed after restore");
    }
}
