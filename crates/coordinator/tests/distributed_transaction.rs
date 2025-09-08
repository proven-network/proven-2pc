//! Integration test demonstrating distributed transactions across SQL and KV systems

use proven_coordinator::MockCoordinator;
use proven_engine::MockEngine;
use proven_kv::stream::operation::KvOperation;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_distributed_transaction_sql_and_kv() {
    // Create the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create streams for SQL and KV
    engine.create_stream("sql-stream".to_string()).unwrap();
    engine.create_stream("kv-stream".to_string()).unwrap();

    // Set up consumers to verify messages are being sent
    let mut sql_consumer = engine.stream_messages("sql-stream", None).unwrap();
    let mut kv_consumer = engine.stream_messages("kv-stream", None).unwrap();

    // Create the coordinator
    let coordinator = Arc::new(MockCoordinator::new(
        "test-coord".to_string(),
        engine.clone(),
    ));

    // Begin a distributed transaction (participants discovered dynamically)
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await
        .unwrap();

    println!("Started distributed transaction: {}", txn_id);

    // Spawn a task to consume SQL messages
    let sql_txn_id = txn_id.clone();
    tokio::spawn(async move {
        let mut operation_count = 0;
        while let Some((msg, _, _)) = sql_consumer.recv().await {
            if let Some(msg_txn_id) = msg.headers.get("txn_id")
                && msg_txn_id == &sql_txn_id
            {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!("SQL received {} phase for txn {}", phase, msg_txn_id);
                } else if !msg.body.is_empty() {
                    operation_count += 1;
                    println!(
                        "SQL received operation {} for txn {}",
                        operation_count, msg_txn_id
                    );
                }
            }
        }
    });

    // Spawn a task to consume KV messages
    let kv_txn_id = txn_id.clone();
    tokio::spawn(async move {
        let mut operation_count = 0;
        while let Some((msg, _, _)) = kv_consumer.recv().await {
            if let Some(msg_txn_id) = msg.headers.get("txn_id")
                && msg_txn_id == &kv_txn_id
            {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!("KV received {} phase for txn {}", phase, msg_txn_id);
                } else if !msg.body.is_empty() {
                    operation_count += 1;
                    // Try to decode as KV operation
                    if let Ok(op) = serde_json::from_slice::<KvOperation>(&msg.body) {
                        println!("KV received operation {}: {:?}", operation_count, op);
                    }
                }
            }
        }
    });

    // Execute SQL operations (as raw bytes - in real use would be serialized SQL requests)
    let sql_create = b"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".to_vec();
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_create)
        .await
        .unwrap();

    let sql_insert = b"INSERT INTO users (id, name) VALUES (1, 'Alice')".to_vec();
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_insert)
        .await
        .unwrap();

    // Execute KV operations
    let kv_put = KvOperation::Put {
        key: "user:1:metadata".to_string(),
        value: proven_kv::types::Value::String("created_at:2024-01-01".to_string()),
    };
    let kv_put_bytes = serde_json::to_vec(&kv_put).unwrap();

    coordinator
        .execute_operation(&txn_id, "kv-stream", kv_put_bytes)
        .await
        .unwrap();

    // Add another KV operation with a map value
    let mut prefs = BTreeMap::new();
    prefs.insert(
        "theme".to_string(),
        proven_kv::types::Value::String("dark".to_string()),
    );
    prefs.insert(
        "lang".to_string(),
        proven_kv::types::Value::String("en".to_string()),
    );

    let kv_put2 = KvOperation::Put {
        key: "user:1:preferences".to_string(),
        value: proven_kv::types::Value::Map(prefs),
    };
    let kv_put2_bytes = serde_json::to_vec(&kv_put2).unwrap();

    coordinator
        .execute_operation(&txn_id, "kv-stream", kv_put2_bytes)
        .await
        .unwrap();

    // Commit the distributed transaction
    println!("Committing distributed transaction...");
    coordinator.commit_transaction(&txn_id).await.unwrap();

    // Verify transaction was committed
    assert!(matches!(
        coordinator.get_transaction_state(&txn_id),
        Some(proven_coordinator::TransactionState::Committed)
    ));

    println!("Distributed transaction committed successfully!");

    // Give message processors time to receive messages
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_distributed_transaction_abort() {
    let engine = Arc::new(MockEngine::new());

    // Create streams
    engine.create_stream("sql-stream".to_string()).unwrap();
    engine.create_stream("kv-stream".to_string()).unwrap();

    let coordinator = Arc::new(MockCoordinator::new(
        "test-coord".to_string(),
        engine.clone(),
    ));

    // Begin transaction
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await
        .unwrap();

    // Execute some operations
    let sql_op = b"INSERT INTO test VALUES (1)".to_vec();
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_op)
        .await
        .unwrap();

    let kv_op = KvOperation::Put {
        key: "test:1".to_string(),
        value: proven_kv::types::Value::Integer(42),
    };
    let kv_op_bytes = serde_json::to_vec(&kv_op).unwrap();
    coordinator
        .execute_operation(&txn_id, "kv-stream", kv_op_bytes)
        .await
        .unwrap();

    // Abort the transaction
    println!("Aborting distributed transaction...");
    coordinator.abort_transaction(&txn_id).await.unwrap();

    // Verify transaction was aborted
    assert!(matches!(
        coordinator.get_transaction_state(&txn_id),
        Some(proven_coordinator::TransactionState::Aborted)
    ));

    println!("Distributed transaction aborted successfully!");
}

#[tokio::test]
async fn test_coordinator_lifecycle() {
    let engine = Arc::new(MockEngine::new());

    // Create streams
    engine.create_stream("sql-stream".to_string()).unwrap();
    engine.create_stream("kv-stream".to_string()).unwrap();

    let coordinator = MockCoordinator::new("coord-1".to_string(), engine.clone());

    // Begin a transaction
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await
        .unwrap();

    // Verify initial state is Active
    assert!(matches!(
        coordinator.get_transaction_state(&txn_id),
        Some(proven_coordinator::TransactionState::Active)
    ));

    // Execute an operation
    let kv_op = KvOperation::Get {
        key: "test".to_string(),
    };
    let bytes = serde_json::to_vec(&kv_op).unwrap();
    coordinator
        .execute_operation(&txn_id, "kv-stream", bytes)
        .await
        .unwrap();

    // Transaction should still be active
    assert!(matches!(
        coordinator.get_transaction_state(&txn_id),
        Some(proven_coordinator::TransactionState::Active)
    ));

    // Commit the transaction
    coordinator.commit_transaction(&txn_id).await.unwrap();

    // Verify final state is Committed
    assert!(matches!(
        coordinator.get_transaction_state(&txn_id),
        Some(proven_coordinator::TransactionState::Committed)
    ));
}
