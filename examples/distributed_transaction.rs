//! Example demonstrating distributed transactions across SQL and KV systems
//!
//! This example shows how a coordinator orchestrates a distributed transaction
//! that spans both SQL and KV storage systems, using two-phase commit.
//!
//! Run with: cargo run --example distributed_transaction

use proven_coordinator::MockCoordinator;
use proven_engine::MockEngine;
use proven_kv::{
    stream::{
        message::{KvOperation, StreamMessage as KvStreamMessage},
        processor::KvStreamProcessor,
        response::MockResponseChannel,
    },
    types::Value as KvValue,
};
use proven_sql::stream::{
    message::StreamMessage as SqlStreamMessage, processor::SqlStreamProcessor,
    response::MockResponseChannel as SqlResponseChannel,
};
use std::collections::BTreeMap;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Distributed Transaction Example ===\n");

    // 1. Create the mock engine (simulates the consensus/streaming layer)
    let engine = Arc::new(MockEngine::new());
    println!("✓ Created mock consensus engine");

    // 2. Create streams for SQL and KV systems
    engine.create_stream("sql-stream".to_string())?;
    engine.create_stream("kv-stream".to_string())?;
    println!("✓ Created SQL and KV streams");

    // 3. Set up SQL processor with response channel
    let sql_response_channel = Box::new(Arc::new(SqlResponseChannel::new()));
    let mut sql_processor = SqlStreamProcessor::new(sql_response_channel);

    // 4. Set up KV processor with response channel
    let kv_response_channel = Box::new(Arc::new(MockResponseChannel::new()));
    let mut kv_processor = KvStreamProcessor::new(kv_response_channel);

    println!("✓ Initialized SQL and KV processors");

    // 5. Create consumers for the streams
    let mut sql_consumer = engine.stream_messages("sql-stream", None)?;
    let mut kv_consumer = engine.stream_messages("kv-stream", None)?;

    // 6. Spawn SQL processor task
    tokio::spawn(async move {
        println!("  [SQL] Processor started");
        while let Some(msg) = sql_consumer.recv().await {
            let sql_msg = SqlStreamMessage {
                body: msg.body,
                headers: msg.headers.clone(),
            };

            if let Some(txn_id) = msg.headers.get("txn_id") {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!(
                        "  [SQL] Received {} phase for transaction {}",
                        phase, txn_id
                    );
                } else if !sql_msg.body.is_empty() {
                    println!("  [SQL] Processing operation for transaction {}", txn_id);
                    // In a real implementation, we'd deserialize and execute SQL
                    let sql_text = String::from_utf8_lossy(&sql_msg.body);
                    println!("  [SQL] Query: {}", sql_text);
                }
            }

            // Process the message
            if let Err(e) = sql_processor.process_message(sql_msg).await {
                println!("  [SQL] Error processing message: {}", e);
            }
        }
    });

    // 7. Spawn KV processor task
    tokio::spawn(async move {
        println!("  [KV] Processor started");
        while let Some(msg) = kv_consumer.recv().await {
            let kv_msg = KvStreamMessage::new(msg.body.clone(), msg.headers.clone());

            if let Some(txn_id) = msg.headers.get("txn_id") {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!("  [KV] Received {} phase for transaction {}", phase, txn_id);
                } else if !kv_msg.body.is_empty() {
                    // Try to decode the KV operation
                    if let Ok((op, _)) = bincode::decode_from_slice::<KvOperation, _>(
                        &msg.body,
                        bincode::config::standard(),
                    ) {
                        println!(
                            "  [KV] Processing operation for transaction {}: {:?}",
                            txn_id, op
                        );
                    }
                }
            }

            // Process the message
            if let Err(e) = kv_processor.process_message(kv_msg).await {
                println!("  [KV] Error processing message: {}", e);
            }
        }
    });

    // 8. Create the coordinator
    let coordinator = Arc::new(MockCoordinator::new(
        "coordinator-1".to_string(),
        engine.clone(),
    ));
    println!("✓ Created coordinator\n");

    // 9. Begin a distributed transaction
    println!("=== Starting Distributed Transaction ===");
    let txn_id = coordinator
        .begin_transaction(vec!["sql-stream".to_string(), "kv-stream".to_string()])
        .await?;
    println!("✓ Transaction started: {}\n", txn_id);

    // 10. Execute SQL operations
    println!("=== Executing SQL Operations ===");

    // Create table
    let sql_create = b"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)".to_vec();
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_create)
        .await?;
    println!("→ Sent CREATE TABLE command");

    // Insert user
    let sql_insert =
        b"INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')".to_vec();
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_insert)
        .await?;
    println!("→ Sent INSERT command\n");

    // 11. Execute KV operations
    println!("=== Executing KV Operations ===");

    // Store user metadata in KV
    let user_metadata = KvOperation::Put {
        key: "user:1:metadata".to_string(),
        value: KvValue::String("created_at:2024-01-01T00:00:00Z".to_string()),
    };
    let metadata_bytes = bincode::encode_to_vec(&user_metadata, bincode::config::standard())?;
    coordinator
        .execute_operation(&txn_id, "kv-stream", metadata_bytes)
        .await?;
    println!("→ Stored user metadata");

    // Store user preferences in KV
    let mut preferences = BTreeMap::new();
    preferences.insert("theme".to_string(), KvValue::String("dark".to_string()));
    preferences.insert("language".to_string(), KvValue::String("en-US".to_string()));
    preferences.insert("notifications".to_string(), KvValue::Boolean(true));

    let user_prefs = KvOperation::Put {
        key: "user:1:preferences".to_string(),
        value: KvValue::Map(preferences),
    };
    let prefs_bytes = bincode::encode_to_vec(&user_prefs, bincode::config::standard())?;
    coordinator
        .execute_operation(&txn_id, "kv-stream", prefs_bytes)
        .await?;
    println!("→ Stored user preferences");

    // Store session data in KV
    let session_data = KvOperation::Put {
        key: "session:abc123".to_string(),
        value: KvValue::Map(BTreeMap::from([
            ("user_id".to_string(), KvValue::Integer(1)),
            ("ip".to_string(), KvValue::String("192.168.1.1".to_string())),
            ("expires".to_string(), KvValue::Integer(1704067200)), // Unix timestamp
        ])),
    };
    let session_bytes = bincode::encode_to_vec(&session_data, bincode::config::standard())?;
    coordinator
        .execute_operation(&txn_id, "kv-stream", session_bytes)
        .await?;
    println!("→ Stored session data\n");

    // 12. Commit the distributed transaction
    println!("=== Committing Distributed Transaction ===");
    coordinator.commit_transaction(&txn_id).await?;
    println!("✓ Transaction committed successfully!\n");

    // 13. Verify transaction state
    let state = coordinator.get_transaction_state(&txn_id);
    println!("Transaction state: {:?}\n", state);

    // Give processors time to finish processing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 14. Demonstrate abort scenario
    println!("=== Demonstrating Transaction Abort ===");
    let abort_txn_id = coordinator
        .begin_transaction(vec!["sql-stream".to_string(), "kv-stream".to_string()])
        .await?;
    println!("✓ Started new transaction: {}", abort_txn_id);

    // Execute some operations
    let sql_op = b"UPDATE users SET name = 'Bob' WHERE id = 1".to_vec();
    coordinator
        .execute_operation(&abort_txn_id, "sql-stream", sql_op)
        .await?;
    println!("→ Sent UPDATE command");

    let kv_op = KvOperation::Delete {
        key: "session:abc123".to_string(),
    };
    let kv_bytes = bincode::encode_to_vec(&kv_op, bincode::config::standard())?;
    coordinator
        .execute_operation(&abort_txn_id, "kv-stream", kv_bytes)
        .await?;
    println!("→ Sent DELETE command");

    // Abort the transaction
    println!("\n! Aborting transaction...");
    coordinator.abort_transaction(&abort_txn_id).await?;
    println!("✓ Transaction aborted successfully!");

    let abort_state = coordinator.get_transaction_state(&abort_txn_id);
    println!("Transaction state: {:?}\n", abort_state);

    // Give processors final time to process
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    println!("=== Example Complete ===");

    // Clean shutdown (in real system, processors would run continuously)
    // For the example, we'll just let them timeout naturally

    Ok(())
}

// Example output:
//
// === Distributed Transaction Example ===
//
// ✓ Created mock consensus engine
// ✓ Created SQL and KV streams
// ✓ Initialized SQL and KV processors
// ✓ Created coordinator
//
// === Starting Distributed Transaction ===
// ✓ Transaction started: coordinator-1:1_0_0000000000000123
//
// === Executing SQL Operations ===
//   [SQL] Processing operation for transaction coordinator-1:1_0_0000000000000123
//   [SQL] Query: CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
// → Sent CREATE TABLE command
//   [SQL] Processing operation for transaction coordinator-1:1_0_0000000000000123
//   [SQL] Query: INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')
// → Sent INSERT command
//
// === Executing KV Operations ===
//   [KV] Processing operation: Put { key: "user:1:metadata", value: String("created_at:2024-01-01T00:00:00Z") }
// → Stored user metadata
//   [KV] Processing operation: Put { key: "user:1:preferences", value: Map({...}) }
// → Stored user preferences
//   [KV] Processing operation: Put { key: "session:abc123", value: Map({...}) }
// → Stored session data
//
// === Committing Distributed Transaction ===
//   [SQL] Received prepare phase for transaction coordinator-1:1_0_0000000000000123
//   [KV] Received prepare phase for transaction coordinator-1:1_0_0000000000000123
//   [SQL] Received commit phase for transaction coordinator-1:1_0_0000000000000123
//   [KV] Received commit phase for transaction coordinator-1:1_0_0000000000000123
// ✓ Transaction committed successfully!
//
// Transaction state: Some(Committed)
