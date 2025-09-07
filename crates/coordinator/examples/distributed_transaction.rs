//! Example demonstrating distributed transactions across SQL and KV systems
//!
//! This example shows how a coordinator orchestrates a distributed transaction
//! that spans both SQL and KV storage systems, using two-phase commit.
//!
//! Run with: cargo run --example distributed_transaction

use proven_coordinator::MockCoordinator;
use proven_engine::{MockClient, MockEngine};
use proven_kv::{
    stream::{KvStreamProcessor, operation::KvOperation},
    types::Value as KvValue,
};
use proven_sql::stream::{SqlStreamProcessor, operation::SqlOperation};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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

    // 3. Create clients for SQL and KV processors
    let sql_client = Arc::new(MockClient::new("sql-processor".to_string(), engine.clone()));
    let kv_client = Arc::new(MockClient::new("kv-processor".to_string(), engine.clone()));
    println!("✓ Created processor clients");

    // 4. Create SQL processor with its client
    let sql_processor = SqlStreamProcessor::new(sql_client.clone(), "sql-stream".to_string());

    // 5. Create KV processor with its client
    let kv_processor = KvStreamProcessor::new(kv_client.clone(), "kv-stream".to_string());

    println!("✓ Initialized SQL and KV processors");

    // 6. Spawn SQL processor task
    tokio::spawn(async move {
        println!("  [SQL] Processor started");

        // SQL processor consumes messages directly from its stream via the client
        let mut sql_stream = sql_client
            .stream_messages("sql-stream".to_string(), None)
            .await
            .expect("Failed to create SQL stream consumer");

        let mut processor = sql_processor;

        while let Some((msg, _, _)) = sql_stream.recv().await {
            // Log what we're processing
            if let Some(txn_id) = msg.headers.get("txn_id") {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!(
                        "  [SQL] Received {} phase for transaction {}",
                        phase, txn_id
                    );
                } else if !msg.body.is_empty() {
                    println!("  [SQL] Processing operation for transaction {}", txn_id);
                }
            }

            // Process the message directly - no conversion needed
            if let Err(e) = processor.process_message(msg).await {
                println!("  [SQL] Error processing message: {}", e);
            }
        }

        println!("  [SQL] Processor stopped");
    });

    // 7. Spawn KV processor task
    tokio::spawn(async move {
        println!("  [KV] Processor started");

        // KV processor consumes messages directly from its stream via the client
        let mut kv_stream = kv_client
            .stream_messages("kv-stream".to_string(), None)
            .await
            .expect("Failed to create KV stream consumer");

        let mut processor = kv_processor;

        while let Some((msg, _, _)) = kv_stream.recv().await {
            // Log what we're processing
            if let Some(txn_id) = msg.headers.get("txn_id") {
                if let Some(phase) = msg.headers.get("txn_phase") {
                    println!("  [KV] Received {} phase for transaction {}", phase, txn_id);
                } else if !msg.body.is_empty() {
                    // Try to decode the KV operation
                    if let Ok(op) = serde_json::from_slice::<KvOperation>(&msg.body) {
                        println!(
                            "  [KV] Processing operation for transaction {}: {:?}",
                            txn_id, op
                        );
                    }
                }
            }

            // Process the message
            if let Err(e) = processor.process_message(msg).await {
                println!("  [KV] Error processing message: {}", e);
            }
        }

        println!("  [KV] Processor stopped");
    });

    // 8. Create the coordinator with prepare vote collection enabled
    let coordinator = Arc::new(MockCoordinator::new_with_prepare_votes(
        "coordinator-1".to_string(),
        engine.clone(),
    ));
    println!("✓ Created coordinator\n");

    // 9. Begin a distributed transaction (participants discovered dynamically)
    println!("=== Starting Distributed Transaction ===");
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await?;
    println!("✓ Transaction started: {}", txn_id);
    println!("  (Participants will be discovered as operations are sent)\n");

    // 10. Execute SQL operations
    println!("=== Executing SQL Operations ===");

    // Create table
    let sql_create = SqlOperation::Execute {
        sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)".to_string(),
    };
    let sql_create_bytes = serde_json::to_vec(&sql_create)?;
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_create_bytes)
        .await?;
    println!("→ Sent CREATE TABLE command");

    // Insert user
    let sql_insert = SqlOperation::Execute {
        sql: "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
            .to_string(),
    };
    let sql_insert_bytes = serde_json::to_vec(&sql_insert)?;
    coordinator
        .execute_operation(&txn_id, "sql-stream", sql_insert_bytes)
        .await?;
    println!("→ Sent INSERT command\n");

    // 11. Execute KV operations
    println!("=== Executing KV Operations ===");

    // Store user metadata in KV
    let user_metadata = KvOperation::Put {
        key: "user:1:metadata".to_string(),
        value: KvValue::String("created_at:2024-01-01T00:00:00Z".to_string()),
    };
    let metadata_bytes = serde_json::to_vec(&user_metadata)?;
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
    let prefs_bytes = serde_json::to_vec(&user_prefs)?;
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
    let session_bytes = serde_json::to_vec(&session_data)?;
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
        .begin_transaction(Duration::from_secs(30))
        .await?;
    println!("✓ Started new transaction: {}", abort_txn_id);

    // Execute some operations
    let sql_update = SqlOperation::Execute {
        sql: "UPDATE users SET name = 'Bob' WHERE id = 1".to_string(),
    };
    let sql_update_bytes = serde_json::to_vec(&sql_update)?;
    coordinator
        .execute_operation(&abort_txn_id, "sql-stream", sql_update_bytes)
        .await?;
    println!("→ Sent UPDATE command");

    let kv_op = KvOperation::Delete {
        key: "session:abc123".to_string(),
    };
    let kv_bytes = serde_json::to_vec(&kv_op)?;
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
