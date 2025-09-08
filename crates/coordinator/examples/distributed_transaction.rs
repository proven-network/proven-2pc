//! Example demonstrating distributed transactions across SQL, KV, and Queue systems
//!
//! This example shows how a coordinator orchestrates a distributed transaction
//! that spans SQL, KV, and Queue storage systems, using two-phase commit.
//!
//! Run with: cargo run --example distributed_transaction

use proven_coordinator::MockCoordinator;
use proven_engine::{MockClient, MockEngine};
use proven_kv::{
    stream::{engine::KvTransactionEngine, operation::KvOperation},
    types::Value as KvValue,
};
use proven_queue::{
    stream::{QueueEngine, QueueOperation},
    types::QueueValue,
};
use proven_sql::stream::{engine::SqlTransactionEngine, operation::SqlOperation};
use proven_stream::StreamProcessor;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Distributed Transaction Example ===\n");

    // 1. Create the mock engine (simulates the consensus/streaming layer)
    let engine = Arc::new(MockEngine::new());
    println!("✓ Created mock consensus engine");

    // 2. Create streams for SQL, KV, and Queue systems
    engine.create_stream("sql-stream".to_string())?;
    engine.create_stream("kv-stream".to_string())?;
    engine.create_stream("queue-stream".to_string())?;
    println!("✓ Created SQL, KV, and Queue streams");

    // 3. Create clients for SQL, KV, and Queue processors
    let sql_client = Arc::new(MockClient::new("sql-processor".to_string(), engine.clone()));
    let kv_client = Arc::new(MockClient::new("kv-processor".to_string(), engine.clone()));
    let queue_client = Arc::new(MockClient::new(
        "queue-processor".to_string(),
        engine.clone(),
    ));
    println!("✓ Created processor clients");

    // 4. Create SQL processor with its client
    let sql_engine = SqlTransactionEngine::new();
    let mut sql_processor =
        StreamProcessor::new(sql_engine, sql_client.clone(), "sql-stream".to_string());

    // 5. Create KV processor with its client
    let kv_engine = KvTransactionEngine::new();
    let mut kv_processor =
        StreamProcessor::new(kv_engine, kv_client.clone(), "kv-stream".to_string());

    // 6. Create Queue processor with its client
    let queue_engine = QueueEngine::new();
    let mut queue_processor = StreamProcessor::new(
        queue_engine,
        queue_client.clone(),
        "queue-stream".to_string(),
    );

    println!("✓ Initialized SQL, KV, and Queue processors");

    // 7. Spawn SQL processor task
    tokio::spawn(async move {
        println!("  [SQL] Processor started");

        // SQL processor now manages its own stream consumption
        if let Err(e) = sql_processor.run().await {
            println!("  [SQL] Processor error: {:?}", e);
        }

        println!("  [SQL] Processor stopped");
    });

    // 8. Spawn KV processor task
    tokio::spawn(async move {
        println!("  [KV] Processor started");

        // KV processor now manages its own stream consumption
        if let Err(e) = kv_processor.run().await {
            println!("  [KV] Processor error: {:?}", e);
        }

        println!("  [KV] Processor stopped");
    });

    // 9. Spawn Queue processor task
    tokio::spawn(async move {
        println!("  [Queue] Processor started");

        // Queue processor now manages its own stream consumption
        if let Err(e) = queue_processor.run().await {
            println!("  [Queue] Processor error: {:?}", e);
        }

        println!("  [Queue] Processor stopped");
    });

    // 10. Create the coordinator with prepare vote collection enabled
    let coordinator = Arc::new(MockCoordinator::new_with_prepare_votes(
        "coordinator-1".to_string(),
        engine.clone(),
    ));
    println!("✓ Created coordinator\n");

    // 11. Begin a distributed transaction (participants discovered dynamically)
    println!("=== Starting Distributed Transaction ===");
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await?;
    println!("✓ Transaction started: {}", txn_id);
    println!("  (Participants will be discovered as operations are sent)\n");

    // 12. Execute SQL operations
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

    // 13. Execute KV operations
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

    // 14. Execute Queue operations
    println!("=== Executing Queue Operations ===");

    // Enqueue notification tasks
    let notification_queue = QueueOperation::Enqueue {
        queue_name: "notifications".to_string(),
        value: QueueValue::Json(serde_json::json!({
            "type": "welcome",
            "user_id": 1,
            "email": "alice@example.com",
            "template": "welcome_email"
        })),
    };
    let notification_bytes = serde_json::to_vec(&notification_queue)?;
    coordinator
        .execute_operation(&txn_id, "queue-stream", notification_bytes)
        .await?;
    println!("→ Enqueued welcome notification");

    // Enqueue audit log event
    let audit_event = QueueOperation::Enqueue {
        queue_name: "audit_log".to_string(),
        value: QueueValue::Json(serde_json::json!({
            "event": "user_created",
            "user_id": 1,
            "timestamp": "2024-01-01T00:00:00Z",
            "ip": "192.168.1.1"
        })),
    };
    let audit_bytes = serde_json::to_vec(&audit_event)?;
    coordinator
        .execute_operation(&txn_id, "queue-stream", audit_bytes)
        .await?;
    println!("→ Enqueued audit log event");

    // Enqueue background job
    let background_job = QueueOperation::Enqueue {
        queue_name: "background_jobs".to_string(),
        value: QueueValue::Json(serde_json::json!({
            "job": "sync_user_data",
            "user_id": 1,
            "priority": "low",
            "retry_count": 0,
            "max_retries": 3
        })),
    };
    let job_bytes = serde_json::to_vec(&background_job)?;
    coordinator
        .execute_operation(&txn_id, "queue-stream", job_bytes)
        .await?;
    println!("→ Enqueued background sync job\n");

    // 15. Commit the distributed transaction
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

    // Try to dequeue from notifications (this will be rolled back)
    let dequeue_op = QueueOperation::Dequeue {
        queue_name: "notifications".to_string(),
    };
    let dequeue_bytes = serde_json::to_vec(&dequeue_op)?;
    coordinator
        .execute_operation(&abort_txn_id, "queue-stream", dequeue_bytes)
        .await?;
    println!("→ Sent DEQUEUE command");

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
