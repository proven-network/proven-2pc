//! Example demonstrating distributed transactions across SQL, KV, Queue, and Resource systems
//!
//! This example shows how a coordinator orchestrates a distributed transaction
//! that spans SQL, KV, Queue, and Resource storage systems, using two-phase commit.
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
use proven_resource::{
    stream::{ResourceEngine, ResourceOperation},
    types::Amount,
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

    // 2. Create streams for SQL, KV, Queue, and Resource systems
    engine.create_stream("sql-stream".to_string())?;
    engine.create_stream("kv-stream".to_string())?;
    engine.create_stream("queue-stream".to_string())?;
    engine.create_stream("resource-stream".to_string())?;
    println!("✓ Created SQL, KV, Queue, and Resource streams");

    // 3. Create clients for SQL, KV, Queue, and Resource processors
    let sql_client = Arc::new(MockClient::new("sql-processor".to_string(), engine.clone()));
    let kv_client = Arc::new(MockClient::new("kv-processor".to_string(), engine.clone()));
    let queue_client = Arc::new(MockClient::new(
        "queue-processor".to_string(),
        engine.clone(),
    ));
    let resource_client = Arc::new(MockClient::new(
        "resource-processor".to_string(),
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

    // 7. Create Resource processor with its client
    let resource_engine = ResourceEngine::new();
    let mut resource_processor = StreamProcessor::new(
        resource_engine,
        resource_client.clone(),
        "resource-stream".to_string(),
    );

    println!("✓ Initialized SQL, KV, Queue, and Resource processors");

    // 8. Spawn SQL processor task
    tokio::spawn(async move {
        println!("  [SQL] Processor started");

        // SQL processor now manages its own stream consumption
        if let Err(e) = sql_processor.run().await {
            println!("  [SQL] Processor error: {:?}", e);
        }

        println!("  [SQL] Processor stopped");
    });

    // 9. Spawn KV processor task
    tokio::spawn(async move {
        println!("  [KV] Processor started");

        // KV processor now manages its own stream consumption
        if let Err(e) = kv_processor.run().await {
            println!("  [KV] Processor error: {:?}", e);
        }

        println!("  [KV] Processor stopped");
    });

    // 10. Spawn Queue processor task
    tokio::spawn(async move {
        println!("  [Queue] Processor started");

        // Queue processor now manages its own stream consumption
        if let Err(e) = queue_processor.run().await {
            println!("  [Queue] Processor error: {:?}", e);
        }

        println!("  [Queue] Processor stopped");
    });

    // 11. Spawn Resource processor task
    tokio::spawn(async move {
        println!("  [Resource] Processor started");

        // Resource processor now manages its own stream consumption
        if let Err(e) = resource_processor.run().await {
            println!("  [Resource] Processor error: {:?}", e);
        }

        println!("  [Resource] Processor stopped");
    });

    // 12. Create the coordinator with prepare vote collection enabled
    let coordinator = Arc::new(MockCoordinator::new_with_prepare_votes(
        "coordinator-1".to_string(),
        engine.clone(),
    ));
    println!("✓ Created coordinator\n");

    // 13. Begin a distributed transaction (participants discovered dynamically)
    println!("=== Starting Distributed Transaction ===");
    let txn_id = coordinator
        .begin_transaction(Duration::from_secs(30))
        .await?;
    println!("✓ Transaction started: {}", txn_id);
    println!("  (Participants will be discovered as operations are sent)\n");

    // 14. Execute SQL operations
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

    // 15. Execute KV operations
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

    // 16. Execute Queue operations
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

    // 17. Execute Resource operations
    println!("=== Executing Resource Operations ===");

    // Initialize a loyalty points resource
    let init_resource = ResourceOperation::Initialize {
        name: "Loyalty Points".to_string(),
        symbol: "LP".to_string(),
        decimals: 2,
    };
    let init_bytes = serde_json::to_vec(&init_resource)?;
    coordinator
        .execute_operation(&txn_id, "resource-stream", init_bytes)
        .await?;
    println!("→ Initialized Loyalty Points resource");

    // Mint initial points for the new user
    let mint_points = ResourceOperation::Mint {
        to: "user:1".to_string(),
        amount: Amount::from_integer(10000, 2), // 100.00 LP
        memo: Some("Welcome bonus".to_string()),
    };
    let mint_bytes = serde_json::to_vec(&mint_points)?;
    coordinator
        .execute_operation(&txn_id, "resource-stream", mint_bytes)
        .await?;
    println!("→ Minted 100.00 LP welcome bonus to user:1");

    // Transfer some points to a referrer
    let transfer_points = ResourceOperation::Transfer {
        from: "user:1".to_string(),
        to: "user:referrer".to_string(),
        amount: Amount::from_integer(1000, 2), // 10.00 LP
        memo: Some("Referral reward".to_string()),
    };
    let transfer_bytes = serde_json::to_vec(&transfer_points)?;
    coordinator
        .execute_operation(&txn_id, "resource-stream", transfer_bytes)
        .await?;
    println!("→ Transferred 10.00 LP referral reward\n");

    // 18. Commit the distributed transaction
    println!("=== Committing Distributed Transaction ===");
    coordinator.commit_transaction(&txn_id).await?;
    println!("✓ Transaction committed successfully!\n");

    // 19. Verify transaction state
    let state = coordinator.get_transaction_state(&txn_id);
    println!("Transaction state: {:?}\n", state);

    // Give processors time to finish processing
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // 20. Demonstrate abort scenario
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

    // Try to burn points (this will be rolled back)
    let burn_op = ResourceOperation::Burn {
        from: "user:1".to_string(),
        amount: Amount::from_integer(5000, 2), // 50.00 LP
        memo: Some("Redemption attempt".to_string()),
    };
    let burn_bytes = serde_json::to_vec(&burn_op)?;
    coordinator
        .execute_operation(&abort_txn_id, "resource-stream", burn_bytes)
        .await?;
    println!("→ Sent BURN command");

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
