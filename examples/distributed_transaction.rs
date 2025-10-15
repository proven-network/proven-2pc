//! Distributed transaction example with the new coordinator
//!
//! This example demonstrates:
//! - Setting up real stream processors for KV, Queue, and Resource storage
//! - Creating storage-specific clients that work with the coordinator
//! - Executing a distributed transaction across multiple storage types
//! - Proper two-phase commit with real stream processors

use proven_coordinator::{Coordinator, Executor};
use proven_engine::{MockClient, MockEngine};
use proven_kv::types::Value;
use proven_kv_client::KvClient;
use proven_queue_client::QueueClient;
use proven_resource_client::ResourceClient;
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use proven_sql_client::SqlClient;
use std::sync::Arc;
use std::time::Duration;

// Queue and Resource clients are now imported from their respective crates

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Distributed Transaction Example ===\n");

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create streams for each storage type
    // In a real system, streams would be placed in consensus groups
    // For this example, we'll use the mock engine's default group placement
    engine.create_stream("sql_stream".to_string())?;
    engine.create_stream("kv_stream".to_string())?;
    engine.create_stream("queue_stream".to_string())?;
    engine.create_stream("resource_stream".to_string())?;

    // Place all streams in group 1 (mock engine default)
    // The runner will automatically start processors on nodes in the group
    println!("✓ Created streams: sql_stream, kv_stream, queue_stream, resource_stream");

    // Create and start the runner to manage stream processors
    let runner_client = Arc::new(MockClient::new("runner-node".to_string(), engine.clone()));
    // Create a memory-based snapshot store for this example
    let snapshot_store = Arc::new(MemorySnapshotStore::new());
    let runner = Arc::new(Runner::new(
        "runner-node",
        runner_client.clone(),
        snapshot_store,
    ));
    runner.start().await.unwrap();
    println!("✓ Started runner with snapshot support");

    // The runner will automatically start processors as needed when transactions are executed
    println!("✓ Runner will manage stream processors on demand\n");

    // Create the coordinator with the runner
    let coordinator_client = Arc::new(MockClient::new("coordinator".to_string(), engine.clone()));
    let coordinator = Coordinator::new(
        "coordinator-1".to_string(),
        coordinator_client,
        runner.clone(),
    );
    println!("✓ Created coordinator with runner integration\n");

    // Begin a distributed transaction
    let executor = Arc::new(
        coordinator
            .begin_read_write(
                Duration::from_secs(60),
                vec![],
                "distributed_transaction".to_string(),
            )
            .await?,
    );

    // Create storage-specific clients for this transaction
    let sql = SqlClient::new(executor.clone());
    let kv = KvClient::new(executor.clone());
    let queue = QueueClient::new(executor.clone());
    let resource = ResourceClient::new(executor.clone());

    // Execute real operations through the processors
    println!("Executing distributed operations:");

    println!("\n1. SQL Operations:");
    sql.create_table(
        "sql_stream",
        "users",
        "id INTEGER PRIMARY KEY, name TEXT, level INTEGER",
    )
    .await?;
    println!("   ✓ Created users table");

    sql.execute(
        "sql_stream",
        "INSERT INTO users (id, name, level) VALUES (1, 'Alice Smith', 5)",
    )
    .await?;
    println!("   ✓ Inserted user record");

    println!("\n2. KV Operations:");
    kv.put_string("kv_stream", "user:alice", "Alice Smith")
        .await?;
    println!("   ✓ Stored user:alice");

    kv.put_integer("kv_stream", "user:alice:level", 5).await?;
    println!("   ✓ Stored user level");

    println!("\n3. Resource Operations:");
    resource
        .mint_integer("resource_stream", "system", 1000)
        .await?;
    println!("   ✓ Minted 1000 coins to system");

    resource
        .transfer_integer("resource_stream", "system", "alice", 500)
        .await?;
    println!("   ✓ Transferred 500 coins to alice");

    println!("\n4. Queue Operations:");
    queue
        .enqueue_bytes("queue_stream", b"welcome:alice".to_vec())
        .await?;
    println!("   ✓ Enqueued welcome event");

    println!("\n5. Complex Types (KV):");
    let preferences = Value::Map(
        vec![
            ("theme".to_string(), Value::Str("dark".to_string())),
            ("language".to_string(), Value::Str("en".to_string())),
        ]
        .into_iter()
        .collect(),
    );
    kv.put("kv_stream", "user:alice:preferences", preferences)
        .await?;
    println!("   ✓ Stored user preferences");

    println!("\n6. Cross-storage Transaction:");
    // Update SQL record
    sql.update("sql_stream", "users", "level = 6", Some("id = 1"))
        .await?;
    println!("   ✓ Updated user level in SQL");

    // Transfer resources
    resource
        .transfer_integer("resource_stream", "alice", "shop", 100)
        .await?;
    println!("   ✓ Transferred 100 coins for purchase");

    kv.put_string("kv_stream", "inventory:alice:sword", "iron_sword")
        .await?;
    println!("   ✓ Added sword to inventory");

    queue
        .enqueue_bytes("queue_stream", b"first_purchase:alice".to_vec())
        .await?;
    println!("   ✓ Enqueued achievement");

    // Commit the distributed transaction
    println!("\nCommitting distributed transaction...");
    executor.finish().await?;
    println!("✅ Transaction committed successfully!");

    // Give processors time to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the changes (in a new transaction)
    println!("\n--- Verification ---");
    let verify_executor = Arc::new(
        coordinator
            .begin_read_only(vec![], "distributed_transaction_verification".to_string())
            .await?,
    );
    let sql_verify = SqlClient::new(verify_executor.clone());
    let kv_verify = KvClient::new(verify_executor.clone());
    let resource_verify = ResourceClient::new(verify_executor.clone());

    // Check stored values
    let sql_result = sql_verify
        .select("sql_stream", "users", &["name", "level"], Some("id = 1"))
        .await?;

    if let Some(name) = sql_result.column_values("name").first() {
        println!("  User name (SQL): {}", name);
    }
    if let Some(level) = sql_result.column_values("level").first() {
        println!("  User level (SQL): {}", level);
    }

    if let Some(name) = kv_verify.get_string("kv_stream", "user:alice").await? {
        println!("  User name (KV): {}", name);
    }

    let balance = resource_verify
        .get_balance_integer("resource_stream", "alice")
        .await?;
    println!("  Alice's balance: {} coins", balance);

    if let Some(Value::Str(item)) = kv_verify.get("kv_stream", "inventory:alice:sword").await? {
        println!("  Inventory item: {}", item);
    }

    verify_executor.finish().await?;

    // Demonstrate abort scenario
    println!("\n--- Abort Scenario ---");
    let abort_executor = Arc::new(
        coordinator
            .begin_read_write(
                Duration::from_secs(60),
                vec![],
                "distributed_transaction_abort".to_string(),
            )
            .await?,
    );
    let kv_abort = KvClient::new(abort_executor.clone());

    kv_abort
        .put_string("kv_stream", "temp:data", "will_be_aborted")
        .await?;
    println!("  Put temporary data");

    println!("  Aborting transaction...");
    abort_executor.cancel().await?;
    println!("  ✓ Transaction aborted");

    // Verify the aborted data is not visible
    println!("\n--- Verify Aborted Data Not Visible ---");
    let check_executor = Arc::new(
        coordinator
            .begin_read_only(vec![], "distributed_transaction_check".to_string())
            .await?,
    );
    let kv_check = KvClient::new(check_executor.clone());

    match kv_check.get("kv_stream", "temp:data").await? {
        None => println!("  ✓ Aborted data not visible (temp:data = None)"),
        Some(value) => println!("  ❌ Unexpected: Found aborted data: {:?}", value),
    }

    check_executor.finish().await?;

    // Clean up
    println!("\n--- Cleanup ---");
    coordinator.stop().await;

    // Give processors time to finish
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Example Complete ===");
    Ok(())
}
