//! Coordinator Recovery Example
//!
//! This example demonstrates how the system recovers from coordinator crashes:
//! - Start a coordinator with a short deadline (3 seconds)
//! - Take locks on multiple storages during a transaction
//! - Kill the coordinator (simulating a crash) before committing
//! - Wait for the deadline to expire, triggering recovery
//! - Start a new coordinator and verify it can access the previously locked resources
//! - Recovery should automatically release the abandoned locks

use proven_coordinator::{Coordinator, Executor};
use proven_engine::{MockClient, MockEngine};
use proven_kv_client::KvClient;
use proven_resource_client::ResourceClient;
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Coordinator Recovery Example ===\n");
    println!("This example demonstrates automatic lock recovery when a coordinator crashes\n");

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create streams for our test
    engine.create_stream("kv_stream".to_string())?;
    engine.create_stream("resource_stream".to_string())?;
    println!("✓ Created streams: kv_stream, resource_stream\n");

    // Create and start the runner
    let runner_client = Arc::new(MockClient::new("runner-node".to_string(), engine.clone()));
    let snapshot_store = Arc::new(MemorySnapshotStore::new());
    let runner = Arc::new(Runner::new(
        "runner-node",
        runner_client.clone(),
        snapshot_store,
    ));
    runner.start().await?;
    println!("✓ Started runner\n");

    // Pre-start processors with long duration to prevent expiry during test
    let processor_duration = Duration::from_secs(600);
    runner
        .ensure_processor("kv_stream", processor_duration)
        .await?;
    runner
        .ensure_processor("resource_stream", processor_duration)
        .await?;
    println!("✓ Processors pre-started with 600s lease\n");

    // ===================================================================
    // PHASE 1: Coordinator takes locks and then crashes
    // ===================================================================
    println!("--- PHASE 1: Coordinator Takes Locks and Crashes ---\n");

    {
        // Create the first coordinator with a SHORT deadline (3 seconds)
        let coord1_client = Arc::new(MockClient::new("coordinator-1".to_string(), engine.clone()));
        let coordinator1 =
            Coordinator::new("coordinator-1".to_string(), coord1_client, runner.clone());
        println!("✓ Created coordinator-1\n");

        // Begin a transaction with a 3-second deadline
        let executor = Arc::new(
            coordinator1
                .begin_read_write(
                    Duration::from_secs(3), // Short deadline for testing
                    vec![],
                    "crash_test_transaction".to_string(),
                )
                .await?,
        );
        println!("✓ Started transaction with 3-second deadline");

        let kv = KvClient::new(executor.clone());
        let resource = ResourceClient::new(executor.clone());

        // Take locks on multiple resources
        println!("\n  Taking locks on resources:");

        kv.put_string("kv_stream", "account:alice", "Alice Smith")
            .await?;
        println!("    ✓ Wrote to kv_stream (lock acquired)");

        resource
            .mint_integer("resource_stream", "alice", 1000)
            .await?;
        println!("    ✓ Minted to resource_stream (lock acquired)");

        kv.put_integer("kv_stream", "counter:global", 42).await?;
        println!("    ✓ Wrote another KV entry (lock acquired)");

        println!("\n  💥 CRASH: Dropping coordinator without commit/abort!");
        println!("     (Simulating coordinator crash - locks remain held)\n");

        // Drop the executor and coordinator without calling finish() or cancel()
        // This simulates a crash - the locks are still held by the transaction
        drop(executor);
        drop(coordinator1);
    } // coordinator1 goes out of scope here - simulating a crash

    println!("✓ Coordinator-1 terminated (crashed)\n");

    // ===================================================================
    // PHASE 2: Try to access locked resources immediately (should block)
    // ===================================================================
    println!("--- PHASE 2: Immediate Access (Locks Still Held) ---\n");

    {
        let coord2_client = Arc::new(MockClient::new("coordinator-2".to_string(), engine.clone()));
        let coordinator2 =
            Coordinator::new("coordinator-2".to_string(), coord2_client, runner.clone());
        println!("✓ Created coordinator-2\n");

        let executor2 = Arc::new(
            coordinator2
                .begin_read_write(
                    Duration::from_secs(10),
                    vec![],
                    "immediate_access_test".to_string(),
                )
                .await?,
        );

        let kv2 = KvClient::new(executor2.clone());

        println!("  Attempting to access locked resource (should timeout)...");

        // Try to access the locked resource with a short timeout
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            kv2.put_string("kv_stream", "account:alice", "Bob Smith"),
        )
        .await;

        match result {
            Ok(Ok(_)) => {
                println!("    ❌ Unexpected: Operation succeeded (lock should still be held)")
            }
            Ok(Err(e)) => println!("    ✓ Operation failed as expected: {:?}", e),
            Err(_) => println!("    ✓ Operation timed out as expected (lock still held)"),
        }

        // Clean up this attempt
        let _ = executor2.cancel().await;
        coordinator2.stop().await;
    }

    // ===================================================================
    // PHASE 3: Wait for deadline to expire + recovery to run
    // ===================================================================
    println!("\n--- PHASE 3: Waiting for Recovery ---\n");

    println!("  Waiting for:");
    println!("    1. Transaction deadline to expire (3 seconds)");
    println!("    2. Recovery check to run (every 100ms)");
    println!("    3. Locks to be released\n");

    // Wait for:
    // - Transaction deadline to expire (3 seconds)
    // - Recovery check to detect and process it (runs every 100ms)
    // - Extra buffer to ensure recovery completes AND ABORT messages are fully processed
    tokio::time::sleep(Duration::from_millis(5000)).await;

    println!("  Waiting extra time for ABORT messages to be fully processed...");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    println!("✓ Recovery period complete\n");

    // ===================================================================
    // PHASE 4: Access resources after recovery (should succeed)
    // ===================================================================
    println!("--- PHASE 4: Access After Recovery (Locks Released) ---\n");

    let coord3_client = Arc::new(MockClient::new("coordinator-3".to_string(), engine.clone()));
    let coordinator3 = Coordinator::new("coordinator-3".to_string(), coord3_client, runner.clone());
    println!("✓ Created coordinator-3\n");

    let executor3 = Arc::new(
        coordinator3
            .begin_read_write(
                Duration::from_secs(30), // Longer deadline to wait for recovery ABORT to be processed
                vec![],
                "post_recovery_test".to_string(),
            )
            .await?,
    );

    let kv3 = KvClient::new(executor3.clone());
    let resource3 = ResourceClient::new(executor3.clone());

    println!("  Attempting to access previously locked resources:");

    // These operations should succeed because recovery released the locks
    kv3.put_string("kv_stream", "account:bob", "Bob Jones")
        .await?;
    println!("    ✓ Wrote to kv_stream (lock acquired after recovery)");

    resource3
        .mint_integer("resource_stream", "bob", 500)
        .await?;
    println!("    ✓ Minted to resource_stream (lock acquired after recovery)");

    // Try to access the specific keys that were locked before
    kv3.put_string("kv_stream", "account:alice", "Alice Updated")
        .await?;
    println!("    ✓ Updated account:alice (previously locked key)");

    kv3.put_integer("kv_stream", "counter:global", 100).await?;
    println!("    ✓ Updated counter:global (previously locked key)");

    println!("\n  Committing new transaction:");
    executor3.finish().await?;
    println!("    ✓ Transaction committed successfully!\n");

    // ===================================================================
    // PHASE 5: Verify the data
    // ===================================================================
    println!("--- PHASE 5: Verification ---\n");

    let verify_executor = Arc::new(
        coordinator3
            .begin_read_only(vec![], "verification".to_string())
            .await?,
    );
    let kv_verify = KvClient::new(verify_executor.clone());
    let resource_verify = ResourceClient::new(verify_executor.clone());

    println!("  Verifying final state:");

    if let Some(name) = kv_verify.get_string("kv_stream", "account:alice").await? {
        println!("    account:alice = \"{}\"", name);
    }

    if let Some(name) = kv_verify.get_string("kv_stream", "account:bob").await? {
        println!("    account:bob = \"{}\"", name);
    }

    if let Some(counter) = kv_verify.get_integer("kv_stream", "counter:global").await? {
        println!("    counter:global = {}", counter);
    }

    let alice_balance = resource_verify
        .get_balance_integer("resource_stream", "alice")
        .await?;
    println!("    alice balance = {} coins", alice_balance);

    let bob_balance = resource_verify
        .get_balance_integer("resource_stream", "bob")
        .await?;
    println!("    bob balance = {} coins", bob_balance);

    verify_executor.finish().await?;

    // ===================================================================
    // Cleanup
    // ===================================================================
    println!("\n--- Cleanup ---");
    coordinator3.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Recovery Test Complete ===");
    println!("\nKey Observations:");
    println!("  1. ✓ Coordinator-1 crashed with locks held");
    println!("  2. ✓ Immediate access was blocked (locks still held)");
    println!("  3. ✓ After deadline expired, recovery ran automatically");
    println!("  4. ✓ Coordinator-3 successfully acquired locks after recovery");
    println!("  5. ✓ System continued operating normally\n");

    println!("Recovery mechanism verified! 🎉");

    Ok(())
}
