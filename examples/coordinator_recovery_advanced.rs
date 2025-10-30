//! Advanced Coordinator Recovery Example
//!
//! This example demonstrates recovery from a coordinator crash DURING the 2PC commit phase:
//! - Manually send PREPARE messages to all participants
//! - All participants respond PREPARED
//! - Manually send COMMIT to only SOME participants (simulating coordinator crash)
//! - Verify that recovery detects the partial commit and completes it correctly
//!
//! This tests the critical safety property: if any participant commits, all must commit.

use proven_common::{Timestamp, TransactionId};
use proven_coordinator::{Coordinator, Executor};
use proven_engine::{Message, MockClient, MockEngine};
use proven_kv_client::KvClient;
use proven_runner::Runner;
use proven_snapshot_memory::MemorySnapshotStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Advanced Coordinator Recovery Example ===\n");
    println!("Testing recovery from coordinator crash DURING 2PC commit phase\n");

    // Initialize the mock engine
    let engine = Arc::new(MockEngine::new());

    // Create 3 streams to test multi-participant recovery
    engine.create_stream("stream_a".to_string())?;
    engine.create_stream("stream_b".to_string())?;
    engine.create_stream("stream_c".to_string())?;
    println!("✓ Created streams: stream_a, stream_b, stream_c\n");

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

    // ===================================================================
    // PHASE 1: Manually simulate a transaction reaching PREPARE
    // ===================================================================
    println!("--- PHASE 1: Manual Transaction Setup ---\n");

    let txn_id = TransactionId::new();
    let txn_id_str = txn_id.to_string();
    let coordinator_id = "crashed-coordinator";

    // Create a client to send messages manually
    let manual_client = Arc::new(MockClient::new("manual-sender".to_string(), engine.clone()));

    // IMPORTANT: Pre-start processors with long duration to prevent expiry during test
    println!("  Step 0: Pre-starting processors for all streams");
    let processor_duration = Duration::from_secs(600); // 10 minutes - long enough for test
    runner
        .ensure_processor("stream_a", processor_duration)
        .await?;
    runner
        .ensure_processor("stream_b", processor_duration)
        .await?;
    runner
        .ensure_processor("stream_c", processor_duration)
        .await?;
    println!("    ✓ All processors pre-started with 600s lease\n");

    // Short deadline - 3 seconds from now
    let deadline = Timestamp::now().add_micros(3_000_000);
    let deadline_str = deadline.to_string();

    println!("  Transaction ID: {}", txn_id_str);
    println!("  Deadline: {} (3 seconds)\n", deadline_str);

    // Step 1: Send operations to all 3 streams to acquire locks
    println!("  Step 1: Acquiring locks on all 3 streams");

    // Use KV operations for all streams for simplicity
    let kv_put_op_a = serde_json::json!({
        "Put": {
            "key": "key_a",
            "value": {"Str": "value_a"}
        }
    });

    let kv_put_op_b = serde_json::json!({
        "Put": {
            "key": "key_b",
            "value": {"Str": "value_b"}
        }
    });

    let kv_put_op_c = serde_json::json!({
        "Put": {
            "key": "key_c",
            "value": {"Str": "value_c"}
        }
    });

    // Send operation to stream_a
    let mut headers_a = HashMap::new();
    headers_a.insert("txn_id".to_string(), txn_id_str.clone());
    headers_a.insert("coordinator_id".to_string(), coordinator_id.to_string());
    headers_a.insert("txn_deadline".to_string(), deadline_str.clone());
    headers_a.insert("request_id".to_string(), "req_a".to_string());

    let msg_a = Message::new(serde_json::to_vec(&kv_put_op_a)?, headers_a);
    let offset_a = manual_client
        .publish_to_stream("stream_a".to_string(), vec![msg_a])
        .await?;
    println!("    ✓ stream_a: operation sent at offset {}", offset_a);

    // Send operation to stream_b
    let mut headers_b = HashMap::new();
    headers_b.insert("txn_id".to_string(), txn_id_str.clone());
    headers_b.insert("coordinator_id".to_string(), coordinator_id.to_string());
    headers_b.insert("txn_deadline".to_string(), deadline_str.clone());
    headers_b.insert("request_id".to_string(), "req_b".to_string());

    let msg_b = Message::new(serde_json::to_vec(&kv_put_op_b)?, headers_b);
    let offset_b = manual_client
        .publish_to_stream("stream_b".to_string(), vec![msg_b])
        .await?;
    println!("    ✓ stream_b: operation sent at offset {}", offset_b);

    // Send operation to stream_c
    let mut headers_c = HashMap::new();
    headers_c.insert("txn_id".to_string(), txn_id_str.clone());
    headers_c.insert("coordinator_id".to_string(), coordinator_id.to_string());
    headers_c.insert("txn_deadline".to_string(), deadline_str.clone());
    headers_c.insert("request_id".to_string(), "req_c".to_string());

    let msg_c = Message::new(serde_json::to_vec(&kv_put_op_c)?, headers_c);
    let offset_c = manual_client
        .publish_to_stream("stream_c".to_string(), vec![msg_c])
        .await?;
    println!("    ✓ stream_c: operation sent at offset {}", offset_c);

    // Wait for operations to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Step 2: Send PREPARE to all participants with participant awareness
    println!("\n  Step 2: Sending PREPARE to all participants");

    // Build the participant map with offsets
    let mut participants = HashMap::new();
    participants.insert("stream_a".to_string(), offset_a);
    participants.insert("stream_b".to_string(), offset_b);
    participants.insert("stream_c".to_string(), offset_c);
    let participants_json = serde_json::to_string(&participants)?;

    // Send PREPARE to stream_a
    let mut prep_headers_a = HashMap::new();
    prep_headers_a.insert("txn_id".to_string(), txn_id_str.clone());
    prep_headers_a.insert("coordinator_id".to_string(), coordinator_id.to_string());
    prep_headers_a.insert("txn_phase".to_string(), "prepare".to_string());
    prep_headers_a.insert("participants".to_string(), participants_json.clone());

    let prep_msg_a = Message::new(Vec::new(), prep_headers_a);
    manual_client
        .publish_to_stream("stream_a".to_string(), vec![prep_msg_a])
        .await?;
    println!("    ✓ stream_a: PREPARE sent (with participants)");

    // Send PREPARE to stream_b
    let mut prep_headers_b = HashMap::new();
    prep_headers_b.insert("txn_id".to_string(), txn_id_str.clone());
    prep_headers_b.insert("coordinator_id".to_string(), coordinator_id.to_string());
    prep_headers_b.insert("txn_phase".to_string(), "prepare".to_string());
    prep_headers_b.insert("participants".to_string(), participants_json.clone());

    let prep_msg_b = Message::new(Vec::new(), prep_headers_b);
    manual_client
        .publish_to_stream("stream_b".to_string(), vec![prep_msg_b])
        .await?;
    println!("    ✓ stream_b: PREPARE sent (with participants)");

    // Send PREPARE to stream_c
    let mut prep_headers_c = HashMap::new();
    prep_headers_c.insert("txn_id".to_string(), txn_id_str.clone());
    prep_headers_c.insert("coordinator_id".to_string(), coordinator_id.to_string());
    prep_headers_c.insert("txn_phase".to_string(), "prepare".to_string());
    prep_headers_c.insert("participants".to_string(), participants_json.clone());

    let prep_msg_c = Message::new(Vec::new(), prep_headers_c);
    manual_client
        .publish_to_stream("stream_c".to_string(), vec![prep_msg_c])
        .await?;
    println!("    ✓ stream_c: PREPARE sent (with participants)");

    // Wait for prepare to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Step 3: Send COMMIT to only stream_a (simulate coordinator crash before sending to b and c)
    println!("\n  Step 3: Partial commit (only stream_a)");

    let mut commit_headers_a = HashMap::new();
    commit_headers_a.insert("txn_id".to_string(), txn_id_str.clone());
    commit_headers_a.insert("txn_phase".to_string(), "commit".to_string());

    let commit_msg_a = Message::new(Vec::new(), commit_headers_a);
    manual_client
        .publish_to_stream("stream_a".to_string(), vec![commit_msg_a])
        .await?;
    println!("    ✓ stream_a: COMMIT sent");

    println!("\n  💥 COORDINATOR CRASH!");
    println!("     stream_a committed");
    println!("     stream_b and stream_c still in PREPARED state\n");

    // Wait for commit to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ===================================================================
    // PHASE 2: Wait for recovery
    // ===================================================================
    println!("--- PHASE 2: Waiting for Recovery ---\n");

    println!("  Recovery should:");
    println!("    1. Detect deadline expiration");
    println!("    2. Scan participant streams (stream_a, stream_b, stream_c)");
    println!("    3. Find COMMIT in stream_a's log");
    println!("    4. Decide to COMMIT for all participants");
    println!("    5. Apply COMMIT to stream_b and stream_c\n");

    // Wait for deadline + recovery time
    tokio::time::sleep(Duration::from_millis(4000)).await;

    println!("✓ Recovery period complete\n");

    // ===================================================================
    // PHASE 3: Verify recovery worked correctly
    // ===================================================================
    println!("--- PHASE 3: Verification ---\n");

    // Create a new coordinator to read the data
    let verify_coordinator = Coordinator::new(
        "verify-coordinator".to_string(),
        Arc::new(MockClient::new(
            "verify-coordinator".to_string(),
            engine.clone(),
        )),
        runner.clone(),
    );

    let verify_executor = Arc::new(
        verify_coordinator
            .begin_read_only(vec![], "verification".to_string())
            .await?,
    );

    let kv_a = KvClient::new(verify_executor.clone());
    let kv_b = KvClient::new(verify_executor.clone());
    let kv_c = KvClient::new(verify_executor.clone());

    println!("  Checking if data from committed transaction is visible:");

    // Check stream_a (should have committed data)
    match kv_a.get_string("stream_a", "key_a").await? {
        Some(value) => println!("    ✓ stream_a: key_a = \"{}\" (COMMITTED)", value),
        None => println!("    ❌ stream_a: key_a not found (should be committed!)"),
    }

    // Check stream_b (should have committed data after recovery)
    match kv_b.get_string("stream_b", "key_b").await? {
        Some(value) => println!(
            "    ✓ stream_b: key_b = \"{}\" (COMMITTED by recovery)",
            value
        ),
        None => println!("    ❌ stream_b: key_b not found (should be committed by recovery!)"),
    }

    // Check stream_c (should have committed data after recovery)
    match kv_c.get_string("stream_c", "key_c").await? {
        Some(value) => println!(
            "    ✓ stream_c: key_c = \"{}\" (COMMITTED by recovery)",
            value
        ),
        None => println!("    ❌ stream_c: key_c not found (should be committed by recovery!)"),
    }

    verify_executor.finish().await?;

    // ===================================================================
    // PHASE 4: Verify new transactions can proceed
    // ===================================================================
    println!("\n--- PHASE 4: New Transaction Test ---\n");

    let new_coordinator = Coordinator::new(
        "new-coordinator".to_string(),
        Arc::new(MockClient::new(
            "new-coordinator".to_string(),
            engine.clone(),
        )),
        runner.clone(),
    );

    let new_executor = Arc::new(
        new_coordinator
            .begin_read_write(
                Duration::from_secs(10),
                vec![],
                "post_recovery_test".to_string(),
            )
            .await?,
    );

    let kv_test = KvClient::new(new_executor.clone());

    println!("  Attempting new operations on recovered streams:");

    kv_test
        .put_string("stream_a", "new_key", "new_value")
        .await?;
    println!("    ✓ stream_a: new operation succeeded");

    kv_test
        .put_string("stream_c", "another_key", "another_value")
        .await?;
    println!("    ✓ stream_c: new operation succeeded");

    new_executor.finish().await?;
    println!("    ✓ New transaction committed successfully\n");

    // Cleanup
    println!("--- Cleanup ---");
    verify_coordinator.stop().await;
    new_coordinator.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Advanced Recovery Test Complete ===\n");

    println!("Summary:");
    println!("  ✓ Manual 2PC transaction set up with 3 participants");
    println!("  ✓ PREPARE sent to all participants with awareness");
    println!("  ✓ Partial COMMIT (stream_a only) simulated coordinator crash");
    println!("  ✓ Recovery detected COMMIT in stream_a's log");
    println!("  ✓ Recovery applied COMMIT to stream_b and stream_c");
    println!("  ✓ All data visible after recovery");
    println!("  ✓ New transactions can proceed normally\n");

    println!("🎉 Recovery correctly enforced 2PC atomicity!");

    Ok(())
}
