use proven_common::ProcessorType;
use proven_engine::{ConsensusGroupId, MockClient, MockEngine};
use proven_runner::Runner;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Create a test engine with a stream
async fn create_test_engine_with_stream(stream_name: &str) -> Arc<MockEngine> {
    let engine = Arc::new(MockEngine::new());
    engine.create_stream(stream_name.to_string()).unwrap();
    engine
}

#[tokio::test]
async fn test_basic_processor_request_ack_flow() {
    let engine = create_test_engine_with_stream("test-stream").await;

    let temp_dir1 = tempfile::tempdir().unwrap();
    let temp_dir2 = tempfile::tempdir().unwrap();

    // Create two runners (nodes)
    let runner1 = Runner::new(
        "node1",
        Arc::new(MockClient::new("node1".to_string(), engine.clone())),
        temp_dir1.path(),
    );
    let runner2 = Runner::new(
        "node2",
        Arc::new(MockClient::new("node2".to_string(), engine.clone())),
        temp_dir2.path(),
    );

    // Start both runners
    runner1.start().await.unwrap();
    runner2.start().await.unwrap();

    // Wait a bit for listeners to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request a processor from runner1
    let result = timeout(
        Duration::from_secs(2),
        runner1.ensure_processor("test-stream", ProcessorType::Kv, Duration::from_secs(60)),
    )
    .await;

    match result {
        Ok(Ok(info)) => {
            // Success! A node handled the request
            println!("Processor started on node: {}", info.node_id);
            assert!(info.node_id == "node1" || info.node_id == "node2");
            assert_eq!(info.stream, "test-stream");
        }
        Ok(Err(_)) | Err(_) => {
            panic!("Expected processor to start successfully");
        }
    }

    // Cleanup
    runner1.shutdown().await;
    runner2.shutdown().await;
}

#[tokio::test]
async fn test_direct_node_assignment() {
    let engine = Arc::new(MockEngine::new());

    // Create a stream in group 1
    engine.create_stream("group1-stream".to_string()).unwrap();

    // Create nodes in the same group
    let client1 = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let client2 = Arc::new(MockClient::new("node2".to_string(), engine.clone()));

    // Ensure both nodes are in group 1 (happens automatically in mock)
    let groups1 = client1.node_groups().await.unwrap();
    assert!(groups1.contains(&ConsensusGroupId(1)));

    let groups2 = client2.node_groups().await.unwrap();
    assert!(groups2.contains(&ConsensusGroupId(1)));

    let temp_dir1 = tempfile::tempdir().unwrap();
    let temp_dir2 = tempfile::tempdir().unwrap();

    // Create runners
    let runner1 = Runner::new("node1", client1.clone(), temp_dir1.path());
    let runner2 = Runner::new("node2", client2.clone(), temp_dir2.path());

    // Start both runners
    runner1.start().await.unwrap();
    runner2.start().await.unwrap();

    // Wait for listeners
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request a processor - it should be assigned to exactly one node
    let result = timeout(
        Duration::from_secs(2),
        runner1.ensure_processor("group1-stream", ProcessorType::Kv, Duration::from_secs(60)),
    )
    .await;

    match result {
        Ok(Ok(info)) => {
            // Should be assigned to either node1 or node2 (random)
            assert!(info.node_id == "node1" || info.node_id == "node2");
            println!("Processor assigned to: {}", info.node_id);
        }
        _ => {
            panic!("Expected processor to be assigned to one of the nodes");
        }
    }

    // Cleanup
    runner1.shutdown().await;
    runner2.shutdown().await;
}

#[tokio::test]
async fn test_heartbeat_includes_groups() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir().unwrap();
    let runner = Runner::new("node1", client.clone(), temp_dir.path());

    runner.start().await.unwrap();

    // Subscribe to heartbeats
    let mut sub = client.subscribe("runner.heartbeat", None).await.unwrap();

    // Wait for a heartbeat
    let result = timeout(Duration::from_secs(10), sub.recv()).await;

    if let Ok(Some(message)) = result {
        let heartbeat: proven_runner::Heartbeat = serde_json::from_slice(&message.body).unwrap();

        assert_eq!(heartbeat.node_id, "node1");
        // Should include group membership
        assert!(!heartbeat.raft_groups.is_empty());
        assert!(heartbeat.raft_groups.contains(&"group-1".to_string()));
    } else {
        panic!("Expected to receive a heartbeat");
    }

    runner.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_processor_requests() {
    let engine = Arc::new(MockEngine::new());

    let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir().unwrap();
    let runner = Arc::new(Runner::new("node1", client.clone(), temp_dir.path()));

    runner.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Make multiple concurrent requests
    let mut handles = vec![];
    for i in 0..5 {
        let runner_clone = runner.clone();
        let stream_name = format!("stream-{}", i);
        engine.create_stream(stream_name.clone()).unwrap();

        let handle = tokio::spawn(async move {
            runner_clone
                .ensure_processor(&stream_name, ProcessorType::Kv, Duration::from_secs(30))
                .await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(_)) = handle.await {
            success_count += 1;
        }
    }

    // At least some should succeed (since node1 is in the group for all streams)
    assert!(
        success_count > 0,
        "Expected at least some processors to start"
    );

    runner.shutdown().await;
}

#[tokio::test]
async fn test_idle_processor_shutdown() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir().unwrap();

    // Create runner with 2 second idle timeout
    let runner = Runner::new("node1", client.clone(), temp_dir.path())
        .with_idle_shutdown(Duration::from_secs(2));

    runner.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a stream and request a processor with short guarantee (3 seconds)
    engine.create_stream("idle-stream".to_string()).unwrap();
    let result = runner
        .ensure_processor("idle-stream", ProcessorType::Kv, Duration::from_secs(3))
        .await;

    assert!(result.is_ok());
    println!("Processor started for idle-stream");

    // Verify processor is running
    assert!(runner.has_processor("idle-stream"));

    // Wait for guarantee to expire (3 seconds) + idle timeout (2 seconds) + checker interval (10 seconds max)
    // To be safe, we'll wait a bit longer
    tokio::time::sleep(Duration::from_secs(6)).await;

    // Processor should still be in map because guarantee just expired
    assert!(runner.has_processor("idle-stream"));

    // Wait for idle checker to run (runs every 10 seconds, so wait up to 12 seconds total)
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Now processor should be shut down and removed
    assert!(
        !runner.has_processor("idle-stream"),
        "Processor should have been shut down due to idle timeout"
    );

    println!("Processor successfully shut down after idle period");

    runner.shutdown().await;
}

#[tokio::test]
async fn test_active_processor_not_shutdown() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir().unwrap();

    // Create runner with 2 second idle timeout
    let runner = Arc::new(
        Runner::new("node1", client.clone(), temp_dir.path())
            .with_idle_shutdown(Duration::from_secs(2)),
    );

    runner.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a stream and request a processor with short guarantee (3 seconds)
    engine.create_stream("active-stream".to_string()).unwrap();
    let result = runner
        .ensure_processor("active-stream", ProcessorType::Kv, Duration::from_secs(3))
        .await;

    assert!(result.is_ok());
    println!("Processor started for active-stream");

    // Spawn a task that continuously sends messages to keep the processor active
    let client_clone = runner.client().clone();
    let keep_alive_task = tokio::spawn(async move {
        use proven_protocol::OrderedMessage;
        for i in 0..20 {
            // Send a noop message every 500ms for 10 seconds
            tokio::time::sleep(Duration::from_millis(500)).await;
            let noop_msg = OrderedMessage::<proven_kv::KvOperation>::Noop;
            let _ = client_clone
                .publish_to_stream("active-stream".to_string(), vec![noop_msg.into_message()])
                .await;
            println!("Sent keep-alive message {}", i);
        }
    });

    // Wait for guarantee to expire + idle timeout + checker cycles
    tokio::time::sleep(Duration::from_secs(14)).await;

    // Processor should STILL be running because it's been receiving messages
    assert!(
        runner.has_processor("active-stream"),
        "Active processor should NOT be shut down"
    );

    println!("Active processor correctly stayed alive");

    // Stop sending messages
    keep_alive_task.abort();

    runner.shutdown().await;
}

#[tokio::test]
async fn test_idle_shutdown_respects_guarantee() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
    let temp_dir = tempfile::tempdir().unwrap();

    // Create runner with 2 second idle timeout
    let runner = Runner::new("node1", client.clone(), temp_dir.path())
        .with_idle_shutdown(Duration::from_secs(2));

    runner.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a stream and request a processor with LONG guarantee (20 seconds)
    engine
        .create_stream("guaranteed-stream".to_string())
        .unwrap();
    let result = runner
        .ensure_processor(
            "guaranteed-stream",
            ProcessorType::Kv,
            Duration::from_secs(20),
        )
        .await;

    assert!(result.is_ok());
    println!("Processor started for guaranteed-stream with 20s guarantee");

    // Wait for idle timeout to pass (2 seconds) + checker cycles
    tokio::time::sleep(Duration::from_secs(14)).await;

    // Processor should STILL be running because guarantee hasn't expired
    assert!(
        runner.has_processor("guaranteed-stream"),
        "Processor should NOT be shut down while guarantee is active, even if idle"
    );

    println!("Processor correctly stayed alive during guarantee period despite being idle");

    runner.shutdown().await;
}
