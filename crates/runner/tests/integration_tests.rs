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

    // Create two runners (nodes)
    let runner1 = Runner::new(
        "node1",
        Arc::new(MockClient::new("node1".to_string(), engine.clone())),
    );
    let runner2 = Runner::new(
        "node2",
        Arc::new(MockClient::new("node2".to_string(), engine.clone())),
    );

    // Start both runners
    runner1.start().await.unwrap();
    runner2.start().await.unwrap();

    // Wait a bit for listeners to be ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request a processor from runner1
    let result = timeout(
        Duration::from_secs(2),
        runner1.ensure_processor("test-stream", Duration::from_secs(60)),
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

    // Create runners
    let runner1 = Runner::new("node1", client1.clone());
    let runner2 = Runner::new("node2", client2.clone());

    // Start both runners
    runner1.start().await.unwrap();
    runner2.start().await.unwrap();

    // Wait for listeners
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Request a processor - it should be assigned to exactly one node
    let result = timeout(
        Duration::from_secs(2),
        runner1.ensure_processor("group1-stream", Duration::from_secs(60)),
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
    let runner = Runner::new("node1", client.clone());

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
    let runner = Arc::new(Runner::new("node1", client.clone()));

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
                .ensure_processor(&stream_name, Duration::from_secs(30))
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
