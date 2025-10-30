//! Generic tests for wound-wait deadlock prevention
//!
//! These tests verify the deterministic behavior of the wound-wait
//! mechanism in the generic stream processor.

#[cfg(test)]
mod tests {
    use crate::processor::StreamProcessor;
    use crate::test_utils::{LockOp, LockResponse, TestEngine};
    use proven_common::{Timestamp, TransactionId};
    use proven_engine::{Message, MockClient, MockEngine};
    use proven_snapshot_memory::MemorySnapshotStore;
    use std::sync::Arc;
    use std::time::Duration;

    /// Helper to generate deterministic transaction ID strings for tests
    /// Uses well-known UUIDs that maintain lexicographic ordering based on input
    /// Smaller timestamp_ms = older transaction (smaller UUID)
    fn txn_id(timestamp_ms: u64) -> String {
        // Create a UUIDv7-like string where the timestamp portion corresponds to input
        // Format: xxxxxxxx-xxxx-7xxx-xxxx-xxxxxxxxxxxx (standard UUID format)
        // We encode timestamp_ms in the first 48 bits to maintain ordering

        // UUIDv7 format: timestamp_ms (48 bits) split into: 32 bits - 16 bits
        let high = ((timestamp_ms >> 16) & 0xFFFFFFFF) as u32;
        let mid = (timestamp_ms & 0xFFFF) as u16;

        format!("{:08x}-{:04x}-7000-8000-000000000000", high, mid)
    }

    fn create_message(
        operation: Option<LockOp>,
        txn_id: &str,
        coordinator_id: &str,
        txn_phase: Option<&str>,
    ) -> Message {
        use proven_protocol::messages::TransactionMode;
        use proven_protocol::{
            CoordinatorMessage, OperationMessage, TransactionControlMessage, TransactionPhase,
        };

        let txn_id = TransactionId::parse(txn_id).unwrap();

        // Add deadline (far in the future to avoid timeout issues in tests)
        let now = Timestamp::now();
        let deadline = now.add_micros(60_000_000); // 60 seconds in the future

        if let Some(phase_str) = txn_phase {
            // Control message (prepare/commit/abort)
            let phase = TransactionPhase::parse(phase_str).unwrap();
            CoordinatorMessage::Control(TransactionControlMessage {
                txn_id,
                phase,
                coordinator_id: Some(coordinator_id.to_string()),
                request_id: Some(format!("req-{}", txn_id)),
                participants: None,
            })
            .into_message()
        } else {
            // Operation message
            let body = if let Some(op) = operation {
                serde_json::to_vec(&op).unwrap()
            } else {
                Vec::new()
            };

            CoordinatorMessage::Operation(OperationMessage {
                txn_id,
                coordinator_id: coordinator_id.to_string(),
                request_id: format!("req-{}", txn_id),
                txn_deadline: Some(deadline),
                participants: None,
                mode: TransactionMode::ReadWrite,
                operation: body,
            })
            .into_message()
        }
    }

    #[tokio::test]
    async fn test_older_wounds_younger() {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test".to_string(), engine.clone()));

        // Create stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Subscribe to responses
        let mut younger_responses = client
            .subscribe("coordinator.younger.response", None)
            .await
            .unwrap();
        let mut older_responses = client
            .subscribe("coordinator.older.response", None)
            .await
            .unwrap();

        // Start processor first
        let test_engine = TestEngine::<LockOp, LockResponse>::new();
        let snapshot_store = Arc::new(MemorySnapshotStore::new());
        let processor = StreamProcessor::new(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Give processor time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Younger transaction (3000) acquires lock first
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older transaction (2000) tries to acquire same lock
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000),
            "older",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Younger should succeed
        let younger_msg = younger_responses.recv().await.unwrap();
        assert!(
            !younger_msg.body.is_empty(),
            "Younger should get success response"
        );

        // Check that younger gets wounded notification (via header)
        let wounded_msg = younger_responses.recv().await.unwrap();
        assert_eq!(
            wounded_msg.headers.get("status"),
            Some(&"wounded".to_string()),
            "Younger should be wounded"
        );

        // Older should succeed
        let older_msg = older_responses.recv().await.unwrap();
        assert!(
            !older_msg.body.is_empty(),
            "Older should succeed after wounding"
        );

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_younger_defers_to_older() {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test".to_string(), engine.clone()));

        // Create stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Subscribe to responses
        let mut older_responses = client
            .subscribe("coordinator.older.response", None)
            .await
            .unwrap();
        let mut younger_responses = client
            .subscribe("coordinator.younger.response", None)
            .await
            .unwrap();

        // Start processor first
        let test_engine = TestEngine::<LockOp, LockResponse>::new();
        let snapshot_store = Arc::new(MemorySnapshotStore::new());
        let processor = StreamProcessor::new(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Give processor time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Older transaction (2000) acquires lock first
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000),
            "older",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Younger transaction (3000) tries - should defer
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Commit older to release lock
        let msg = create_message(None, &txn_id(2000), "older", Some("commit"));
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older should succeed
        let older_msg = older_responses.recv().await.unwrap();
        assert!(!older_msg.body.is_empty(), "Older should acquire lock");

        // Give processor time to process commit and retry deferred operation
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Younger transaction is deferred internally (no deferred message sent anymore)
        // It will be automatically retried after older commits and should succeed
        let younger_msg = tokio::time::timeout(Duration::from_secs(2), younger_responses.recv())
            .await
            .expect("Timeout waiting for younger response - deferred operation may not have been retried")
            .unwrap();
        assert!(
            !younger_msg.body.is_empty(),
            "Younger should succeed after retry"
        );

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_multi_level_wound_chain() {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test".to_string(), engine.clone()));

        // Create stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Subscribe to responses
        let mut oldest_responses = client
            .subscribe("coordinator.oldest.response", None)
            .await
            .unwrap();

        // Start processor first
        let test_engine = TestEngine::<LockOp, LockResponse>::new();
        let snapshot_store = Arc::new(MemorySnapshotStore::new());
        let processor = StreamProcessor::new(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Give processor time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Youngest (4000) gets lock
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(4000),
            "youngest",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Middle (3000) tries - should defer
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000),
            "middle",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Oldest (2000) tries - should wound youngest
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000),
            "oldest",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Oldest should succeed
        let oldest_msg = oldest_responses.recv().await.unwrap();
        assert!(
            !oldest_msg.body.is_empty(),
            "Oldest should succeed after wounding"
        );

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_wound_preserves_determinism() {
        // Run the same sequence twice - should get identical results
        let results1 = run_wound_sequence().await;
        let results2 = run_wound_sequence().await;

        assert_eq!(
            results1.len(),
            results2.len(),
            "Should have same number of results"
        );

        for (i, (r1, r2)) in results1.iter().zip(results2.iter()).enumerate() {
            assert_eq!(r1.0, r2.0, "Coordinator mismatch at position {}", i);
            assert_eq!(r1.1, r2.1, "Status mismatch at position {}", i);
        }
    }

    async fn run_wound_sequence() -> Vec<(String, String)> {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test".to_string(), engine.clone()));

        // Create stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        let mut results = Vec::new();

        // Subscribe to all coordinators
        let mut coord1_resp = client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();
        let mut coord2_resp = client
            .subscribe("coordinator.coord2.response", None)
            .await
            .unwrap();
        let mut coord3_resp = client
            .subscribe("coordinator.coord3.response", None)
            .await
            .unwrap();

        // Fixed sequence of operations
        let operations = vec![
            (txn_id(3000), "coord3", "resource1"),
            (txn_id(2000), "coord2", "resource1"),
            (txn_id(4000), "coord1", "resource1"),
        ];

        // Publish all operations to stream
        for (txn_id_str, coord_id, resource) in &operations {
            let msg = create_message(
                Some(LockOp::Lock {
                    resource: resource.to_string(),
                }),
                txn_id_str,
                coord_id,
                None,
            );
            client
                .publish_to_stream("test-stream".to_string(), vec![msg])
                .await
                .unwrap();
        }

        // Start processor
        let test_engine = TestEngine::<LockOp, LockResponse>::new();
        let snapshot_store = Arc::new(MemorySnapshotStore::new());
        let processor = StreamProcessor::new(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Give processor time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Process responses
        for (_txn_id_str, coord_id, _resource) in operations {
            // Collect responses
            let responses = match coord_id {
                "coord1" => &mut coord1_resp,
                "coord2" => &mut coord2_resp,
                "coord3" => &mut coord3_resp,
                _ => continue,
            };

            while let Some(response_msg) = responses.try_recv() {
                let status = response_msg
                    .headers
                    .get("status")
                    .unwrap_or(&"success".to_string())
                    .clone();
                results.push((coord_id.to_string(), status));
            }
        }

        // Give time for any delayed responses
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Collect remaining responses
        for (coord_id, responses) in [
            ("coord1", &mut coord1_resp),
            ("coord2", &mut coord2_resp),
            ("coord3", &mut coord3_resp),
        ] {
            while let Some(response_msg) = responses.try_recv() {
                let status = response_msg
                    .headers
                    .get("status")
                    .unwrap_or(&"success".to_string())
                    .clone();
                results.push((coord_id.to_string(), status));
            }
        }

        // Shutdown the processor
        let _ = shutdown_tx.send(());

        // Wait for processor to handle shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;

        results
    }

    #[tokio::test]
    async fn test_prepare_phase_with_wound() {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test".to_string(), engine.clone()));

        // Create stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Subscribe to responses
        let mut younger_responses = client
            .subscribe("coordinator.younger.response", None)
            .await
            .unwrap();

        // Start processor first
        let test_engine = TestEngine::<LockOp, LockResponse>::new();
        let snapshot_store = Arc::new(MemorySnapshotStore::new());
        let processor = StreamProcessor::new(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store,
        );

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Give processor time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Younger transaction acquires lock
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older transaction wounds younger
        let msg = create_message(
            Some(LockOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000),
            "older",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Try to prepare younger transaction - should fail
        let msg = create_message(None, &txn_id(3000), "younger", Some("prepare"));
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Younger should succeed
        let _younger_msg = younger_responses.recv().await.unwrap();

        // Younger should be wounded
        let _wounded_msg = younger_responses.recv().await.unwrap();

        // Should get wounded response
        let prepare_response = younger_responses.recv().await.unwrap();
        assert_eq!(
            prepare_response.headers.get("status"),
            Some(&"wounded".to_string()),
            "Wounded transaction should not be able to prepare. Got error: {:?}",
            prepare_response.headers.get("error")
        );

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }
}
