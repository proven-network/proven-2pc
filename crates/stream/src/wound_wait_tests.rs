//! Generic tests for wound-wait deadlock prevention
//!
//! These tests verify the deterministic behavior of the wound-wait
//! mechanism in the generic stream processor.

#[cfg(test)]
mod tests {
    use crate::engine::{OperationResult, RetryOn, TransactionEngine};
    use crate::processor::StreamProcessor;
    use proven_engine::{Message, MockClient, MockEngine};
    use proven_hlc::{HlcTimestamp, NodeId};
    use proven_snapshot_memory::MemorySnapshotStore;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Helper to generate test timestamps
    #[allow(dead_code)]
    fn test_timestamp() -> HlcTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        HlcTimestamp::new(physical, 0, NodeId::new(1))
    }

    /// Helper to generate transaction ID strings in HLC format
    fn txn_id(physical: u64, logical: u32) -> String {
        HlcTimestamp::new(physical, logical, NodeId::new(1)).to_string()
    }

    // Test operation and response types
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestOp {
        Lock { resource: String },
        Read { resource: String },
    }

    impl proven_common::Operation for TestOp {
        fn operation_type(&self) -> proven_common::OperationType {
            match self {
                TestOp::Lock { .. } => proven_common::OperationType::Write,
                TestOp::Read { .. } => proven_common::OperationType::Read,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestResponse {
        Success { message: String },
        Value { data: String },
    }

    impl proven_common::Response for TestResponse {}

    // Test engine that simulates lock conflicts
    struct TestEngine {
        locks: HashMap<String, HlcTimestamp>,
    }

    impl TestEngine {
        fn new() -> Self {
            Self {
                locks: HashMap::new(),
            }
        }
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOp;
        type Response = TestResponse;

        fn read_at_timestamp(
            &mut self,
            operation: Self::Operation,
            _read_timestamp: HlcTimestamp,
        ) -> OperationResult<Self::Response> {
            match operation {
                TestOp::Lock { .. } => {
                    panic!("Lock operations not supported for read-only operations");
                }
                TestOp::Read { resource } => OperationResult::Complete(TestResponse::Value {
                    data: format!("Data from {}", resource),
                }),
            }
        }

        fn apply_operation(
            &mut self,
            operation: Self::Operation,
            txn_id: HlcTimestamp,
        ) -> OperationResult<Self::Response> {
            match operation {
                TestOp::Lock { resource } => {
                    if let Some(&holder) = self.locks.get(&resource)
                        && holder != txn_id
                    {
                        // Just report the conflict - stream processor handles wound-wait
                        return OperationResult::WouldBlock {
                            blockers: vec![crate::engine::BlockingInfo {
                                txn: holder,
                                retry_on: RetryOn::CommitOrAbort,
                            }],
                        };
                    }
                    self.locks.insert(resource, txn_id);
                    OperationResult::Complete(TestResponse::Success {
                        message: "Lock acquired".to_string(),
                    })
                }
                TestOp::Read { resource } => OperationResult::Complete(TestResponse::Value {
                    data: format!("Data from {}", resource),
                }),
            }
        }

        fn prepare(&mut self, _txn_id: HlcTimestamp) {
            // No-op for test engine
        }

        fn commit(&mut self, txn_id: HlcTimestamp) {
            // Release locks
            self.locks.retain(|_, &mut holder| holder != txn_id);
        }

        fn abort(&mut self, txn_id: HlcTimestamp) {
            // Release locks
            self.locks.retain(|_, &mut holder| holder != txn_id);
        }

        fn begin(&mut self, _txn_id: HlcTimestamp) {}

        fn is_transaction_active(&self, _txn_id: &HlcTimestamp) -> bool {
            true // For test purposes, always consider transactions active
        }

        fn engine_name(&self) -> &'static str {
            "test"
        }
    }

    fn create_message(
        operation: Option<TestOp>,
        txn_id: &str,
        coordinator_id: &str,
        txn_phase: Option<&str>,
    ) -> Message {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), coordinator_id.to_string());

        if let Some(phase) = txn_phase {
            headers.insert("txn_phase".to_string(), phase.to_string());
        }

        let body = if let Some(op) = operation {
            serde_json::to_vec(&op).unwrap()
        } else {
            Vec::new()
        };

        Message::new(body, headers)
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
        let test_engine = TestEngine::new();
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
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000000000, 0),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older transaction (2000) tries to acquire same lock
        let msg = create_message(
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000000000, 0),
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
        let test_engine = TestEngine::new();
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
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000000000, 0),
            "older",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Younger transaction (3000) tries - should defer
        let msg = create_message(
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000000000, 0),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Commit older to release lock
        let msg = create_message(None, &txn_id(2000000000, 0), "older", Some("commit"));
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older should succeed
        let older_msg = older_responses.recv().await.unwrap();
        assert!(!older_msg.body.is_empty(), "Older should acquire lock");

        // Younger transaction is deferred internally (no deferred message sent anymore)
        // It will be automatically retried after older commits and should succeed
        let younger_msg = younger_responses.recv().await.unwrap();
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
        let test_engine = TestEngine::new();
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
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(4000000000, 0),
            "youngest",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Middle (3000) tries - should defer
        let msg = create_message(
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000000000, 0),
            "middle",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Oldest (2000) tries - should wound youngest
        let msg = create_message(
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000000000, 0),
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
            (txn_id(3000000000, 0), "coord3", "resource1"),
            (txn_id(2000000000, 0), "coord2", "resource1"),
            (txn_id(4000000000, 0), "coord1", "resource1"),
        ];

        // Publish all operations to stream
        for (txn_id_str, coord_id, resource) in &operations {
            let msg = create_message(
                Some(TestOp::Lock {
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
        let test_engine = TestEngine::new();
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
        let test_engine = TestEngine::new();
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
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(3000000000, 0),
            "younger",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Older transaction wounds younger
        let msg = create_message(
            Some(TestOp::Lock {
                resource: "resource1".to_string(),
            }),
            &txn_id(2000000000, 0),
            "older",
            None,
        );
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Try to prepare younger transaction - should fail
        let msg = create_message(None, &txn_id(3000000000, 0), "younger", Some("prepare"));
        client
            .publish_to_stream("test-stream".to_string(), vec![msg])
            .await
            .unwrap();

        // Younger should succeed
        let _younger_msg = younger_responses.recv().await.unwrap();

        // Younger should be wounded
        let _wounded_msg = younger_responses.recv().await.unwrap();

        // Should get error response
        let prepare_response = younger_responses.recv().await.unwrap();
        assert_eq!(
            prepare_response.headers.get("status"),
            Some(&"wounded".to_string()),
            "Wounded transaction should not be able to prepare"
        );

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }
}
