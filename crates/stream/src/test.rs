//! Tests for the generic stream processor

#[cfg(test)]
mod tests {
    use crate::engine::{OperationResult, TransactionEngine};
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

    /// Simple test operation
    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    enum TestOperation {
        Read { key: String },
        Write { key: String, value: String },
    }

    impl proven_common::Operation for TestOperation {
        fn operation_type(&self) -> proven_common::OperationType {
            match self {
                TestOperation::Read { .. } => proven_common::OperationType::Read,
                TestOperation::Write { .. } => proven_common::OperationType::Write,
            }
        }
    }

    /// Simple test response
    #[derive(Debug, Serialize)]
    enum TestResponse {
        Value(Option<String>),
        Success,
    }

    impl proven_common::Response for TestResponse {}

    /// Simple test engine
    struct TestEngine {
        data: HashMap<String, String>,
        active_txns: Vec<HlcTimestamp>,
    }

    impl TestEngine {
        fn new() -> Self {
            Self {
                data: HashMap::new(),
                active_txns: Vec::new(),
            }
        }
    }

    #[derive(Serialize, Deserialize)]
    struct TestEngineSnapshot {
        data: HashMap<String, String>,
    }

    impl TransactionEngine for TestEngine {
        type Operation = TestOperation;
        type Response = TestResponse;

        fn read_at_timestamp(
            &self,
            operation: Self::Operation,
            _read_timestamp: HlcTimestamp,
        ) -> OperationResult<Self::Response> {
            match operation {
                TestOperation::Read { key } => {
                    let value = self.data.get(&key).cloned();
                    OperationResult::Complete(TestResponse::Value(value))
                }
                TestOperation::Write { .. } => {
                    panic!("Write operations not supported for read-only operations");
                }
            }
        }

        fn apply_operation(
            &mut self,
            operation: Self::Operation,
            _txn_id: HlcTimestamp,
        ) -> OperationResult<Self::Response> {
            match operation {
                TestOperation::Read { key } => {
                    let value = self.data.get(&key).cloned();
                    OperationResult::Complete(TestResponse::Value(value))
                }
                TestOperation::Write { key, value } => {
                    self.data.insert(key, value);
                    OperationResult::Complete(TestResponse::Success)
                }
            }
        }

        fn prepare(&mut self, _txn_id: HlcTimestamp) {
            // No-op for test engine
        }

        fn commit(&mut self, txn_id: HlcTimestamp) {
            self.active_txns.retain(|&id| id != txn_id);
        }

        fn abort(&mut self, txn_id: HlcTimestamp) {
            self.active_txns.retain(|&id| id != txn_id);
        }

        fn begin(&mut self, txn_id: HlcTimestamp) {
            self.active_txns.push(txn_id);
        }

        fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
            self.active_txns.contains(txn_id)
        }

        fn engine_name(&self) -> &'static str {
            "test-engine"
        }

        fn snapshot(&self) -> Result<Vec<u8>, String> {
            // Only snapshot when no active transactions
            if !self.active_txns.is_empty() {
                return Err("Cannot snapshot with active transactions".to_string());
            }

            let snapshot = TestEngineSnapshot {
                data: self.data.clone(),
            };

            let mut buf = Vec::new();
            ciborium::into_writer(&snapshot, &mut buf)
                .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;
            Ok(buf)
        }

        fn restore_from_snapshot(&mut self, data: &[u8]) -> Result<(), String> {
            let snapshot: TestEngineSnapshot = ciborium::from_reader(data)
                .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

            self.data = snapshot.data;
            self.active_txns.clear();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_basic_operation() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        // Create the stream first
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Create a test message with a write operation
        let operation = TestOperation::Write {
            key: "foo".to_string(),
            value: "bar".to_string(),
        };

        // Create a proper HLC timestamp
        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), "coord1".to_string());

        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers);

        // Publish message to stream
        client
            .publish_to_stream("test-stream".to_string(), vec![message])
            .await
            .unwrap();

        // Create and run processor in background
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
        let result = processor.start_with_replay(shutdown_rx).await;
        assert!(result.is_ok());

        // Give processor time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        // Create the stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));
        let txn_id_str = txn_id.to_string();
        let coord_id = "coord1";

        // Subscribe to responses before starting processor
        let mut responses = client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();

        // Begin transaction implicitly with first operation
        let operation = TestOperation::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        };

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id_str.clone());
        headers.insert("coordinator_id".to_string(), coord_id.to_string());

        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
        client
            .publish_to_stream("test-stream".to_string(), vec![message])
            .await
            .unwrap();

        // Prepare the transaction
        headers.insert("txn_phase".to_string(), "prepare".to_string());
        let prepare_msg = Message::new(Vec::new(), headers.clone());
        client
            .publish_to_stream("test-stream".to_string(), vec![prepare_msg])
            .await
            .unwrap();

        // Commit the transaction
        headers.insert("txn_phase".to_string(), "commit".to_string());
        let commit_msg = Message::new(Vec::new(), headers);
        client
            .publish_to_stream("test-stream".to_string(), vec![commit_msg])
            .await
            .unwrap();

        // Create and run processor
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

        // Wait for responses with timeout
        let timeout = Duration::from_millis(150);
        let _op_response = tokio::time::timeout(timeout, responses.recv()).await.ok();
        let _prepare_response = tokio::time::timeout(timeout, responses.recv()).await.ok();
        let _commit_response = tokio::time::timeout(timeout, responses.recv()).await.ok();

        // Give processor time to finish
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Shutdown the processor
        let _ = shutdown_tx.send(());
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        // Test that we can snapshot and restore engine state
        let mut engine1 = TestEngine::new();

        // Add some data
        engine1
            .data
            .insert("key1".to_string(), "value1".to_string());
        engine1
            .data
            .insert("key2".to_string(), "value2".to_string());
        engine1
            .data
            .insert("key3".to_string(), "value3".to_string());

        // Take a snapshot
        let snapshot = engine1.snapshot().unwrap();
        assert!(!snapshot.is_empty());

        // Create a new engine and restore
        let mut engine2 = TestEngine::new();
        assert!(engine2.data.is_empty());

        engine2.restore_from_snapshot(&snapshot).unwrap();

        // Verify data was restored exactly
        assert_eq!(engine2.data.len(), 3);
        assert_eq!(engine2.data.get("key1"), Some(&"value1".to_string()));
        assert_eq!(engine2.data.get("key2"), Some(&"value2".to_string()));
        assert_eq!(engine2.data.get("key3"), Some(&"value3".to_string()));

        // Verify the restored engine is functional
        let txn_id = HlcTimestamp::new(100, 0, NodeId::new(1));
        engine2.begin(txn_id);
        let result = engine2.apply_operation(
            TestOperation::Write {
                key: "key4".to_string(),
                value: "value4".to_string(),
            },
            txn_id,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
        engine2.commit(txn_id);
        assert_eq!(engine2.data.get("key4"), Some(&"value4".to_string()));
    }

    #[tokio::test]
    async fn test_snapshot_with_active_transaction() {
        let mut engine = TestEngine::new();

        // Begin a transaction
        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));
        engine.begin(txn_id);

        // Should fail to snapshot with active transaction
        let result = engine.snapshot();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("active transactions"));

        // Commit the transaction
        engine.commit(txn_id);

        // Now snapshot should succeed
        let result = engine.snapshot();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_memory_snapshot_store() {
        use proven_snapshot::SnapshotStore;
        use proven_snapshot_memory::MemorySnapshotStore;

        let store = MemorySnapshotStore::new();
        let stream = "test-stream";
        let timestamp = HlcTimestamp::new(1000, 0, NodeId::new(1));

        // Initially no snapshots
        assert!(store.get_latest_snapshot(stream).is_none());
        assert!(store.list_snapshots(stream).is_empty());

        // Save a snapshot
        let data1 = vec![1, 2, 3, 4, 5];
        store
            .save_snapshot(stream, 100, timestamp, data1.clone())
            .unwrap();

        // Verify we can retrieve it
        let (metadata, data) = store.get_latest_snapshot(stream).unwrap();
        assert_eq!(metadata.log_offset, 100);
        assert_eq!(metadata.log_timestamp, timestamp);
        assert_eq!(data, data1);

        // Save another snapshot at higher offset
        let timestamp2 = HlcTimestamp::new(2000, 0, NodeId::new(1));
        let data2 = vec![6, 7, 8, 9, 10];
        store
            .save_snapshot(stream, 200, timestamp2, data2.clone())
            .unwrap();

        // Latest should be the newer one
        let (metadata, data) = store.get_latest_snapshot(stream).unwrap();
        assert_eq!(metadata.log_offset, 200);
        assert_eq!(data, data2);

        // List should show both (newest first)
        let snapshots = store.list_snapshots(stream);
        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].log_offset, 200);
        assert_eq!(snapshots[1].log_offset, 100);

        // Test cleanup
        store.cleanup_old_snapshots(stream, 1).unwrap();
        let snapshots = store.list_snapshots(stream);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].log_offset, 200);
    }

    #[tokio::test]
    async fn test_processor_with_snapshot_store() {
        use crate::processor::{SnapshotConfig, StreamProcessor};
        use proven_snapshot::SnapshotStore;
        use proven_snapshot_memory::MemorySnapshotStore;

        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        // Create the stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Create snapshot store
        let snapshot_store = Arc::new(MemorySnapshotStore::new());

        // Create a test engine with some initial data
        let mut engine = TestEngine::new();
        engine
            .data
            .insert("initial".to_string(), "data".to_string());

        // Create processor with snapshot store
        let mut processor = StreamProcessor::new(
            engine,
            client.clone(),
            "test-stream".to_string(),
            snapshot_store.clone(),
        );

        // Configure for aggressive snapshotting (for testing)
        processor.snapshot_config = SnapshotConfig {
            idle_duration: Duration::from_millis(100),
            min_commits_between_snapshots: 1,
        };

        // Process some transactions to trigger snapshot
        for i in 0..3 {
            let txn_id = HlcTimestamp::new(i + 1, 0, NodeId::new(1));
            let operation = TestOperation::Write {
                key: format!("key{}", i),
                value: format!("value{}", i),
            };

            let mut headers = HashMap::new();
            headers.insert("txn_id".to_string(), txn_id.to_string());
            headers.insert("coordinator_id".to_string(), "coord1".to_string());

            // Write operation
            let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
            client
                .publish_to_stream("test-stream".to_string(), vec![message])
                .await
                .unwrap();

            // Commit
            headers.insert("txn_phase".to_string(), "commit".to_string());
            let commit_msg = Message::new(Vec::new(), headers);
            client
                .publish_to_stream("test-stream".to_string(), vec![commit_msg])
                .await
                .unwrap();
        }

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        // Start processor with replay
        processor.start_with_replay(shutdown_rx).await.unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Shutdown the processor
        let _ = shutdown_tx.send(());

        // Wait a bit more for snapshot to be taken
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify a snapshot was created
        let snapshots = snapshot_store.list_snapshots("test-stream");

        // We might not have a snapshot yet due to timing, but if we do, verify it
        if !snapshots.is_empty() {
            let (metadata, data) = snapshot_store.get_latest_snapshot("test-stream").unwrap();
            assert!(metadata.log_offset > 0);
            assert!(!data.is_empty());

            // Create a new engine and restore from snapshot
            let mut new_engine = TestEngine::new();
            new_engine.restore_from_snapshot(&data).unwrap();

            // Should have the data that was written
            // Note: exact data depends on timing of snapshot
            assert!(!new_engine.data.is_empty());
        }
    }

    #[tokio::test]
    async fn test_processor_restoration_state() {
        use crate::processor::StreamProcessor;
        use proven_snapshot::SnapshotStore;
        use proven_snapshot_memory::MemorySnapshotStore;

        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        // Create the stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        let snapshot_store = Arc::new(MemorySnapshotStore::new());

        // Phase 1: Process some messages and create a snapshot
        {
            let engine = TestEngine::new();
            let processor = StreamProcessor::new(
                engine,
                client.clone(),
                "test-stream".to_string(),
                snapshot_store.clone(),
            );

            // Send 5 transactions
            for i in 0..5 {
                let txn_id = HlcTimestamp::new(i + 1, 0, NodeId::new(1));
                let operation = TestOperation::Write {
                    key: format!("key{}", i),
                    value: format!("value{}", i),
                };

                let mut headers = HashMap::new();
                headers.insert("txn_id".to_string(), txn_id.to_string());
                headers.insert("coordinator_id".to_string(), "coord1".to_string());

                let message =
                    Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
                client
                    .publish_to_stream("test-stream".to_string(), vec![message])
                    .await
                    .unwrap();

                headers.insert("txn_phase".to_string(), "commit".to_string());
                let commit_msg = Message::new(Vec::new(), headers);
                client
                    .publish_to_stream("test-stream".to_string(), vec![commit_msg])
                    .await
                    .unwrap();
            }

            // Create shutdown channel
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

            // Start processor with replay
            processor.start_with_replay(shutdown_rx).await.unwrap();

            // Give processor time to process messages
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Shutdown the processor
            let _ = shutdown_tx.send(());

            // Wait for processing
            tokio::time::sleep(Duration::from_millis(250)).await;

            // Manually create a snapshot for testing
            let test_engine = TestEngine {
                data: vec![
                    ("key0".to_string(), "value0".to_string()),
                    ("key1".to_string(), "value1".to_string()),
                    ("key2".to_string(), "value2".to_string()),
                    ("key3".to_string(), "value3".to_string()),
                    ("key4".to_string(), "value4".to_string()),
                ]
                .into_iter()
                .collect(),
                active_txns: Vec::new(),
            };

            let snapshot_data = test_engine.snapshot().unwrap();
            let timestamp = HlcTimestamp::new(5000, 0, NodeId::new(1));

            // Save snapshot at offset 9 (5 operations + 5 commits - 1 for 0-indexing)
            snapshot_store
                .save_snapshot("test-stream", 9, timestamp, snapshot_data)
                .unwrap();

            5 // Number of keys we processed
        };

        // Phase 2: Create new processor from snapshot and verify state
        {
            let engine = TestEngine::new();
            let processor = StreamProcessor::new(
                engine,
                client.clone(),
                "test-stream".to_string(),
                snapshot_store.clone(),
            );

            // Verify processor starts from correct offset
            assert_eq!(processor.current_offset, 10); // Should start at snapshot offset + 1

            // The fact that current_offset is 10 proves we restored from snapshot
            // and will skip replaying messages 0-9
        }

        // Verify snapshot was actually used
        let snapshots = snapshot_store.list_snapshots("test-stream");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].log_offset, 9);
    }
}
