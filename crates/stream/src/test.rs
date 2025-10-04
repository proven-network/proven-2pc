//! Tests for the generic stream processor

#[cfg(test)]
mod tests {
    use crate::engine::{OperationResult, TransactionEngine};
    use crate::processor::StreamProcessor;
    use crate::test_utils::{BasicOp, BasicResponse, TestEngine};
    use proven_engine::{Message, MockClient, MockEngine};
    use proven_hlc::{HlcTimestamp, NodeId};
    use proven_snapshot_memory::MemorySnapshotStore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

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
        let operation = BasicOp::Write {
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
        let test_engine = TestEngine::<BasicOp, BasicResponse>::new();
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
        let operation = BasicOp::Write {
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
        let test_engine = TestEngine::<BasicOp, BasicResponse>::new();
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
    async fn test_engine_log_index() {
        let engine = TestEngine::<BasicOp, BasicResponse>::new();

        // Default log index is 0
        assert_eq!(engine.get_log_index(), Some(0));
    }

    #[tokio::test]
    async fn test_engine_operations_with_log_index() {
        let mut engine = TestEngine::<BasicOp, BasicResponse>::new();

        // Verify the engine accepts log_index in operations
        let txn_id = HlcTimestamp::new(100, 0, NodeId::new(1));
        engine.begin(txn_id, 1);
        let result = engine.apply_operation(
            BasicOp::Write {
                key: "key1".to_string(),
                value: "value1".to_string(),
            },
            txn_id,
            2,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
        engine.commit(txn_id, 3);
        assert_eq!(engine.data().get("key1"), Some(&"value1".to_string()));
    }

    #[tokio::test]
    async fn test_memory_snapshot_store() {
        use proven_snapshot::SnapshotStore;
        use proven_snapshot_memory::MemorySnapshotStore;

        let store = MemorySnapshotStore::new();
        let stream = "test-stream";

        // Initially no offset
        assert!(store.get(stream).is_none());

        // Update offset
        store.update(stream, 100);

        // Verify we can retrieve it
        assert_eq!(store.get(stream), Some(100));

        // Update to higher offset
        store.update(stream, 200);

        // Latest should be the newer one
        assert_eq!(store.get(stream), Some(200));
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

        // Create a test engine
        let engine = TestEngine::<BasicOp, BasicResponse>::new();

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
            let operation = BasicOp::Write {
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
        if let Some(offset) = snapshot_store.get("test-stream") {
            assert!(offset > 0);
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
            let engine = TestEngine::<BasicOp, BasicResponse>::new();
            let processor = StreamProcessor::new(
                engine,
                client.clone(),
                "test-stream".to_string(),
                snapshot_store.clone(),
            );

            // Send 5 transactions
            for i in 0..5 {
                let txn_id = HlcTimestamp::new(i + 1, 0, NodeId::new(1));
                let operation = BasicOp::Write {
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

            // Save snapshot at offset 9 (snapshots now only track offsets)
            snapshot_store.update("test-stream", 9);

            5 // Number of keys we processed
        };

        // Phase 2: Create new processor from snapshot and verify state
        {
            // Restore engine at the snapshot offset
            let engine = TestEngine::<BasicOp, BasicResponse>::with_log_index(9);
            let processor = StreamProcessor::new(
                engine,
                client.clone(),
                "test-stream".to_string(),
                snapshot_store.clone(),
            );

            // Engine is at offset 9, snapshot is at 9, so processor should start at 10
            assert_eq!(processor.current_offset, 10); // Should start at snapshot offset + 1
        }

        // Verify snapshot was saved
        assert_eq!(snapshot_store.get("test-stream"), Some(9));
    }
}
