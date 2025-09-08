//! Tests for the generic stream processor

#[cfg(test)]
mod tests {
    use crate::engine::{OperationResult, TransactionEngine};
    use crate::processor::{ProcessorPhase, StreamProcessor};
    use proven_engine::{Message, MockClient, MockEngine};
    use proven_hlc::{HlcTimestamp, NodeId};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Helper to generate test timestamps
    fn test_timestamp() -> HlcTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        HlcTimestamp::new(physical, 0, NodeId::new(1))
    }

    /// Simple test operation
    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestOperation {
        Read { key: String },
        Write { key: String, value: String },
    }

    /// Simple test response
    #[derive(Debug, Serialize)]
    enum TestResponse {
        Value(Option<String>),
        Success,
    }

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

    impl TransactionEngine for TestEngine {
        type Operation = TestOperation;
        type Response = TestResponse;

        fn apply_operation(
            &mut self,
            operation: Self::Operation,
            _txn_id: HlcTimestamp,
        ) -> OperationResult<Self::Response> {
            match operation {
                TestOperation::Read { key } => {
                    let value = self.data.get(&key).cloned();
                    OperationResult::Success(TestResponse::Value(value))
                }
                TestOperation::Write { key, value } => {
                    self.data.insert(key, value);
                    OperationResult::Success(TestResponse::Success)
                }
            }
        }

        fn prepare(&mut self, _txn_id: HlcTimestamp) -> Result<(), String> {
            Ok(())
        }

        fn commit(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
            self.active_txns.retain(|&id| id != txn_id);
            Ok(())
        }

        fn abort(&mut self, txn_id: HlcTimestamp) -> Result<(), String> {
            self.active_txns.retain(|&id| id != txn_id);
            Ok(())
        }

        fn begin_transaction(&mut self, txn_id: HlcTimestamp) {
            self.active_txns.push(txn_id);
        }

        fn is_transaction_active(&self, txn_id: &HlcTimestamp) -> bool {
            self.active_txns.contains(txn_id)
        }

        fn engine_name(&self) -> &str {
            "test-engine"
        }
    }

    #[tokio::test]
    async fn test_basic_operation() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        let test_engine = TestEngine::new();
        let mut processor =
            StreamProcessor::new(test_engine, client.clone(), "test-stream".to_string());

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

        // Process the message (offset 0 for first message)
        let result = processor
            .process_message(message, test_timestamp(), 0)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transaction_lifecycle() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        let test_engine = TestEngine::new();
        let mut processor =
            StreamProcessor::new(test_engine, client.clone(), "test-stream".to_string());

        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));
        let txn_id_str = txn_id.to_string();
        let coord_id = "coord1";

        // Begin transaction implicitly with first operation
        let operation = TestOperation::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        };

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id_str.clone());
        headers.insert("coordinator_id".to_string(), coord_id.to_string());

        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
        assert!(
            processor
                .process_message(message, test_timestamp(), 0)
                .await
                .is_ok()
        );

        // Prepare the transaction
        headers.insert("txn_phase".to_string(), "prepare".to_string());
        let prepare_msg = Message::new(Vec::new(), headers.clone());
        assert!(
            processor
                .process_message(prepare_msg, test_timestamp(), 1)
                .await
                .is_ok()
        );

        // Commit the transaction
        headers.insert("txn_phase".to_string(), "commit".to_string());
        let commit_msg = Message::new(Vec::new(), headers);
        assert!(
            processor
                .process_message(commit_msg, test_timestamp(), 2)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_phase_transitions() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        let test_engine = TestEngine::new();

        // Create processor starting in replay mode
        let mut processor = StreamProcessor::new_with_replay(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            5, // Target offset to transition at
        );

        // Verify initial phase is Replay
        assert!(matches!(
            processor.phase,
            ProcessorPhase::Replay { target_offset: 5 }
        ));

        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), "coord1".to_string());

        // Process messages during replay phase - they should not produce responses
        for offset in 0..5 {
            let operation = TestOperation::Write {
                key: format!("key{}", offset),
                value: format!("value{}", offset),
            };
            let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());

            // Messages during replay should be tracked but not executed
            let result = processor
                .process_message(message, test_timestamp(), offset)
                .await;
            assert!(result.is_ok());

            // Should still be in replay phase until we reach target
            if offset < 4 {
                assert!(matches!(processor.phase, ProcessorPhase::Replay { .. }));
            }
        }

        // Still in replay because offset 4 < target_offset 5
        assert!(matches!(processor.phase, ProcessorPhase::Replay { .. }));

        // Process message at offset 5 to trigger transition
        let operation = TestOperation::Write {
            key: "trigger".to_string(),
            value: "transition".to_string(),
        };
        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
        processor
            .process_message(message, test_timestamp(), 5)
            .await
            .unwrap();

        // Now we should have transitioned through Recovery to Live
        assert!(matches!(processor.phase, ProcessorPhase::Live));

        // Now process a message in live mode - should execute normally
        let operation = TestOperation::Write {
            key: "live_key".to_string(),
            value: "live_value".to_string(),
        };
        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers);

        let result = processor
            .process_message(message, test_timestamp(), 6)
            .await;
        assert!(result.is_ok());

        // Should still be in live phase
        assert!(matches!(processor.phase, ProcessorPhase::Live));
    }

    #[tokio::test]
    async fn test_replay_state_tracking() {
        let engine_backend = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-node".to_string(),
            engine_backend.clone(),
        ));

        let test_engine = TestEngine::new();

        // Create processor starting in replay mode
        let mut processor = StreamProcessor::new_with_replay(
            test_engine,
            client.clone(),
            "test-stream".to_string(),
            3,
        );

        let txn1 = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let txn2 = HlcTimestamp::new(2000, 0, NodeId::new(1));

        // Simulate replay: txn1 prepares
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn1.to_string());
        headers.insert("coordinator_id".to_string(), "coord1".to_string());
        headers.insert("txn_phase".to_string(), "prepare".to_string());

        // Add deadline and participants for recovery tracking
        let deadline = HlcTimestamp::new(1500, 0, NodeId::new(1));
        headers.insert("deadline".to_string(), deadline.to_string());
        headers.insert("participant_stream1".to_string(), "10".to_string());
        headers.insert("participant_stream2".to_string(), "20".to_string());

        let prepare_msg = Message::new(Vec::new(), headers.clone());
        processor
            .process_message(prepare_msg, test_timestamp(), 0)
            .await
            .unwrap();

        // txn2 commits
        headers.clear();
        headers.insert("txn_id".to_string(), txn2.to_string());
        headers.insert("coordinator_id".to_string(), "coord2".to_string());
        headers.insert("txn_phase".to_string(), "commit".to_string());

        let commit_msg = Message::new(Vec::new(), headers);
        processor
            .process_message(commit_msg, test_timestamp(), 1)
            .await
            .unwrap();

        // Still in replay at offset 1
        assert!(matches!(processor.phase, ProcessorPhase::Replay { .. }));

        // Process message at offset 2 - still in replay
        let operation = TestOperation::Read {
            key: "dummy".to_string(),
        };
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn2.to_string());
        headers.insert("coordinator_id".to_string(), "coord2".to_string());

        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers.clone());
        processor
            .process_message(message, test_timestamp(), 2)
            .await
            .unwrap();

        // Still in replay because offset 2 < target 3
        assert!(matches!(processor.phase, ProcessorPhase::Replay { .. }));

        // Process message at offset 3 to trigger transition
        let message = Message::new(serde_json::to_vec(&operation).unwrap(), headers);
        processor
            .process_message(message, test_timestamp(), 3)
            .await
            .unwrap();

        // Should have transitioned to Live after recovery
        assert!(matches!(processor.phase, ProcessorPhase::Live));
    }
}
