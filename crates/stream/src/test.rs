//! Tests for the generic stream processor

#[cfg(test)]
mod tests {
    use crate::engine::{OperationResult, TransactionEngine};
    use crate::processor::StreamProcessor;
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

        // Process the message
        let result = processor.process_message(message, test_timestamp()).await;
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
                .process_message(message, test_timestamp())
                .await
                .is_ok()
        );

        // Prepare the transaction
        headers.insert("txn_phase".to_string(), "prepare".to_string());
        let prepare_msg = Message::new(Vec::new(), headers.clone());
        assert!(
            processor
                .process_message(prepare_msg, test_timestamp())
                .await
                .is_ok()
        );

        // Commit the transaction
        headers.insert("txn_phase".to_string(), "commit".to_string());
        let commit_msg = Message::new(Vec::new(), headers);
        assert!(
            processor
                .process_message(commit_msg, test_timestamp())
                .await
                .is_ok()
        );
    }
}
