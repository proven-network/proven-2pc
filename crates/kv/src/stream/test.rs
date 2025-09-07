//! Tests for KV stream processor

#[cfg(test)]
mod tests {
    use crate::stream::{KvOperation, KvStreamProcessor};
    use crate::types::Value;
    use proven_engine::{Message, MockClient, MockEngine};
    use proven_hlc::{HlcTimestamp, NodeId};
    use serde_json;
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

    /// Helper to generate transaction ID strings in HLC format
    fn txn_id(physical: u64, logical: u32) -> String {
        HlcTimestamp::new(physical, logical, NodeId::new(1)).to_string()
    }

    fn create_message(
        operation: Option<KvOperation>,
        txn_id: &str,
        coordinator_id: &str,
        auto_commit: bool,
        txn_phase: Option<&str>,
    ) -> Message {
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());

        // Add deadline for new transactions (1 hour from now)
        if operation.is_some() {
            let now_micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let deadline_micros = now_micros + 3600_000_000; // Add 1 hour in microseconds
            let deadline = HlcTimestamp::new(deadline_micros, 0, NodeId::new(1));
            headers.insert("txn_deadline".to_string(), deadline.to_string());
        }

        // Always include coordinator_id for operations (not needed for commit/abort)
        if operation.is_some() || txn_phase.is_some() {
            headers.insert("coordinator_id".to_string(), coordinator_id.to_string());
        }

        if auto_commit {
            headers.insert("auto_commit".to_string(), "true".to_string());
        }

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
    async fn test_basic_operations() {
        let (mut processor, engine) = KvStreamProcessor::new_for_testing();

        // Create a test client to subscribe to coordinator responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());

        // Subscribe to coordinator response channels BEFORE processing any messages
        let mut coord1_responses = test_client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();

        // Small delay to ensure subscription is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Put operation with auto-commit
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            &txn_id(1000000001, 0),
            "coord1",
            true, // auto-commit
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Give async task time to publish
        tokio::task::yield_now().await;

        // Get response - use timeout to avoid hanging forever
        let response =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), coord1_responses.recv())
                .await;

        let response = match response {
            Ok(Some(r)) => r,
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Timeout waiting for response - response was not sent"),
        };
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();

        // Check it's a PutResult
        assert_eq!(body["PutResult"]["key"], "key1");
        assert_eq!(body["PutResult"]["previous"], serde_json::Value::Null);

        // Get operation
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            &txn_id(2000000002, 0), // Different transaction
            "coord1",
            true,
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Give async task time to publish
        tokio::task::yield_now().await;

        // Get the response
        let response = coord1_responses.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();

        // Check it's a GetResult with the correct value
        assert_eq!(body["GetResult"]["key"], "key1");
        assert_eq!(body["GetResult"]["value"]["String"], "value1");

        // Delete operation
        let delete_op = KvOperation::Delete {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(delete_op),
            &txn_id(3000000003, 0),
            "coord1",
            true,
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Give async task time to publish
        tokio::task::yield_now().await;

        // Get the response
        let response = coord1_responses.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();

        // Check it's a DeleteResult
        assert_eq!(body["DeleteResult"]["key"], "key1");
        assert_eq!(body["DeleteResult"]["deleted"], true);
    }

    #[tokio::test]
    async fn test_transaction_isolation() {
        let (mut processor, engine) = KvStreamProcessor::new_for_testing();

        // Create test client for responses
        let test_client = MockClient::new("test-observer".to_string(), engine.clone());
        let mut coord_responses = test_client
            .subscribe("coordinator.coord1.response", None)
            .await
            .unwrap();

        // Small delay to ensure subscription is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Transaction 1: Put key1=value1
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            &txn_id(1000000001, 0),
            "coord1",
            false, // Not auto-commit
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Give async task time to publish
        tokio::task::yield_now().await;
        // Get response - use timeout to avoid hanging forever
        let response =
            tokio::time::timeout(tokio::time::Duration::from_secs(1), coord_responses.recv()).await;

        let response = match response {
            Ok(Some(r)) => r,
            Ok(None) => panic!("Channel closed"),
            Err(_) => panic!("Timeout waiting for response - response was not sent"),
        };
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["PutResult"]["key"], "key1");
        // Transaction 2: Try to read key1 (should not see uncommitted value)
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(
            Some(get_op),
            &txn_id(2000000002, 0),
            "coord1",
            true, // Auto-commit this one
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Give async task time to publish
        tokio::task::yield_now().await;

        // Get response - should be deferred since txn1 has exclusive lock
        let response = coord_responses.recv().await.unwrap();

        // Should get a deferred response
        assert_eq!(
            response.headers.get("status"),
            Some(&"deferred".to_string()),
            "Expected deferred response since txn1 has exclusive lock"
        );

        // Commit transaction 1 using prepare_and_commit
        let msg = create_message(
            None,
            &txn_id(1000000001, 0),
            "coord1",
            false,
            Some("prepare_and_commit"),
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Should get a prepared response (now in headers, not body)
        let response = coord_responses.recv().await.unwrap();
        assert_eq!(
            response.headers.get("status"),
            Some(&"prepared".to_string())
        );

        // After commit, txn2's deferred GET should be automatically retried
        // and we should receive its response
        let retried_response = coord_responses.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&retried_response.body).unwrap();
        assert_eq!(
            body["GetResult"]["value"]["String"], "value1",
            "Deferred operation should see committed value after retry"
        );

        // Transaction 3: Now read key1 (should see committed value)
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };
        let msg = create_message(Some(get_op), &txn_id(3000000003, 0), "coord1", true, None);
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Get response - should now see the committed value
        let response = coord_responses.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["GetResult"]["value"]["String"], "value1");
    }

    #[tokio::test]
    async fn test_kv_operations_through_processor() {
        // Create engine and processor
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));
        let mut processor = KvStreamProcessor::new(client, "kv-stream".to_string());

        let txn_id = HlcTimestamp::new(1, 0, NodeId::new(1));
        let coord_id = "test-coord";

        // Test PUT operation
        let put_op = KvOperation::Put {
            key: "key1".to_string(),
            value: crate::types::Value::String("value1".to_string()),
        };

        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), txn_id.to_string());
        headers.insert("coordinator_id".to_string(), coord_id.to_string());

        // Add deadline for the transaction (1 hour from now)
        let now_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let deadline_micros = now_micros + 3600_000_000; // Add 1 hour in microseconds
        let deadline = HlcTimestamp::new(deadline_micros, 0, NodeId::new(1));
        headers.insert("txn_deadline".to_string(), deadline.to_string());

        let put_msg = Message::new(serde_json::to_vec(&put_op).unwrap(), headers.clone());
        assert!(
            processor
                .process_message(put_msg, test_timestamp())
                .await
                .is_ok()
        );

        // Test GET operation
        let get_op = KvOperation::Get {
            key: "key1".to_string(),
        };

        // Remove deadline from headers for subsequent operations (already set in the processor)
        headers.remove("txn_deadline");

        let get_msg = Message::new(serde_json::to_vec(&get_op).unwrap(), headers.clone());
        assert!(
            processor
                .process_message(get_msg, test_timestamp())
                .await
                .is_ok()
        );

        // Test COMMIT
        headers.insert("txn_phase".to_string(), "prepare_and_commit".to_string());
        let commit_msg = Message::new(Vec::new(), headers);
        assert!(
            processor
                .process_message(commit_msg, test_timestamp())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_wound_wait_with_new_processor() {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));
        let mut processor = KvStreamProcessor::new(client, "kv-stream".to_string());

        // Older transaction
        let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
        // Younger transaction
        let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));

        // Transaction 2 gets exclusive lock first
        let put_op = KvOperation::Put {
            key: "contested".to_string(),
            value: crate::types::Value::String("txn2".to_string()),
        };

        let mut headers2 = HashMap::new();
        headers2.insert("txn_id".to_string(), txn2.to_string());
        headers2.insert("coordinator_id".to_string(), "coord2".to_string());

        let msg2 = Message::new(serde_json::to_vec(&put_op).unwrap(), headers2);
        assert!(
            processor
                .process_message(msg2, test_timestamp())
                .await
                .is_ok()
        );

        // Transaction 1 tries to get exclusive lock - should be deferred
        // (in the old system it would wound txn2, but now it just waits)
        let put_op2 = KvOperation::Put {
            key: "contested".to_string(),
            value: crate::types::Value::String("txn1".to_string()),
        };

        let mut headers1 = HashMap::new();
        headers1.insert("txn_id".to_string(), txn1.to_string());
        headers1.insert("coordinator_id".to_string(), "coord1".to_string());

        let msg1 = Message::new(serde_json::to_vec(&put_op2).unwrap(), headers1);
        // This should succeed (operation is deferred internally)
        assert!(
            processor
                .process_message(msg1, test_timestamp())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        let (mut processor, _engine) = KvStreamProcessor::new_for_testing();

        // Put a value first
        let put_op = KvOperation::Put {
            key: "shared_key".to_string(),
            value: Value::String("shared_value".to_string()),
        };
        let msg = create_message(
            Some(put_op),
            &txn_id(1000000001, 0),
            "coord1",
            true, // auto-commit
            None,
        );
        processor
            .process_message(msg, test_timestamp())
            .await
            .unwrap();

        // Multiple concurrent reads should all succeed (shared locks)
        let get_op = KvOperation::Get {
            key: "shared_key".to_string(),
        };

        // Transaction 2 reads
        let msg2 = create_message(
            Some(get_op.clone()),
            &txn_id(2000000002, 0),
            "coord2",
            false,
            None,
        );
        processor
            .process_message(msg2, test_timestamp())
            .await
            .unwrap();

        // Transaction 3 reads (should also succeed)
        let msg3 = create_message(
            Some(get_op.clone()),
            &txn_id(3000000003, 0),
            "coord3",
            false,
            None,
        );
        processor
            .process_message(msg3, test_timestamp())
            .await
            .unwrap();

        // Both transactions should be able to commit
        let commit2 = create_message(
            None,
            &txn_id(2000000002, 0),
            "coord2",
            false,
            Some("prepare_and_commit"),
        );
        processor
            .process_message(commit2, test_timestamp())
            .await
            .unwrap();

        let commit3 = create_message(
            None,
            &txn_id(3000000003, 0),
            "coord3",
            false,
            Some("prepare_and_commit"),
        );
        processor
            .process_message(commit3, test_timestamp())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_write_conflict_resolution() {
        let (mut processor, _engine) = KvStreamProcessor::new_for_testing();

        let _test_client = MockClient::new("test-observer".to_string(), _engine.clone());

        // Older transaction tries to write after younger one has lock
        let txn_old = &txn_id(1000000001, 0); // Older
        let txn_young = &txn_id(2000000002, 0); // Younger

        // Younger transaction gets exclusive lock first
        let put_young = KvOperation::Put {
            key: "conflict_key".to_string(),
            value: Value::String("young_value".to_string()),
        };
        let msg_young = create_message(Some(put_young), txn_young, "coord_young", false, None);
        processor
            .process_message(msg_young, test_timestamp())
            .await
            .unwrap();

        // Older transaction tries to write - should be deferred and wound the younger
        let put_old = KvOperation::Put {
            key: "conflict_key".to_string(),
            value: Value::String("old_value".to_string()),
        };
        let msg_old = create_message(Some(put_old.clone()), txn_old, "coord_old", false, None);
        processor
            .process_message(msg_old, test_timestamp())
            .await
            .unwrap();

        // Younger transaction should fail to commit because it was wounded
        let commit_young = create_message(
            None,
            txn_young,
            "coord_young",
            false,
            Some("prepare_and_commit"),
        );
        // Process returns Ok but transaction gets wounded response (not an error)
        processor
            .process_message(commit_young, test_timestamp())
            .await
            .unwrap();

        // Abort the younger transaction
        let abort_young = create_message(None, txn_young, "coord_young", false, Some("abort"));
        processor
            .process_message(abort_young, test_timestamp())
            .await
            .unwrap();

        // Now the older transaction's deferred operation should succeed
        // and it should be able to commit
        let commit_old = create_message(
            None,
            txn_old,
            "coord_old",
            false,
            Some("prepare_and_commit"),
        );
        processor
            .process_message(commit_old, test_timestamp())
            .await
            .unwrap();
    }
}
