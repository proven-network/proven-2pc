//! Integration tests for basic 2PC transaction flows
//!
//! These tests verify the fundamental two-phase commit protocol:
//! - Begin → Execute → Prepare → Commit
//! - Begin → Execute → Abort
//! - Ad-hoc operations (auto-commit)
//! - Read-only operations (snapshot isolation)

mod common;

use common::{BasicOp, BasicResponse, TestEngine, run_test_processor, txn_id};
use proven_engine::{MockClient, MockEngine};
use proven_protocol::{OrderedMessage, TransactionPhase};
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Test complete 2PC flow: begin → execute → prepare → commit
#[tokio::test]
async fn test_full_2pc_commit() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<BasicOp, BasicResponse>::new();

    // Start processor in background using test wrapper
    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    // Wait for replay to complete (replay has 1 second timeout for empty stream)
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Subscribe to responses
    let mut coord_responses = client
        .subscribe("coordinator.coord1.response", None)
        .await
        .unwrap();

    let txn_id = txn_id(1000);

    // Execute: Write key1=value1
    let op_msg = OrderedMessage::TransactionOperation {
        txn_id,
        coordinator_id: "coord1".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: BasicOp::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![op_msg.into_message()])
        .await
        .unwrap();

    // Should get success response for write
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");
    assert_eq!(response.get_header("status").unwrap(), "complete");

    // Prepare
    let prepare_msg = OrderedMessage::<BasicOp>::TransactionControl {
        txn_id,
        phase: TransactionPhase::Prepare(std::collections::HashMap::new()),
        coordinator_id: Some("coord1".to_string()),
        request_id: Some("req2".to_string()),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![prepare_msg.into_message()])
        .await
        .unwrap();

    // Should get prepared response
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");
    assert_eq!(response.get_header("status").unwrap(), "prepared");

    // Commit
    let commit_msg = OrderedMessage::<BasicOp>::TransactionControl {
        txn_id,
        phase: TransactionPhase::Commit,
        coordinator_id: Some("coord1".to_string()),
        request_id: Some("req3".to_string()),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![commit_msg.into_message()])
        .await
        .unwrap();

    // Give time to process
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Shutdown processor
    let _ = shutdown_tx.send(());
}

/// Test 2PC abort flow
#[tokio::test]
async fn test_2pc_abort() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<BasicOp, BasicResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut coord_responses = client
        .subscribe("coordinator.coord1.response", None)
        .await
        .unwrap();

    let txn_id = txn_id(2000);

    // Execute operation
    let op_msg = OrderedMessage::TransactionOperation {
        txn_id,
        coordinator_id: "coord1".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: BasicOp::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![op_msg.into_message()])
        .await
        .unwrap();

    // Wait for success response
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await;

    // Abort
    let abort_msg = OrderedMessage::<BasicOp>::TransactionControl {
        txn_id,
        phase: TransactionPhase::Abort,
        coordinator_id: Some("coord1".to_string()),
        request_id: Some("req2".to_string()),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![abort_msg.into_message()])
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test ad-hoc auto-commit
#[tokio::test]
async fn test_adhoc_autocommit() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<BasicOp, BasicResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut coord_responses = client
        .subscribe("coordinator.coord1.response", None)
        .await
        .unwrap();

    let txn_id = txn_id(3000);

    // Ad-hoc write (should auto-commit)
    let op_msg = OrderedMessage::AutoCommitOperation {
        txn_id,
        coordinator_id: "coord1".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: BasicOp::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![op_msg.into_message()])
        .await
        .unwrap();

    // Should get success response
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");
    assert_eq!(response.get_header("status").unwrap(), "complete");

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test multiple operations in a transaction
#[tokio::test]
async fn test_multiple_operations_in_transaction() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<BasicOp, BasicResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut coord_responses = client
        .subscribe("coordinator.coord1.response", None)
        .await
        .unwrap();

    let txn_id = txn_id(4000);

    // Multiple operations
    for i in 1..=3 {
        let op_msg = OrderedMessage::TransactionOperation {
            txn_id,
            coordinator_id: "coord1".to_string(),
            request_id: format!("req{}", i),
            txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
            operation: BasicOp::Write {
                key: format!("key{}", i),
                value: format!("value{}", i),
            },
        };

        client
            .publish_to_stream("test-stream".to_string(), vec![op_msg.into_message()])
            .await
            .unwrap();

        // Wait for response
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            coord_responses.next(),
        )
        .await;
    }

    // Prepare and commit
    let prepare_msg = OrderedMessage::<BasicOp>::TransactionControl {
        txn_id,
        phase: TransactionPhase::PrepareAndCommit,
        coordinator_id: Some("coord1".to_string()),
        request_id: Some("prepare".to_string()),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![prepare_msg.into_message()])
        .await
        .unwrap();

    // Wait for prepared response
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test prepare-and-commit optimization
#[tokio::test]
async fn test_prepare_and_commit_optimization() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<BasicOp, BasicResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut coord_responses = client
        .subscribe("coordinator.coord1.response", None)
        .await
        .unwrap();

    let txn_id = txn_id(5000);

    // Execute operation
    let op_msg = OrderedMessage::TransactionOperation {
        txn_id,
        coordinator_id: "coord1".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: BasicOp::Write {
            key: "key1".to_string(),
            value: "value1".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![op_msg.into_message()])
        .await
        .unwrap();

    // Wait for success
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await;

    // Prepare and commit in one message
    let prepare_commit_msg = OrderedMessage::<BasicOp>::TransactionControl {
        txn_id,
        phase: TransactionPhase::PrepareAndCommit,
        coordinator_id: Some("coord1".to_string()),
        request_id: Some("prepare_commit".to_string()),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
    };

    client
        .publish_to_stream(
            "test-stream".to_string(),
            vec![prepare_commit_msg.into_message()],
        )
        .await
        .unwrap();

    // Should get prepared response
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        coord_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");
    assert_eq!(response.get_header("status").unwrap(), "prepared");

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Shutdown
    let _ = shutdown_tx.send(());
}
