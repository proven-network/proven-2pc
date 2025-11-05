//! Integration tests for wound-wait deadlock prevention
//!
//! These tests verify that the stream processor correctly implements
//! the wound-wait protocol for deadlock prevention. In wound-wait:
//! - Older transactions (smaller UUID) wound younger transactions
//! - Younger transactions defer (wait) for older transactions

mod common;

use common::{LockOp, LockResponse, TestEngine, run_test_processor, txn_id};
use proven_engine::{MockClient, MockEngine};
use proven_protocol::OrderedMessage;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Test that an older transaction wounds a younger transaction
///
/// Scenario:
/// 1. Younger transaction (T2) acquires lock on resource A
/// 2. Older transaction (T1) tries to acquire same lock
/// 3. Expected: T2 is wounded (aborted), T1 acquires lock
#[tokio::test]
async fn test_older_wounds_younger() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<LockOp, LockResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // Subscribe to responses
    let mut younger_responses = client
        .subscribe("coordinator.younger.response", None)
        .await
        .unwrap();
    let mut older_responses = client
        .subscribe("coordinator.older.response", None)
        .await
        .unwrap();

    // T2 (younger, timestamp=2000) acquires lock on "A"
    let t2_id = txn_id(2000);
    let t2_msg = OrderedMessage::TransactionOperation {
        txn_id: t2_id,
        coordinator_id: "younger".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t2_msg.into_message()])
        .await
        .unwrap();

    // T2 should successfully acquire lock
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        younger_responses.next(),
    )
    .await
    .expect("timeout waiting for T2 response")
    .expect("no response from T2");

    assert_eq!(response.get_header("status").unwrap(), "complete");

    // T1 (older, timestamp=1000) tries to acquire lock on "A"
    let t1_id = txn_id(1000);
    let t1_msg = OrderedMessage::TransactionOperation {
        txn_id: t1_id,
        coordinator_id: "older".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t1_msg.into_message()])
        .await
        .unwrap();

    // T2 should get wounded notification
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        younger_responses.next(),
    )
    .await
    .expect("timeout waiting for wound notification")
    .expect("no wound notification");

    assert_eq!(response.get_header("status").unwrap(), "wounded");

    // T1 should succeed after wounding T2
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        older_responses.next(),
    )
    .await
    .expect("timeout waiting for T1 response")
    .expect("no response from T1");

    assert_eq!(response.get_header("status").unwrap(), "complete");

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test that a younger transaction defers to an older transaction
///
/// Scenario:
/// 1. Older transaction (T1) acquires lock on resource A
/// 2. Younger transaction (T2) tries to acquire same lock
/// 3. Expected: T2 defers (waits), T1 keeps lock
#[tokio::test]
async fn test_younger_defers_to_older() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<LockOp, LockResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut older_responses = client
        .subscribe("coordinator.older.response", None)
        .await
        .unwrap();
    let mut younger_responses = client
        .subscribe("coordinator.younger.response", None)
        .await
        .unwrap();

    // T1 (older, timestamp=1000) acquires lock on "A"
    let t1_id = txn_id(1000);
    let t1_msg = OrderedMessage::TransactionOperation {
        txn_id: t1_id,
        coordinator_id: "older".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t1_msg.into_message()])
        .await
        .unwrap();

    // T1 should acquire lock
    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        older_responses.next(),
    )
    .await;

    // T2 (younger, timestamp=2000) tries to acquire lock on "A"
    let t2_id = txn_id(2000);
    let t2_msg = OrderedMessage::TransactionOperation {
        txn_id: t2_id,
        coordinator_id: "younger".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t2_msg.into_message()])
        .await
        .unwrap();

    // T2 should NOT get an immediate response (deferred)
    let result = tokio::time::timeout(
        tokio::time::Duration::from_millis(300),
        younger_responses.next(),
    )
    .await;

    assert!(
        result.is_err(),
        "T2 should be deferred, not get immediate response"
    );

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test multi-level wound chain
///
/// Scenario:
/// 1. T3 (youngest) acquires lock
/// 2. T2 (middle) tries to acquire → defers to T3
/// 3. T1 (oldest) tries to acquire → wounds T3, T2 can proceed
#[tokio::test]
async fn test_multi_level_wound_chain() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<LockOp, LockResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut youngest_responses = client
        .subscribe("coordinator.youngest.response", None)
        .await
        .unwrap();
    let mut oldest_responses = client
        .subscribe("coordinator.oldest.response", None)
        .await
        .unwrap();

    // T3 (youngest=3000) acquires lock
    let t3_id = txn_id(3000);
    let t3_msg = OrderedMessage::TransactionOperation {
        txn_id: t3_id,
        coordinator_id: "youngest".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t3_msg.into_message()])
        .await
        .unwrap();

    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        youngest_responses.next(),
    )
    .await;

    // T2 (middle=2000) tries to acquire - should defer
    let t2_id = txn_id(2000);
    let t2_msg = OrderedMessage::TransactionOperation {
        txn_id: t2_id,
        coordinator_id: "middle".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t2_msg.into_message()])
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    // T1 (oldest=1000) tries to acquire - should wound T3
    let t1_id = txn_id(1000);
    let t1_msg = OrderedMessage::TransactionOperation {
        txn_id: t1_id,
        coordinator_id: "oldest".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![t1_msg.into_message()])
        .await
        .unwrap();

    // T3 should be wounded
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        youngest_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");

    assert_eq!(response.get_header("status").unwrap(), "wounded");

    // T1 should succeed
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        oldest_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");

    assert_eq!(response.get_header("status").unwrap(), "complete");

    // Shutdown
    let _ = shutdown_tx.send(());
}

/// Test that wound-wait preserves determinism across runs
#[tokio::test]
async fn test_wound_preserves_determinism() {
    // Run the same scenario twice and verify same outcome
    for run in 1..=2 {
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        let test_engine = TestEngine::<LockOp, LockResponse>::new();

        let shutdown_tx =
            run_test_processor(test_engine, client.clone(), "test-stream".to_string());

        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        let mut younger_responses = client
            .subscribe("coordinator.younger.response", None)
            .await
            .unwrap();

        // Younger acquires lock
        let younger_id = txn_id(2000);
        let msg = OrderedMessage::TransactionOperation {
            txn_id: younger_id,
            coordinator_id: "younger".to_string(),
            request_id: "req1".to_string(),
            txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
            operation: LockOp::Lock {
                resource: "A".to_string(),
            },
        };

        client
            .publish_to_stream("test-stream".to_string(), vec![msg.into_message()])
            .await
            .unwrap();

        let _ = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            younger_responses.next(),
        )
        .await;

        // Older tries to acquire
        let older_id = txn_id(1000);
        let msg = OrderedMessage::TransactionOperation {
            txn_id: older_id,
            coordinator_id: "older".to_string(),
            request_id: "req1".to_string(),
            txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
            operation: LockOp::Lock {
                resource: "A".to_string(),
            },
        };

        client
            .publish_to_stream("test-stream".to_string(), vec![msg.into_message()])
            .await
            .unwrap();

        // Should always wound younger
        let response = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            younger_responses.next(),
        )
        .await
        .unwrap_or_else(|_| panic!("Run {}: timeout waiting for wound", run))
        .unwrap_or_else(|| panic!("Run {}: no wound response", run));

        assert_eq!(
            response.get_header("status").unwrap(),
            "wounded",
            "Run {}: younger should be wounded",
            run
        );

        // Shutdown
        let _ = shutdown_tx.send(());
    }
}

/// Test prepare phase with wound
#[tokio::test]
async fn test_prepare_phase_with_wound() {
    let engine = Arc::new(MockEngine::new());
    let client = Arc::new(MockClient::new("test-node".to_string(), engine.clone()));

    client
        .create_group_stream("test-stream".to_string())
        .await
        .unwrap();

    let test_engine = TestEngine::<LockOp, LockResponse>::new();

    let shutdown_tx = run_test_processor(test_engine, client.clone(), "test-stream".to_string());

    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let mut younger_responses = client
        .subscribe("coordinator.younger.response", None)
        .await
        .unwrap();

    // Younger acquires lock
    let younger_id = txn_id(2000);
    let msg = OrderedMessage::TransactionOperation {
        txn_id: younger_id,
        coordinator_id: "younger".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![msg.into_message()])
        .await
        .unwrap();

    let _ = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        younger_responses.next(),
    )
    .await;

    // Older tries to acquire - younger should be wounded
    let older_id = txn_id(1000);
    let msg = OrderedMessage::TransactionOperation {
        txn_id: older_id,
        coordinator_id: "older".to_string(),
        request_id: "req1".to_string(),
        txn_deadline: proven_common::Timestamp::now().add_micros(10_000_000),
        operation: LockOp::Lock {
            resource: "A".to_string(),
        },
    };

    client
        .publish_to_stream("test-stream".to_string(), vec![msg.into_message()])
        .await
        .unwrap();

    // Younger should be wounded
    let response = tokio::time::timeout(
        tokio::time::Duration::from_millis(500),
        younger_responses.next(),
    )
    .await
    .expect("timeout")
    .expect("no response");

    assert_eq!(response.get_header("status").unwrap(), "wounded");

    // Shutdown
    let _ = shutdown_tx.send(());
}
