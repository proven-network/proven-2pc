//! Integration tests for the queue crate

use proven_hlc::{HlcTimestamp, NodeId};
use proven_queue::stream::{QueueEngine, QueueOperation, QueueResponse};
use proven_queue::types::QueueValue;
use proven_stream::RetryOn;
use proven_stream::engine::{OperationResult, TransactionEngine};

fn create_timestamp(seconds: u64) -> HlcTimestamp {
    HlcTimestamp::new(seconds, 0, NodeId::new(1))
}

#[test]
fn test_basic_queue_operations() {
    let mut engine = QueueEngine::new();
    let tx = create_timestamp(100);

    engine.begin_transaction(tx);

    // Test enqueue
    let enqueue1 = QueueOperation::Enqueue {
        queue_name: "test_queue".to_string(),
        value: QueueValue::String("first".to_string()),
    };

    let result = engine.apply_operation(enqueue1, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Enqueued)
    ));

    let enqueue2 = QueueOperation::Enqueue {
        queue_name: "test_queue".to_string(),
        value: QueueValue::String("second".to_string()),
    };

    let result = engine.apply_operation(enqueue2, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Enqueued)
    ));

    // Test size
    let size_op = QueueOperation::Size {
        queue_name: "test_queue".to_string(),
    };

    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(2))
    ));

    // Test dequeue (FIFO)
    let dequeue_op = QueueOperation::Dequeue {
        queue_name: "test_queue".to_string(),
    };

    let result = engine.apply_operation(dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Dequeued(Some(QueueValue::String(s)))) if s == "first"
    ));

    let result = engine.apply_operation(dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Dequeued(Some(QueueValue::String(s)))) if s == "second"
    ));

    // Queue should be empty now
    let result = engine.apply_operation(dequeue_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Dequeued(None))
    ));

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_transaction_isolation() {
    let mut engine = QueueEngine::new();
    let tx1 = create_timestamp(100);
    let tx2 = create_timestamp(200);

    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // tx1 enqueues a value (acquires exclusive lock)
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "isolated_queue".to_string(),
        value: QueueValue::Integer(42),
    };

    let result = engine.apply_operation(enqueue_op, tx1);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Enqueued)
    ));

    // tx2 tries to read the queue but is blocked by tx1's exclusive lock
    // This is correct behavior for eager locking
    let size_op = QueueOperation::Size {
        queue_name: "isolated_queue".to_string(),
    };

    let result = engine.apply_operation(size_op.clone(), tx2);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    // Commit tx1 to release its lock
    assert!(engine.commit(tx1).is_ok());

    // Now tx2 can read and should see the committed value
    let result = engine.apply_operation(size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(1))
    ));
    assert!(engine.commit(tx2).is_ok());

    // Start a new transaction - should also see the committed value
    let tx3 = create_timestamp(300);
    engine.begin_transaction(tx3);

    let result = engine.apply_operation(size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(1))
    ));

    assert!(engine.commit(tx3).is_ok());
}

#[test]
fn test_concurrent_access_with_locking() {
    let mut engine = QueueEngine::new();
    let tx1 = create_timestamp(100);
    let tx2 = create_timestamp(200);

    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // tx1 acquires exclusive lock by enqueuing
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "locked_queue".to_string(),
        value: QueueValue::String("tx1_data".to_string()),
    };

    let result = engine.apply_operation(enqueue_op, tx1);
    assert!(matches!(result, OperationResult::Success(_)));

    // tx2 tries to dequeue - should be blocked
    let dequeue_op = QueueOperation::Dequeue {
        queue_name: "locked_queue".to_string(),
    };

    let result = engine.apply_operation(dequeue_op.clone(), tx2);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    // After tx1 commits, tx2 should be able to proceed
    assert!(engine.commit(tx1).is_ok());

    // Now tx2 can dequeue
    let result = engine.apply_operation(dequeue_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Dequeued(Some(QueueValue::String(s)))) if s == "tx1_data"
    ));

    assert!(engine.commit(tx2).is_ok());
}

#[test]
fn test_abort_rollback() {
    let mut engine = QueueEngine::new();
    let tx1 = create_timestamp(100);

    engine.begin_transaction(tx1);

    // Enqueue some values
    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "rollback_queue".to_string(),
            value: QueueValue::Integer(i),
        };
        engine.apply_operation(enqueue_op, tx1);
    }

    // Check size before abort
    let size_op = QueueOperation::Size {
        queue_name: "rollback_queue".to_string(),
    };

    let result = engine.apply_operation(size_op.clone(), tx1);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(3))
    ));

    // Abort the transaction
    assert!(engine.abort(tx1).is_ok());

    // Start new transaction - should see empty queue
    let tx2 = create_timestamp(200);
    engine.begin_transaction(tx2);

    let result = engine.apply_operation(size_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(0))
    ));

    assert!(engine.commit(tx2).is_ok());
}

#[test]
fn test_multiple_queues() {
    let mut engine = QueueEngine::new();
    let tx = create_timestamp(100);

    engine.begin_transaction(tx);

    // Create multiple queues
    let queues = ["queue_a", "queue_b", "queue_c"];

    for (i, queue_name) in queues.iter().enumerate() {
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: queue_name.to_string(),
            value: QueueValue::Integer(i as i64),
        };
        engine.apply_operation(enqueue_op, tx);
    }

    // Each queue should have one item
    for queue_name in queues.iter() {
        let size_op = QueueOperation::Size {
            queue_name: queue_name.to_string(),
        };

        let result = engine.apply_operation(size_op, tx);
        assert!(matches!(
            result,
            OperationResult::Success(QueueResponse::Size(1))
        ));
    }

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_peek_operation() {
    let mut engine = QueueEngine::new();
    let tx = create_timestamp(100);

    engine.begin_transaction(tx);

    // Enqueue a value
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "peek_queue".to_string(),
        value: QueueValue::String("peek_me".to_string()),
    };

    engine.apply_operation(enqueue_op, tx);

    // Peek should not remove the value
    let peek_op = QueueOperation::Peek {
        queue_name: "peek_queue".to_string(),
    };

    let result = engine.apply_operation(peek_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Peeked(Some(QueueValue::String(s)))) if s == "peek_me"
    ));

    // Peek again - should still be there
    let result = engine.apply_operation(peek_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Peeked(Some(QueueValue::String(s)))) if s == "peek_me"
    ));

    // Size should still be 1
    let size_op = QueueOperation::Size {
        queue_name: "peek_queue".to_string(),
    };

    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(1))
    ));

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_clear_operation() {
    let mut engine = QueueEngine::new();
    let tx = create_timestamp(100);

    engine.begin_transaction(tx);

    // Enqueue multiple values
    for i in 0..5 {
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "clear_queue".to_string(),
            value: QueueValue::Integer(i),
        };
        engine.apply_operation(enqueue_op, tx);
    }

    // Verify size
    let size_op = QueueOperation::Size {
        queue_name: "clear_queue".to_string(),
    };

    let result = engine.apply_operation(size_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(5))
    ));

    // Clear the queue
    let clear_op = QueueOperation::Clear {
        queue_name: "clear_queue".to_string(),
    };

    let result = engine.apply_operation(clear_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Cleared)
    ));

    // Queue should be empty
    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(0))
    ));

    let is_empty_op = QueueOperation::IsEmpty {
        queue_name: "clear_queue".to_string(),
    };

    let result = engine.apply_operation(is_empty_op, tx);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::IsEmpty(true))
    ));

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_shared_locks_for_reads() {
    let mut engine = QueueEngine::new();
    let tx1 = create_timestamp(100);
    let tx2 = create_timestamp(200);
    let tx3 = create_timestamp(300);

    // First transaction enqueues and commits
    engine.begin_transaction(tx1);
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "shared_queue".to_string(),
        value: QueueValue::String("shared_data".to_string()),
    };
    engine.apply_operation(enqueue_op, tx1);
    assert!(engine.commit(tx1).is_ok());

    // Now two transactions try to read concurrently
    engine.begin_transaction(tx2);
    engine.begin_transaction(tx3);

    // Both should be able to peek (shared lock)
    let peek_op = QueueOperation::Peek {
        queue_name: "shared_queue".to_string(),
    };

    let result = engine.apply_operation(peek_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Peeked(_))
    ));

    let result = engine.apply_operation(peek_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Peeked(_))
    ));

    // Both should be able to check size (shared lock)
    let size_op = QueueOperation::Size {
        queue_name: "shared_queue".to_string(),
    };

    let result = engine.apply_operation(size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(1))
    ));

    let result = engine.apply_operation(size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Success(QueueResponse::Size(1))
    ));

    assert!(engine.commit(tx2).is_ok());
    assert!(engine.commit(tx3).is_ok());
}

#[test]
fn test_various_value_types() {
    let mut engine = QueueEngine::new();
    let tx = create_timestamp(100);

    engine.begin_transaction(tx);

    // Test different value types
    let values = [
        QueueValue::String("text".to_string()),
        QueueValue::Integer(42),
        QueueValue::Float(std::f64::consts::PI),
        QueueValue::Boolean(true),
        QueueValue::Bytes(vec![1, 2, 3, 4]),
        QueueValue::Json(serde_json::json!({"key": "value"})),
    ];

    // Enqueue all value types
    for value in values.iter() {
        let enqueue_op = QueueOperation::Enqueue {
            queue_name: "mixed_queue".to_string(),
            value: value.clone(),
        };
        engine.apply_operation(enqueue_op, tx);
    }

    // Dequeue and verify FIFO order
    for expected_value in values.iter() {
        let dequeue_op = QueueOperation::Dequeue {
            queue_name: "mixed_queue".to_string(),
        };

        let result = engine.apply_operation(dequeue_op, tx);
        match result {
            OperationResult::Success(QueueResponse::Dequeued(Some(value))) => {
                assert_eq!(&value, expected_value);
            }
            _ => panic!("Expected successful dequeue"),
        }
    }

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = QueueEngine::new();
    let tx1 = HlcTimestamp::new(100, 0, NodeId::new(1));
    let tx2 = HlcTimestamp::new(200, 0, NodeId::new(1));

    // Begin both transactions
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Peek queue1 (acquires shared lock)
    let peek_op = QueueOperation::Peek {
        queue_name: "queue1".to_string(),
    };
    let result = engine.apply_operation(peek_op, tx1);
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to enqueue to queue1 (should be blocked)
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "queue1".to_string(),
        value: QueueValue::String("value2".to_string()),
    };
    let result = engine.apply_operation(enqueue_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock {
            blocking_txn,
            retry_on,
        } => {
            assert_eq!(blocking_txn, tx1);
            assert_eq!(retry_on, RetryOn::Prepare); // Can retry after prepare
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (should release read lock)
    engine.prepare(tx1).expect("Prepare should succeed");

    // TX2: Retry enqueue (should now succeed)
    let result = engine.apply_operation(enqueue_op, tx2);
    assert!(matches!(result, OperationResult::Success(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = QueueEngine::new();
    let tx1 = HlcTimestamp::new(100, 0, NodeId::new(1));
    let tx2 = HlcTimestamp::new(200, 0, NodeId::new(1));

    // Begin both transactions
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Enqueue to queue1 (acquires exclusive lock)
    let enqueue_op = QueueOperation::Enqueue {
        queue_name: "queue1".to_string(),
        value: QueueValue::String("value1".to_string()),
    };
    let result = engine.apply_operation(enqueue_op, tx1);
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to peek queue1 (should be blocked)
    let peek_op = QueueOperation::Peek {
        queue_name: "queue1".to_string(),
    };
    let result = engine.apply_operation(peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock {
            blocking_txn,
            retry_on,
        } => {
            assert_eq!(blocking_txn, tx1);
            assert_eq!(retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (write lock should NOT be released)
    engine.prepare(tx1).expect("Prepare should succeed");

    // TX2: Retry peek (should still be blocked)
    let result = engine.apply_operation(peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock {
            blocking_txn,
            retry_on,
        } => {
            assert_eq!(blocking_txn, tx1);
            assert_eq!(retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }

    // TX1: Commit (should release write lock)
    engine.commit(tx1).expect("Commit should succeed");

    // TX2: Retry peek (should now succeed)
    let result = engine.apply_operation(peek_op, tx2);
    assert!(matches!(result, OperationResult::Success(_)));
}
