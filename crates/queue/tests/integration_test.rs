//! Integration tests for the queue crate

use proven_common::TransactionId;
use proven_queue::engine::QueueTransactionEngine;
use proven_queue::types::{QueueOperation, QueueResponse, QueueValue};
use proven_stream::RetryOn;
use proven_stream::engine::{OperationResult, TransactionEngine};

fn create_tx_id() -> TransactionId {
    TransactionId::new()
}

#[test]
fn test_basic_queue_operations() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Test enqueue
    let enqueue1 = QueueOperation::Enqueue {
        value: QueueValue::Str("first".to_string()),
    };

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, enqueue1, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));
    engine.commit_batch(batch, 2);

    let enqueue2 = QueueOperation::Enqueue {
        value: QueueValue::Str("second".to_string()),
    };

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, enqueue2, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));
    engine.commit_batch(batch, 3);

    // Test size
    let size_op = QueueOperation::Size;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(2))
    ));
    engine.commit_batch(batch, 4);

    // Test dequeue (FIFO)
    let dequeue_op = QueueOperation::Dequeue;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "first"
    ));
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "second"
    ));
    engine.commit_batch(batch, 6);

    // Queue should be empty now
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, dequeue_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None))
    ));
    engine.commit_batch(batch, 7);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 8);
}

#[test]
fn test_transaction_isolation() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);

    let mut batch2 = engine.start_batch();
    engine.begin(&mut batch2, tx2);

    // tx1 dequeues a value (acquires exclusive lock - blocks other operations)
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(&mut batch1, dequeue_op, tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None)) // Empty queue
    ));

    // tx2 tries to read the size (should be blocked by tx1's exclusive lock)
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(&mut batch2, size_op.clone(), tx2);

    // Should be blocked because Exclusive (Dequeue) blocks Shared (Size)
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // Commit tx1 to release the exclusive lock
    engine.commit(&mut batch1, tx1);
    engine.commit_batch(batch1, 5);

    // tx2 can now read the size
    let result = engine.apply_operation(&mut batch2, size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));
    engine.commit(&mut batch2, tx2);
    engine.commit_batch(batch2, 7);

    // Start a new transaction - should also see empty queue
    let tx3 = create_tx_id();
    let mut batch3 = engine.start_batch();
    engine.begin(&mut batch3, tx3);

    let result = engine.apply_operation(&mut batch3, size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));

    engine.commit(&mut batch3, tx3);
    engine.commit_batch(batch3, 10);
}

#[test]
fn test_concurrent_access_with_locking() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 2);

    // tx1 acquires exclusive lock by enqueuing
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("tx1_data".to_string()),
    };

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, enqueue_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 3);

    // tx2 tries to dequeue - should be blocked
    let dequeue_op = QueueOperation::Dequeue;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, dequeue_op.clone(), tx2);
    assert!(matches!(result, OperationResult::WouldBlock { blockers } if !blockers.is_empty()));
    // Don't commit this batch since it was blocked

    // After tx1 commits, tx2 should be able to proceed
    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 4);

    // Now tx2 can dequeue
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, dequeue_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "tx1_data"
    ));
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 6);
}

#[test]
fn test_abort_rollback() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    // Enqueue some values
    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx1);
        engine.commit_batch(batch, 2 + i as u64);
    }

    // Check size before abort
    let size_op = QueueOperation::Size;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op.clone(), tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(3))
    ));
    engine.commit_batch(batch, 5);

    // Abort the transaction
    let mut batch = engine.start_batch();
    engine.abort(&mut batch, tx1);
    engine.commit_batch(batch, 6);

    // Start new transaction - should see empty queue
    let tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 7);

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));
    engine.commit_batch(batch, 8);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 9);
}

#[test]
fn test_peek_operation() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Enqueue a value
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("peek_me".to_string()),
    };

    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx);
    engine.commit_batch(batch, 2);

    // Peek should not remove the value
    let peek_op = QueueOperation::Peek;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, peek_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "peek_me"
    ));
    engine.commit_batch(batch, 3);

    // Peek again - should still be there
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, peek_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "peek_me"
    ));
    engine.commit_batch(batch, 4);

    // Size should still be 1
    let size_op = QueueOperation::Size;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 6);
}

#[test]
fn test_clear_operation() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Enqueue multiple values
    for i in 0..5 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx);
        engine.commit_batch(batch, 2 + i as u64);
    }

    // Verify size
    let size_op = QueueOperation::Size;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(5))
    ));
    engine.commit_batch(batch, 7);

    // Clear the queue
    let clear_op = QueueOperation::Clear;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, clear_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Cleared)
    ));
    engine.commit_batch(batch, 8);

    // Queue should be empty
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));
    engine.commit_batch(batch, 9);

    let is_empty_op = QueueOperation::IsEmpty;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, is_empty_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(true))
    ));
    engine.commit_batch(batch, 10);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 11);
}

#[test]
fn test_shared_locks_for_reads() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();
    let tx3 = create_tx_id();

    // First transaction enqueues and commits
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("shared_data".to_string()),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx1);
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 3);

    // Now two transactions try to read concurrently
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 4);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx3);
    engine.commit_batch(batch, 5);

    // Both should be able to peek (shared lock)
    let peek_op = QueueOperation::Peek;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, peek_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));
    engine.commit_batch(batch, 6);

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, peek_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));
    engine.commit_batch(batch, 7);

    // Both should be able to check size (shared lock)
    let size_op = QueueOperation::Size;

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));
    engine.commit_batch(batch, 8);

    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));
    engine.commit_batch(batch, 9);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 10);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx3);
    engine.commit_batch(batch, 11);
}

#[test]
fn test_various_value_types() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Test different value types
    let values = [
        QueueValue::Str("text".to_string()),
        QueueValue::I64(42),
        QueueValue::F64(std::f64::consts::PI),
        QueueValue::Bool(true),
        QueueValue::Bytea(vec![1, 2, 3, 4]),
        QueueValue::Json(serde_json::json!({"key": "value"})),
    ];

    // Enqueue all value types
    let mut lsn = 2;
    for value in values.iter() {
        let enqueue_op = QueueOperation::Enqueue {
            value: value.clone(),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx);
        engine.commit_batch(batch, lsn);
        lsn += 1;
    }

    // Dequeue and verify FIFO order
    for expected_value in values.iter() {
        let dequeue_op = QueueOperation::Dequeue;

        let mut batch = engine.start_batch();
        let result = engine.apply_operation(&mut batch, dequeue_op, tx);
        match result {
            OperationResult::Complete(QueueResponse::Dequeued(Some(value))) => {
                assert_eq!(&value, expected_value);
            }
            _ => panic!("Expected successful dequeue"),
        }
        engine.commit_batch(batch, lsn);
        lsn += 1;
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, lsn);
}

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);

    let mut batch2 = engine.start_batch();
    engine.begin(&mut batch2, tx2);

    // TX1: Peek queue1 (acquires shared lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(&mut batch1, peek_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to dequeue from queue1 (should be blocked - Exclusive vs Shared)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(&mut batch2, dequeue_op.clone(), tx2);

    // Should be blocked because Exclusive (Dequeue) conflicts with Shared (Peek)
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::Prepare); // Can retry after prepare
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (releases read lock)
    engine.prepare(&mut batch1, tx1);
    engine.commit_batch(batch1, 5);

    // TX2: Retry dequeue (should now succeed since read lock was released)
    let result = engine.apply_operation(&mut batch2, dequeue_op, tx2);
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);

    let mut batch2 = engine.start_batch();
    engine.begin(&mut batch2, tx2);

    // TX1: Dequeue from queue1 (acquires exclusive lock)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(&mut batch1, dequeue_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to peek queue1 (should be blocked by exclusive lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(&mut batch2, peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (exclusive lock should NOT be released)
    engine.prepare(&mut batch1, tx1);

    // TX2: Retry peek (should still be blocked)
    let result = engine.apply_operation(&mut batch2, peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }

    // TX1: Commit (should release exclusive lock)
    engine.commit(&mut batch1, tx1);
    engine.commit_batch(batch1, 7);

    // TX2: Retry peek (should now succeed)
    let result = engine.apply_operation(&mut batch2, peek_op, tx2);
    assert!(matches!(result, OperationResult::Complete(_)));
}

// ============================================================================
// Snapshot Read Tests
// ============================================================================

#[test]
fn test_snapshot_peek_doesnt_block_on_later_write() {
    let mut engine = QueueTransactionEngine::new();

    // Setup: Create a queue with some data
    let setup_tx = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, setup_tx);
    engine.commit_batch(batch, 1);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("initial".to_string()),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, setup_tx);
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, setup_tx);
    engine.commit_batch(batch, 3);

    // Capture snapshot AFTER setup_tx but BEFORE write_tx starts
    // This snapshot timestamp is EARLIER than write_tx so should NOT block
    let read_ts = create_tx_id();

    // Start a write transaction AFTER our read timestamp
    let write_tx = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx);
    engine.commit_batch(batch, 4);

    // Write transaction enqueues (gets append lock)
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("new_value".to_string()),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, enqueue_op, write_tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));
    engine.commit_batch(batch, 5);

    // Snapshot read should see only committed data without blocking
    // because read_ts < write_tx (read is earlier than the write)
    let peek_op = QueueOperation::Peek;
    let result = engine.read_at_timestamp(peek_op, read_ts);
    // Should see only committed data (initial value) without blocking
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "initial"
    ));
}

#[test]
fn test_snapshot_size_blocks_on_earlier_write() {
    let mut engine = QueueTransactionEngine::new();

    // Start a write transaction at timestamp 100
    let write_tx = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx);

    // Write transaction enqueues (but doesn't commit yet)
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("pending".to_string()),
    };
    engine.apply_operation(&mut batch, enqueue_op, write_tx);

    // Snapshot read at timestamp 200 (after write tx started) should block
    let read_ts = create_tx_id();
    let size_op = QueueOperation::Size;

    let result = engine.read_at_timestamp(size_op, read_ts);
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, write_tx);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock for snapshot read after pending write"),
    }
}

#[test]
fn test_snapshot_is_empty_sees_committed_state() {
    let mut engine = QueueTransactionEngine::new();
    let is_empty_op = QueueOperation::IsEmpty;

    // Capture snapshot BEFORE any operations
    let snapshot_before = create_tx_id();

    // Transaction: enqueue 3 items and commit
    let tx1 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx1);
        engine.commit_batch(batch, 2 + i as u64);
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 5);

    // Capture snapshot AFTER tx1
    let snapshot_after_enqueue = create_tx_id();

    // Transaction: dequeue one and commit
    let tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 6);

    let dequeue_op = QueueOperation::Dequeue;
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, dequeue_op, tx2);
    engine.commit_batch(batch, 7);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 8);

    // Capture snapshot AFTER tx2
    let snapshot_after_dequeue = create_tx_id();

    // Transaction: clear and commit
    let tx3 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx3);
    engine.commit_batch(batch, 9);

    let clear_op = QueueOperation::Clear;
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, clear_op, tx3);
    engine.commit_batch(batch, 10);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx3);
    engine.commit_batch(batch, 11);

    // Capture snapshot AFTER tx3
    let snapshot_after_clear = create_tx_id();

    // Read at snapshot before any data: should be empty
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_before);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(true))
    ));

    // Read at snapshot after enqueue: should not be empty (has 3 items)
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_after_enqueue);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(false))
    ));

    // Read at snapshot after dequeue: still not empty (2 items left)
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_after_dequeue);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(false))
    ));

    // Read at snapshot after clear: should be empty
    let result = engine.read_at_timestamp(is_empty_op, snapshot_after_clear);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(true))
    ));
}

#[test]
fn test_snapshot_peek_ignores_aborted_operations() {
    let mut engine = QueueTransactionEngine::new();

    // Transaction 1: enqueue and commit
    let tx1 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("committed".to_string()),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx1);
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 3);

    // Transaction 2: enqueue but abort
    let tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 4);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("aborted".to_string()),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx2);
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    engine.abort(&mut batch, tx2);
    engine.commit_batch(batch, 6);

    // Snapshot read at time 300 should only see committed value
    let peek_op = QueueOperation::Peek;
    let result = engine.read_at_timestamp(peek_op, create_tx_id());
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "committed"
    ));
}

#[test]
fn test_snapshot_size_with_concurrent_operations() {
    let mut engine = QueueTransactionEngine::new();

    // Initial state: 2 items committed
    let tx1 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    for i in 0..2 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx1);
        engine.commit_batch(batch, 2 + i as u64);
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 4);

    // Capture snapshot AFTER tx1 commits
    let snapshot_after_tx1 = create_tx_id();

    // Start transaction that will enqueue more (but not commit yet)
    let tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 5);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::I64(2),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx2);
    engine.commit_batch(batch, 6);

    // Capture snapshot AFTER tx2 starts (should block reading at this snapshot)
    let snapshot_during_tx2 = create_tx_id();

    // Start another transaction that also enqueues (not committed)
    let tx3 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx3);
    engine.commit_batch(batch, 7);

    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::I64(3),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, enqueue_op, tx3);
    engine.commit_batch(batch, 8);

    // Capture snapshot AFTER tx3 starts (should block on both)
    let snapshot_during_both = create_tx_id();

    // Snapshot read AFTER tx1 should see 2 items
    let size_op = QueueOperation::Size;
    let result = engine.read_at_timestamp(size_op.clone(), snapshot_after_tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(2))
    ));

    // Snapshot read after tx2 started should block on tx2
    let result = engine.read_at_timestamp(size_op.clone(), snapshot_during_tx2);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    // Snapshot read after both started should block on both tx2 and tx3
    let result = engine.read_at_timestamp(size_op, snapshot_during_both);
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 2);
            // Should contain both tx2 and tx3
            let blocking_txs: Vec<_> = blockers.iter().map(|b| b.txn).collect();
            assert!(blocking_txs.contains(&tx2));
            assert!(blocking_txs.contains(&tx3));
        }
        _ => panic!("Expected WouldBlock with 2 blockers"),
    }
}

#[test]
fn test_snapshot_fifo_ordering_preserved() {
    let mut engine = QueueTransactionEngine::new();
    let peek_op = QueueOperation::Peek;

    // Enqueue items at different times, capturing snapshots
    let mut snapshots = Vec::new();
    let mut lsn = 1;
    for i in 0..5 {
        let tx = create_tx_id();
        let mut batch = engine.start_batch();
        engine.begin(&mut batch, tx);
        engine.commit_batch(batch, lsn);
        lsn += 1;

        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i as i64),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, enqueue_op, tx);
        engine.commit_batch(batch, lsn);
        lsn += 1;

        let mut batch = engine.start_batch();
        engine.commit(&mut batch, tx);
        engine.commit_batch(batch, lsn);
        lsn += 1;

        snapshots.push(create_tx_id()); // Capture snapshot after each enqueue
    }

    // Dequeue some items
    let tx = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, lsn);
    lsn += 1;

    for _ in 0..2 {
        let dequeue_op = QueueOperation::Dequeue;
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, dequeue_op.clone(), tx);
        engine.commit_batch(batch, lsn);
        lsn += 1;
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, lsn);

    let snapshot_after_dequeues = create_tx_id();

    // Snapshot peek at different times should show proper FIFO order
    // After first enqueue: should see first item (0)
    let result = engine.read_at_timestamp(peek_op.clone(), snapshots[0]);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::I64(0))))
    ));

    // After third enqueue: should still see first item (0)
    let result = engine.read_at_timestamp(peek_op.clone(), snapshots[2]);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::I64(0))))
    ));

    // After dequeues: should see third item (2) after two dequeues
    let result = engine.read_at_timestamp(peek_op, snapshot_after_dequeues);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::I64(2))))
    ));
}
