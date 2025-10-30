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

    engine.begin(tx, 1);

    // Test enqueue
    let enqueue1 = QueueOperation::Enqueue {
        value: QueueValue::Str("first".to_string()),
    };

    let result = engine.apply_operation(enqueue1, tx, 2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    let enqueue2 = QueueOperation::Enqueue {
        value: QueueValue::Str("second".to_string()),
    };

    let result = engine.apply_operation(enqueue2, tx, 3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // Test size
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op, tx, 4);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(2))
    ));

    // Test dequeue (FIFO)
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(dequeue_op.clone(), tx, 5);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "first"
    ));

    let result = engine.apply_operation(dequeue_op.clone(), tx, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "second"
    ));

    // Queue should be empty now
    let result = engine.apply_operation(dequeue_op, tx, 7);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None))
    ));

    engine.commit(tx, 8);
}

#[test]
fn test_transaction_isolation() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // tx1 dequeues a value (acquires exclusive lock - blocks other operations)
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(dequeue_op, tx1, 3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None)) // Empty queue
    ));

    // tx2 tries to read the size (should be blocked by tx1's exclusive lock)
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx2, 4);

    // Should be blocked because Exclusive (Dequeue) blocks Shared (Size)
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // Commit tx1 to release the exclusive lock
    engine.commit(tx1, 5);

    // tx2 can now read the size
    let result = engine.apply_operation(size_op.clone(), tx2, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));
    engine.commit(tx2, 7);

    // Start a new transaction - should also see empty queue
    let tx3 = create_tx_id();
    engine.begin(tx3, 8);

    let result = engine.apply_operation(size_op, tx3, 9);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));

    engine.commit(tx3, 10);
}

#[test]
fn test_concurrent_access_with_locking() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // tx1 acquires exclusive lock by enqueuing
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("tx1_data".to_string()),
    };

    let result = engine.apply_operation(enqueue_op, tx1, 3);
    assert!(matches!(result, OperationResult::Complete(_)));

    // tx2 tries to dequeue - should be blocked
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(dequeue_op.clone(), tx2, 4);
    assert!(matches!(result, OperationResult::WouldBlock { blockers } if !blockers.is_empty()));

    // After tx1 commits, tx2 should be able to proceed
    engine.commit(tx1, 5);

    // Now tx2 can dequeue
    let result = engine.apply_operation(dequeue_op, tx2, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(QueueValue::Str(s)))) if s == "tx1_data"
    ));

    engine.commit(tx2, 7);
}

#[test]
fn test_abort_rollback() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();

    engine.begin(tx1, 1);

    // Enqueue some values
    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1, 2);
    }

    // Check size before abort
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx1, 3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(3))
    ));

    // Abort the transaction
    engine.abort(tx1, 4);

    // Start new transaction - should see empty queue
    let tx2 = create_tx_id();
    engine.begin(tx2, 5);

    let result = engine.apply_operation(size_op, tx2, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));

    engine.commit(tx2, 7);
}

#[test]
fn test_peek_operation() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    engine.begin(tx, 1);

    // Enqueue a value
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("peek_me".to_string()),
    };

    engine.apply_operation(enqueue_op, tx, 2);

    // Peek should not remove the value
    let peek_op = QueueOperation::Peek;

    let result = engine.apply_operation(peek_op.clone(), tx, 3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "peek_me"
    ));

    // Peek again - should still be there
    let result = engine.apply_operation(peek_op, tx, 4);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(QueueValue::Str(s)))) if s == "peek_me"
    ));

    // Size should still be 1
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op, tx, 5);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    engine.commit(tx, 6);
}

#[test]
fn test_clear_operation() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    engine.begin(tx, 1);

    // Enqueue multiple values
    for i in 0..5 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        engine.apply_operation(enqueue_op, tx, 2);
    }

    // Verify size
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx, 3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(5))
    ));

    // Clear the queue
    let clear_op = QueueOperation::Clear;

    let result = engine.apply_operation(clear_op, tx, 4);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Cleared)
    ));

    // Queue should be empty
    let result = engine.apply_operation(size_op, tx, 5);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));

    let is_empty_op = QueueOperation::IsEmpty;

    let result = engine.apply_operation(is_empty_op, tx, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(true))
    ));

    engine.commit(tx, 7);
}

#[test]
fn test_shared_locks_for_reads() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();
    let tx3 = create_tx_id();

    // First transaction enqueues and commits
    engine.begin(tx1, 1);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("shared_data".to_string()),
    };
    engine.apply_operation(enqueue_op, tx1, 2);
    engine.commit(tx1, 3);

    // Now two transactions try to read concurrently
    engine.begin(tx2, 4);
    engine.begin(tx3, 5);

    // Both should be able to peek (shared lock)
    let peek_op = QueueOperation::Peek;

    let result = engine.apply_operation(peek_op.clone(), tx2, 6);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));

    let result = engine.apply_operation(peek_op, tx3, 7);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));

    // Both should be able to check size (shared lock)
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx2, 8);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    let result = engine.apply_operation(size_op, tx3, 9);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    engine.commit(tx2, 10);
    engine.commit(tx3, 11);
}

#[test]
fn test_various_value_types() {
    let mut engine = QueueTransactionEngine::new();
    let tx = create_tx_id();

    engine.begin(tx, 1);

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
    for value in values.iter() {
        let enqueue_op = QueueOperation::Enqueue {
            value: value.clone(),
        };
        engine.apply_operation(enqueue_op, tx, 2);
    }

    // Dequeue and verify FIFO order
    for expected_value in values.iter() {
        let dequeue_op = QueueOperation::Dequeue;

        let result = engine.apply_operation(dequeue_op, tx, 3);
        match result {
            OperationResult::Complete(QueueResponse::Dequeued(Some(value))) => {
                assert_eq!(&value, expected_value);
            }
            _ => panic!("Expected successful dequeue"),
        }
    }

    engine.commit(tx, 4);
}

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Peek queue1 (acquires shared lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(peek_op, tx1, 3);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to dequeue from queue1 (should be blocked - Exclusive vs Shared)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(dequeue_op.clone(), tx2, 4);

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
    engine.prepare(tx1, 5);

    // TX2: Retry dequeue (should now succeed since read lock was released)
    let result = engine.apply_operation(dequeue_op, tx2, 6);
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = QueueTransactionEngine::new();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Dequeue from queue1 (acquires exclusive lock)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(dequeue_op, tx1, 3);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to peek queue1 (should be blocked by exclusive lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(peek_op.clone(), tx2, 4);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (exclusive lock should NOT be released)
    engine.prepare(tx1, 5);

    // TX2: Retry peek (should still be blocked)
    let result = engine.apply_operation(peek_op.clone(), tx2, 6);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }

    // TX1: Commit (should release exclusive lock)
    engine.commit(tx1, 7);

    // TX2: Retry peek (should now succeed)
    let result = engine.apply_operation(peek_op, tx2, 8);
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
    engine.begin(setup_tx, 1);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("initial".to_string()),
    };
    engine.apply_operation(enqueue_op, setup_tx, 2);
    engine.commit(setup_tx, 3);

    // Capture snapshot AFTER setup_tx but BEFORE write_tx starts
    // This snapshot timestamp is EARLIER than write_tx so should NOT block
    let read_ts = create_tx_id();

    // Start a write transaction AFTER our read timestamp
    let write_tx = create_tx_id();
    engine.begin(write_tx, 4);

    // Write transaction enqueues (gets append lock)
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("new_value".to_string()),
    };
    let result = engine.apply_operation(enqueue_op, write_tx, 5);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

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
    engine.begin(write_tx, 1);

    // Write transaction enqueues (but doesn't commit yet)
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("pending".to_string()),
    };
    engine.apply_operation(enqueue_op, write_tx, 2);

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
    engine.begin(tx1, 1);
    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1, 2);
    }
    engine.commit(tx1, 3);

    // Capture snapshot AFTER tx1
    let snapshot_after_enqueue = create_tx_id();

    // Transaction: dequeue one and commit
    let tx2 = create_tx_id();
    engine.begin(tx2, 4);
    let dequeue_op = QueueOperation::Dequeue;
    engine.apply_operation(dequeue_op, tx2, 5);
    engine.commit(tx2, 6);

    // Capture snapshot AFTER tx2
    let snapshot_after_dequeue = create_tx_id();

    // Transaction: clear and commit
    let tx3 = create_tx_id();
    engine.begin(tx3, 7);
    let clear_op = QueueOperation::Clear;
    engine.apply_operation(clear_op, tx3, 8);
    engine.commit(tx3, 9);

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
    engine.begin(tx1, 1);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("committed".to_string()),
    };
    engine.apply_operation(enqueue_op, tx1, 2);
    engine.commit(tx1, 3);

    // Transaction 2: enqueue but abort
    let tx2 = create_tx_id();
    engine.begin(tx2, 4);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::Str("aborted".to_string()),
    };
    engine.apply_operation(enqueue_op, tx2, 5);
    engine.abort(tx2, 6);

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
    engine.begin(tx1, 1);
    for i in 0..2 {
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1, 2);
    }
    engine.commit(tx1, 3);

    // Capture snapshot AFTER tx1 commits
    let snapshot_after_tx1 = create_tx_id();

    // Start transaction that will enqueue more (but not commit yet)
    let tx2 = create_tx_id();
    engine.begin(tx2, 4);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::I64(2),
    };
    engine.apply_operation(enqueue_op, tx2, 5);

    // Capture snapshot AFTER tx2 starts (should block reading at this snapshot)
    let snapshot_during_tx2 = create_tx_id();

    // Start another transaction that also enqueues (not committed)
    let tx3 = create_tx_id();
    engine.begin(tx3, 6);
    let enqueue_op = QueueOperation::Enqueue {
        value: QueueValue::I64(3),
    };
    engine.apply_operation(enqueue_op, tx3, 7);

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
    for i in 0..5 {
        let tx = create_tx_id();
        engine.begin(tx, 1);
        let enqueue_op = QueueOperation::Enqueue {
            value: QueueValue::I64(i as i64),
        };
        engine.apply_operation(enqueue_op, tx, 2);
        engine.commit(tx, 3);
        snapshots.push(create_tx_id()); // Capture snapshot after each enqueue
    }

    // Dequeue some items
    let tx = create_tx_id();
    engine.begin(tx, 4);
    for _ in 0..2 {
        let dequeue_op = QueueOperation::Dequeue;
        engine.apply_operation(dequeue_op.clone(), tx, 5);
    }
    engine.commit(tx, 6);
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
