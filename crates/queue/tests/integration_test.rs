//! Integration tests for the queue crate

use proven_common::TransactionId;
use proven_processor::AutoBatchEngine;
use proven_processor::OperationResult;
use proven_processor::RetryOn;
use proven_queue::engine::QueueTransactionEngine;
use proven_queue::types::{QueueOperation, QueueResponse};
use proven_value::Value;

fn create_tx_id() -> TransactionId {
    TransactionId::new()
}

fn create_engine() -> AutoBatchEngine<QueueTransactionEngine> {
    AutoBatchEngine::new(QueueTransactionEngine::new())
}

#[test]
fn test_basic_queue_operations() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Test enqueue
    let enqueue1 = QueueOperation::Enqueue {
        value: Value::Str("first".to_string()),
    };

    let result = engine.apply_operation(enqueue1, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    let enqueue2 = QueueOperation::Enqueue {
        value: Value::Str("second".to_string()),
    };

    let result = engine.apply_operation(enqueue2, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // Test size
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(2))
    ));

    // Test dequeue (FIFO)
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) if s == "first"
    ));

    let result = engine.apply_operation(dequeue_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) if s == "second"
    ));

    // Queue should be empty now
    let result = engine.apply_operation(dequeue_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None))
    ));

    engine.commit(tx);
}

#[test]
fn test_transaction_isolation() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1);

    engine.begin(tx2);

    // tx1 dequeues a value (acquires exclusive lock - blocks other operations)
    let dequeue_op = QueueOperation::Dequeue;

    let result = engine.apply_operation(dequeue_op, tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(None)) // Empty queue
    ));

    // tx2 tries to read the size (should be blocked by tx1's exclusive lock)
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx2);

    // Should be blocked because Exclusive (Dequeue) blocks Shared (Size)
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // Commit tx1 to release the exclusive lock
    engine.commit(tx1);

    // tx2 can now read the size
    let result = engine.apply_operation(size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));
    engine.commit(tx2);

    // Start a new transaction - should also see empty queue
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let result = engine.apply_operation(size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0)) // Still empty after dequeue
    ));

    engine.commit(tx3);
}

#[test]
fn test_concurrent_access_with_locking() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1);
    engine.begin(tx2);

    // tx1 enqueues (LOCK-FREE - no longer blocks other enqueues!)
    let enqueue_op1 = QueueOperation::Enqueue {
        value: Value::Str("tx1_data".to_string()),
    };
    let result = engine.apply_operation(enqueue_op1, tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // tx2 enqueues - should succeed immediately (concurrent append!)
    let enqueue_op2 = QueueOperation::Enqueue {
        value: Value::Str("tx2_data".to_string()),
    };
    let result = engine.apply_operation(enqueue_op2, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // Commit both transactions
    engine.commit(tx1);
    engine.commit(tx2);

    // Verify both values were enqueued in order
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(dequeue_op.clone(), tx3);
    match &result {
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) => {
            assert_eq!(
                s, "tx1_data",
                "First dequeue should return tx1_data (seq=1), got: {}",
                s
            );
        }
        _ => panic!("Expected Dequeued(Some(tx1_data)), got {:?}", result),
    }

    let result = engine.apply_operation(dequeue_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) if s == "tx2_data"
    ));

    engine.commit(tx3);
}

#[test]
fn test_abort_rollback() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();

    engine.begin(tx1);

    // Enqueue some values
    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: Value::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1);
    }

    // Check size before abort
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(3))
    ));

    // Abort the transaction
    engine.abort(tx1);

    // Start new transaction - should see empty queue
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(size_op, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));

    engine.commit(tx2);
}

#[test]
fn test_peek_operation() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Enqueue a value
    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("peek_me".to_string()),
    };

    engine.apply_operation(enqueue_op, tx);

    // Peek should not remove the value
    let peek_op = QueueOperation::Peek;

    let result = engine.apply_operation(peek_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(Value::Str(s)))) if s == "peek_me"
    ));

    // Peek again - should still be there
    let result = engine.apply_operation(peek_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(Some(Value::Str(s)))) if s == "peek_me"
    ));

    // Size should still be 1
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    engine.commit(tx);
}

#[test]
fn test_clear_operation() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Enqueue multiple values
    for i in 0..5 {
        let enqueue_op = QueueOperation::Enqueue {
            value: Value::I64(i),
        };
        engine.apply_operation(enqueue_op, tx);
    }

    // Verify size
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(5))
    ));

    // Clear the queue
    let clear_op = QueueOperation::Clear;

    let result = engine.apply_operation(clear_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Cleared)
    ));

    // Queue should be empty
    let result = engine.apply_operation(size_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(0))
    ));

    let is_empty_op = QueueOperation::IsEmpty;

    let result = engine.apply_operation(is_empty_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::IsEmpty(true))
    ));

    engine.commit(tx);
}

#[test]
fn test_shared_locks_for_reads() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();
    let tx3 = create_tx_id();

    // First transaction enqueues and commits
    engine.begin(tx1);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("shared_data".to_string()),
    };
    engine.apply_operation(enqueue_op, tx1);

    engine.commit(tx1);

    // Now two transactions try to read concurrently
    engine.begin(tx2);

    engine.begin(tx3);

    // Both should be able to peek (shared lock)
    let peek_op = QueueOperation::Peek;

    let result = engine.apply_operation(peek_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));

    let result = engine.apply_operation(peek_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Peeked(_))
    ));

    // Both should be able to check size (shared lock)
    let size_op = QueueOperation::Size;

    let result = engine.apply_operation(size_op.clone(), tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    let result = engine.apply_operation(size_op, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Size(1))
    ));

    engine.commit(tx2);

    engine.commit(tx3);
}

#[test]
fn test_various_value_types() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Test different value types
    let values = [
        Value::Str("text".to_string()),
        Value::I64(42),
        Value::F64(std::f64::consts::PI),
        Value::Bool(true),
        Value::Bytea(vec![1, 2, 3, 4]),
        Value::Json(serde_json::json!({"key": "value"})),
    ];

    // Enqueue all value types
    for value in values.iter() {
        let enqueue_op = QueueOperation::Enqueue {
            value: value.clone(),
        };
        engine.apply_operation(enqueue_op, tx);
    }

    // Dequeue and verify FIFO order
    for expected_value in values.iter() {
        let dequeue_op = QueueOperation::Dequeue;

        let result = engine.apply_operation(dequeue_op, tx);
        match result {
            OperationResult::Complete(QueueResponse::Dequeued(Some(value))) => {
                assert_eq!(&value, expected_value);
            }
            _ => panic!("Expected successful dequeue"),
        }
    }

    engine.commit(tx);
}

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = create_engine();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    engine.begin(tx1);

    engine.begin(tx2);

    // TX1: Peek queue1 (acquires shared lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(peek_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to dequeue from queue1 (should be blocked - Exclusive vs Shared)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(dequeue_op.clone(), tx2);

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
    engine.prepare(tx1);

    // TX2: Retry dequeue (should now succeed since read lock was released)
    let result = engine.apply_operation(dequeue_op, tx2);
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = create_engine();
    let tx1 = TransactionId::new();
    let tx2 = TransactionId::new();

    // Begin both transactions
    engine.begin(tx1);

    engine.begin(tx2);

    // TX1: Dequeue from queue1 (acquires exclusive lock)
    let dequeue_op = QueueOperation::Dequeue;
    let result = engine.apply_operation(dequeue_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to peek queue1 (should be blocked by exclusive lock)
    let peek_op = QueueOperation::Peek;
    let result = engine.apply_operation(peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (exclusive lock should NOT be released)
    engine.prepare(tx1);

    // TX2: Retry peek (should still be blocked)
    let result = engine.apply_operation(peek_op.clone(), tx2);

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }

    // TX1: Commit (should release exclusive lock)
    engine.commit(tx1);

    // TX2: Retry peek (should now succeed)
    let result = engine.apply_operation(peek_op, tx2);
    assert!(matches!(result, OperationResult::Complete(_)));
}

// ============================================================================
// Snapshot Read Tests
// ============================================================================

#[test]
fn test_snapshot_peek_doesnt_block_on_later_write() {
    let mut engine = create_engine();

    // Setup: Create a queue with some data
    let setup_tx = create_tx_id();
    engine.begin(setup_tx);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("initial".to_string()),
    };
    engine.apply_operation(enqueue_op, setup_tx);

    engine.commit(setup_tx);

    // Capture snapshot AFTER setup_tx but BEFORE write_tx starts
    // This snapshot timestamp is EARLIER than write_tx so should NOT block
    let read_ts = create_tx_id();

    // Start a write transaction AFTER our read timestamp
    let write_tx = create_tx_id();
    engine.begin(write_tx);

    // Write transaction enqueues (gets append lock)
    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("new_value".to_string()),
    };
    let result = engine.apply_operation(enqueue_op, write_tx);
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
        QueueResponse::Peeked(Some(Value::Str(s))) if s == "initial"
    ));
}

#[test]
fn test_snapshot_size_with_uncommitted_write() {
    let mut engine = create_engine();

    // Start a write transaction
    let write_tx = create_tx_id();
    engine.begin(write_tx);

    // Write transaction enqueues (but doesn't commit yet)
    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("pending".to_string()),
    };
    engine.apply_operation(enqueue_op, write_tx);

    // Snapshot read at timestamp after write tx started
    // With lock-free enqueues, this NO LONGER blocks because enqueues don't hold locks
    // MVCC handles visibility - the snapshot won't see uncommitted data
    let read_ts = create_tx_id();
    let size_op = QueueOperation::Size;

    let result = engine.read_at_timestamp(size_op.clone(), read_ts);
    // Should succeed (not block) and see size=0 because the enqueue hasn't committed
    assert!(matches!(result, QueueResponse::Size(0)));

    // Now commit the write transaction
    engine.commit(write_tx);

    // A new snapshot read should now see the committed item
    let read_ts2 = create_tx_id();
    let result = engine.read_at_timestamp(size_op.clone(), read_ts2);
    assert!(matches!(result, QueueResponse::Size(1)));
}

#[test]
fn test_snapshot_is_empty_sees_committed_state() {
    let mut engine = create_engine();
    let is_empty_op = QueueOperation::IsEmpty;

    // Capture snapshot BEFORE any operations
    let snapshot_before = create_tx_id();
    println!("snapshot_before: {:?}", snapshot_before);

    // Transaction: enqueue 3 items and commit
    let tx1 = create_tx_id();
    println!("tx1: {:?}", tx1);
    engine.begin(tx1);

    for i in 0..3 {
        let enqueue_op = QueueOperation::Enqueue {
            value: Value::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1);
    }

    engine.commit(tx1);

    // Small delay to ensure timestamp ordering (UUIDv7 sub-millisecond randomness)
    std::thread::sleep(std::time::Duration::from_millis(2));

    // Capture snapshot AFTER tx1
    let snapshot_after_enqueue = create_tx_id();
    println!("snapshot_after_enqueue: {:?}", snapshot_after_enqueue);

    // Transaction: dequeue one and commit
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let dequeue_op = QueueOperation::Dequeue;
    engine.apply_operation(dequeue_op, tx2);

    engine.commit(tx2);

    // Small delay to ensure timestamp ordering
    std::thread::sleep(std::time::Duration::from_millis(2));

    // Capture snapshot AFTER tx2
    let snapshot_after_dequeue = create_tx_id();

    // Transaction: clear and commit
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let clear_op = QueueOperation::Clear;
    engine.apply_operation(clear_op, tx3);

    engine.commit(tx3);

    // Capture snapshot AFTER tx3
    let snapshot_after_clear = create_tx_id();

    // Read at snapshot before any data: should be empty
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_before);
    assert!(matches!(result, QueueResponse::IsEmpty(true)));

    // Read at snapshot after enqueue: should not be empty (has 3 items)
    println!("snapshot_after_enqueue: {:?}", snapshot_after_enqueue);
    println!("About to read at snapshot...");
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_after_enqueue);
    println!("Result: {:?}", result);
    assert!(matches!(result, QueueResponse::IsEmpty(false)));

    // Read at snapshot after dequeue: still not empty (2 items left)
    let result = engine.read_at_timestamp(is_empty_op.clone(), snapshot_after_dequeue);
    assert!(matches!(result, QueueResponse::IsEmpty(false)));

    // Read at snapshot after clear: should be empty
    let result = engine.read_at_timestamp(is_empty_op, snapshot_after_clear);
    assert!(matches!(result, QueueResponse::IsEmpty(true)));
}

#[test]
fn test_snapshot_peek_ignores_aborted_operations() {
    let mut engine = create_engine();

    // Transaction 1: enqueue and commit
    let tx1 = create_tx_id();
    engine.begin(tx1);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("committed".to_string()),
    };
    engine.apply_operation(enqueue_op, tx1);

    engine.commit(tx1);

    // Transaction 2: enqueue but abort
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::Str("aborted".to_string()),
    };
    engine.apply_operation(enqueue_op, tx2);

    engine.abort(tx2);

    // Snapshot read at time 300 should only see committed value
    let peek_op = QueueOperation::Peek;
    let result = engine.read_at_timestamp(peek_op, create_tx_id());
    assert!(matches!(
        result,
        QueueResponse::Peeked(Some(Value::Str(s))) if s == "committed"
    ));
}

#[test]
fn test_snapshot_size_with_concurrent_operations() {
    let mut engine = create_engine();

    // Initial state: 2 items committed
    let tx1 = create_tx_id();
    engine.begin(tx1);

    for i in 0..2 {
        let enqueue_op = QueueOperation::Enqueue {
            value: Value::I64(i),
        };
        engine.apply_operation(enqueue_op, tx1);
    }

    engine.commit(tx1);

    // Capture snapshot AFTER tx1 commits
    let snapshot_after_tx1 = create_tx_id();

    // Start transaction that will enqueue more (but not commit yet)
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::I64(2),
    };
    engine.apply_operation(enqueue_op, tx2);

    // Capture snapshot AFTER tx2 starts
    let snapshot_during_tx2 = create_tx_id();

    // Start another transaction that also enqueues (both succeed concurrently!)
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let enqueue_op = QueueOperation::Enqueue {
        value: Value::I64(3),
    };
    engine.apply_operation(enqueue_op, tx3);

    // Capture snapshot AFTER tx3 starts
    let snapshot_during_both = create_tx_id();

    // Snapshot read AFTER tx1 should see 2 items
    let size_op = QueueOperation::Size;
    let result = engine.read_at_timestamp(size_op.clone(), snapshot_after_tx1);
    assert!(matches!(result, QueueResponse::Size(2)));

    // Snapshot read after tx2 started should NOT block (enqueues are lock-free)
    // MVCC ensures snapshot isolation - uncommitted data is not visible
    let result = engine.read_at_timestamp(size_op.clone(), snapshot_during_tx2);
    assert!(matches!(result, QueueResponse::Size(2)));

    // Snapshot read after tx3 should also see only committed data
    let result = engine.read_at_timestamp(size_op, snapshot_during_both);
    assert!(matches!(result, QueueResponse::Size(2)));
}

#[test]
fn test_snapshot_fifo_ordering_preserved() {
    let mut engine = create_engine();
    let peek_op = QueueOperation::Peek;

    // Enqueue items at different times, capturing snapshots
    let mut snapshots = Vec::new();
    for i in 0..5 {
        let tx = create_tx_id();
        engine.begin(tx);
        let enqueue_op = QueueOperation::Enqueue {
            value: Value::I64(i as i64),
        };
        engine.apply_operation(enqueue_op, tx);

        engine.commit(tx);
        snapshots.push(create_tx_id()); // Capture snapshot after each enqueue
    }

    // Dequeue some items
    let tx = create_tx_id();
    engine.begin(tx);
    for _ in 0..2 {
        let dequeue_op = QueueOperation::Dequeue;
        engine.apply_operation(dequeue_op.clone(), tx);
    }

    engine.commit(tx);
    let snapshot_after_dequeues = create_tx_id();

    // Snapshot peek at different times should show proper FIFO order
    // After first enqueue: should see first item (0)
    let result = engine.read_at_timestamp(peek_op.clone(), snapshots[0]);
    assert!(matches!(result, QueueResponse::Peeked(Some(Value::I64(0)))));

    // After third enqueue: should still see first item (0)
    let result = engine.read_at_timestamp(peek_op.clone(), snapshots[2]);
    assert!(matches!(result, QueueResponse::Peeked(Some(Value::I64(0)))));

    // After dequeues: should see third item (2) after two dequeues
    let result = engine.read_at_timestamp(peek_op, snapshot_after_dequeues);
    assert!(matches!(result, QueueResponse::Peeked(Some(Value::I64(2)))));
}

#[test]
fn test_out_of_order_commits_maintain_sequence_order() {
    // This test verifies the critical scenario from LINKED_LIST_LINKING_ANALYSIS.md
    // Two transactions enqueue concurrently, but commit in reverse order.
    // The dequeue order MUST follow sequence numbers (1, 2), not commit order (2, 1).

    let mut engine = create_engine();

    // Create two transactions with controlled ordering
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1);
    engine.begin(tx2);

    // tx1 enqueues "A" (will get seq=1)
    let enqueue1 = QueueOperation::Enqueue {
        value: Value::Str("A".to_string()),
    };
    let result = engine.apply_operation(enqueue1, tx1);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // tx2 enqueues "B" (will get seq=2)
    let enqueue2 = QueueOperation::Enqueue {
        value: Value::Str("B".to_string()),
    };
    let result = engine.apply_operation(enqueue2, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(QueueResponse::Enqueued)
    ));

    // CRITICAL: tx2 commits FIRST (out of order!)
    engine.commit(tx2);

    // tx1 commits SECOND
    engine.commit(tx1);

    // Now dequeue - should get items in SEQUENCE order (A, B), not commit order (B, A)
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let dequeue_op = QueueOperation::Dequeue;

    // First dequeue should return "A" (seq=1), not "B" (seq=2)
    let result = engine.apply_operation(dequeue_op.clone(), tx3);
    match result {
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) => {
            assert_eq!(
                s, "A",
                "First dequeue should return seq=1 (A), not seq=2 (B)"
            );
        }
        _ => panic!("Expected successful dequeue of 'A', got {:?}", result),
    }

    // Second dequeue should return "B" (seq=2)
    let result = engine.apply_operation(dequeue_op, tx3);
    match result {
        OperationResult::Complete(QueueResponse::Dequeued(Some(Value::Str(s)))) => {
            assert_eq!(s, "B", "Second dequeue should return seq=2 (B)");
        }
        _ => panic!("Expected successful dequeue of 'B', got {:?}", result),
    }

    engine.commit(tx3);
}

#[test]
fn test_out_of_order_commits_with_gaps() {
    // More complex scenario: tx1, tx3 commit; tx2 commits last
    // Should maintain order: seq1 -> seq2 -> seq3

    let mut engine = create_engine();

    let tx1 = create_tx_id();
    let tx2 = create_tx_id();
    let tx3 = create_tx_id();

    engine.begin(tx1);
    engine.begin(tx2);
    engine.begin(tx3);

    // Enqueue three items
    let result = engine.apply_operation(
        QueueOperation::Enqueue {
            value: Value::I64(1),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    let result = engine.apply_operation(
        QueueOperation::Enqueue {
            value: Value::I64(2),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    let result = engine.apply_operation(
        QueueOperation::Enqueue {
            value: Value::I64(3),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Commit out of order: tx1, tx3, then tx2
    engine.commit(tx1);
    engine.commit(tx3);

    // At this point: seq=1 committed, seq=2 uncommitted (gap), seq=3 committed
    // Dequeue should block waiting for seq=2
    let tx_dequeue = create_tx_id();
    engine.begin(tx_dequeue);

    let result = engine.apply_operation(QueueOperation::Dequeue, tx_dequeue);
    // Should be blocked by uncommitted seq=2
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx2);
        }
        _ => panic!("Expected WouldBlock due to gap at seq=2, got {:?}", result),
    }

    engine.abort(tx_dequeue);

    // Now commit tx2 to fill the gap
    engine.commit(tx2);

    // Dequeue should now succeed and return items in order: 1, 2, 3
    let tx_verify = create_tx_id();
    engine.begin(tx_verify);

    for expected in [1, 2, 3] {
        let result = engine.apply_operation(QueueOperation::Dequeue, tx_verify);
        match result {
            OperationResult::Complete(QueueResponse::Dequeued(Some(Value::I64(v)))) => {
                assert_eq!(v, expected, "Dequeue should return seq={}", expected);
            }
            _ => panic!("Expected dequeue of {}, got {:?}", expected, result),
        }
    }

    engine.commit(tx_verify);
}
