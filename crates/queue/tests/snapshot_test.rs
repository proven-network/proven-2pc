//! Tests for Queue engine snapshot functionality

use proven_hlc::{HlcTimestamp, NodeId};
use proven_queue::stream::engine::QueueTransactionEngine;
use proven_queue::stream::operation::QueueOperation;
use proven_queue::types::QueueValue;
use proven_stream::{OperationResult, TransactionEngine};

#[test]
fn test_queue_snapshot_and_restore() {
    // Create engine and add some queue data
    let mut engine1 = QueueTransactionEngine::new();

    // Begin transaction and add data
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine1.begin(txn1);

    // Enqueue some values to different queues
    let op1 = QueueOperation::Enqueue {
        queue_name: "orders".to_string(),
        value: QueueValue::String("order-123".to_string()),
    };
    let result = engine1.apply_operation(op1, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    let op2 = QueueOperation::Enqueue {
        queue_name: "orders".to_string(),
        value: QueueValue::String("order-456".to_string()),
    };
    let result = engine1.apply_operation(op2, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    let op3 = QueueOperation::Enqueue {
        queue_name: "notifications".to_string(),
        value: QueueValue::Integer(42),
    };
    let result = engine1.apply_operation(op3, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Commit transaction
    engine1.prepare(txn1);
    engine1.commit(txn1);

    // Take a snapshot
    let snapshot = engine1.snapshot().unwrap();
    assert!(!snapshot.is_empty());

    // Create a new engine and restore
    let mut engine2 = QueueTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify data was restored by dequeuing
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    // Dequeue from orders queue
    let dequeue1 = QueueOperation::Dequeue {
        queue_name: "orders".to_string(),
    };
    let result = engine2.apply_operation(dequeue1, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"Dequeued(Some(String("order-123")))"#
        );
    } else {
        panic!("Expected success");
    }

    // Check queue size
    let size_op = QueueOperation::Size {
        queue_name: "orders".to_string(),
    };
    let result = engine2.apply_operation(size_op, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(format!("{:?}", response), "Size(1)");
    }

    // Check notifications queue
    let peek_op = QueueOperation::Peek {
        queue_name: "notifications".to_string(),
    };
    let result = engine2.apply_operation(peek_op, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(format!("{:?}", response), "Peeked(Some(Integer(42)))");
    }
}

#[test]
fn test_snapshot_with_active_transaction_fails() {
    let mut engine = QueueTransactionEngine::new();

    // Begin a transaction
    let txn = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn);

    // Try to snapshot with active transaction
    let result = engine.snapshot();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("active transactions"));

    // Commit the transaction
    engine.commit(txn);

    // Now snapshot should succeed
    let result = engine.snapshot();
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_with_empty_queues() {
    let mut engine = QueueTransactionEngine::new();

    // Add data then dequeue everything
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    // Enqueue values
    let op1 = QueueOperation::Enqueue {
        queue_name: "temp".to_string(),
        value: QueueValue::String("temp1".to_string()),
    };
    engine.apply_operation(op1, txn1);

    let op2 = QueueOperation::Enqueue {
        queue_name: "temp".to_string(),
        value: QueueValue::String("temp2".to_string()),
    };
    engine.apply_operation(op2, txn1);

    engine.prepare(txn1);
    engine.commit(txn1);

    // Dequeue everything in a new transaction
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin(txn2);

    let dequeue1 = QueueOperation::Dequeue {
        queue_name: "temp".to_string(),
    };
    engine.apply_operation(dequeue1, txn2);

    let dequeue2 = QueueOperation::Dequeue {
        queue_name: "temp".to_string(),
    };
    engine.apply_operation(dequeue2, txn2);

    engine.prepare(txn2);
    engine.commit(txn2);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // Restore to new engine
    let mut engine2 = QueueTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify queue is empty
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin(txn3);

    let is_empty = QueueOperation::IsEmpty {
        queue_name: "temp".to_string(),
    };
    let result = engine2.apply_operation(is_empty, txn3);
    if let OperationResult::Complete(response) = result {
        assert_eq!(format!("{:?}", response), "IsEmpty(true)");
    }
}

#[test]
fn test_snapshot_compression() {
    let mut engine = QueueTransactionEngine::new();

    // Add a lot of repetitive data to test compression
    let txn = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn);

    // Create data with good compression potential
    let long_string = "A".repeat(1000);
    for i in 0..100 {
        let op = QueueOperation::Enqueue {
            queue_name: format!("queue{}", i % 10), // 10 different queues
            value: QueueValue::String(long_string.clone()),
        };
        engine.apply_operation(op, txn);
    }

    engine.prepare(txn);
    engine.commit(txn);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // The compressed snapshot should be much smaller than the raw data
    // 100 entries * 1000 chars = 100KB uncompressed
    // With compression, should be much smaller
    println!("Snapshot size: {} bytes", snapshot.len());
    assert!(snapshot.len() < 50000, "Snapshot should be compressed");

    // Verify it can be restored
    let mut engine2 = QueueTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Spot check a queue
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    let size_op = QueueOperation::Size {
        queue_name: "queue0".to_string(),
    };
    let result = engine2.apply_operation(size_op, txn2);
    if let OperationResult::Complete(response) = result {
        // queue0 should have 10 entries (i % 10 == 0 for i = 0, 10, 20, ..., 90)
        assert_eq!(format!("{:?}", response), "Size(10)");
    }
}

#[test]
fn test_queue_ordering_preserved_in_snapshot() {
    let mut engine = QueueTransactionEngine::new();

    // Create a queue with specific ordering
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    for i in 1..=5 {
        let op = QueueOperation::Enqueue {
            queue_name: "ordered".to_string(),
            value: QueueValue::Integer(i),
        };
        engine.apply_operation(op, txn1);
    }

    engine.prepare(txn1);
    engine.commit(txn1);

    // Snapshot and restore
    let snapshot = engine.snapshot().unwrap();
    let mut engine2 = QueueTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify order is preserved by dequeuing
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    for expected in 1..=5 {
        let dequeue = QueueOperation::Dequeue {
            queue_name: "ordered".to_string(),
        };
        let result = engine2.apply_operation(dequeue, txn2);
        if let OperationResult::Complete(response) = result {
            assert_eq!(
                format!("{:?}", response),
                format!("Dequeued(Some(Integer({})))", expected)
            );
        }
    }

    println!("Queue ordering preserved across snapshot/restore");
}
