//! Integration tests for the stream crate

use proven_common::TransactionId;
use proven_processor::{AutoBatchEngine, OperationResult};
use proven_stream::engine::StreamTransactionEngine;
use proven_stream::types::{StreamOperation, StreamResponse};
use proven_value::Value;

fn create_tx_id() -> TransactionId {
    TransactionId::new()
}

fn create_engine() -> AutoBatchEngine<StreamTransactionEngine> {
    AutoBatchEngine::new(StreamTransactionEngine::new())
}

#[test]
fn test_basic_append_and_read() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append values
    let append_op = StreamOperation::Append {
        values: vec![
            Value::Str("first".to_string()),
            Value::Str("second".to_string()),
        ],
    };

    let result = engine.apply_operation(append_op, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(StreamResponse::Appended)
    ));

    // Read from position 1 BEFORE commit - should be empty (commit-time sequencing)
    let read_op = StreamOperation::ReadFrom {
        position: 1,
        limit: 10,
    };

    let result = engine.apply_operation(read_op.clone(), tx);
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(
                entries.len(),
                0,
                "Uncommitted appends should not be visible"
            );
        }
        _ => panic!("Expected Read response, got {:?}", result),
    }

    engine.commit(tx);

    // Read AFTER commit - should see the data
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(read_op, tx2);
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, 1);
            assert_eq!(entries[0].1, Value::Str("first".to_string()));
            assert_eq!(entries[1].0, 2);
            assert_eq!(entries[1].1, Value::Str("second".to_string()));
        }
        _ => panic!("Expected Read response, got {:?}", result),
    }

    engine.commit(tx2);
}

#[test]
fn test_multiple_appends_in_transaction() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Multiple appends in same transaction
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(1), Value::I64(2)],
        },
        tx,
    );

    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(3), Value::I64(4)],
        },
        tx,
    );

    // Before commit - should NOT see uncommitted values
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Uncommitted appends not visible");
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx);

    // After commit - should see all 4 values
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 4);
            assert_eq!(entries[0], (1, Value::I64(1)));
            assert_eq!(entries[1], (2, Value::I64(2)));
            assert_eq!(entries[2], (3, Value::I64(3)));
            assert_eq!(entries[3], (4, Value::I64(4)));
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}

#[test]
fn test_append_not_visible_until_commit() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1);

    // tx1 appends
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::Str("tx1_data".to_string())],
        },
        tx1,
    );

    engine.begin(tx2);

    // tx2 should not see tx1's uncommitted append
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "tx2 should not see uncommitted data");
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);

    // Commit tx1
    engine.commit(tx1);

    // New transaction should now see the data
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx3,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], (1, Value::Str("tx1_data".to_string())));
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx3);
}

#[test]
fn test_append_disappears_on_abort() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();

    engine.begin(tx1);

    // Append values
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(1), Value::I64(2), Value::I64(3)],
        },
        tx1,
    );

    // Should NOT see uncommitted values in same transaction (commit-time sequencing)
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx1,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Uncommitted appends not visible");
        }
        _ => panic!("Expected Read response"),
    }

    // Abort the transaction
    engine.abort(tx1);

    // New transaction should see empty stream
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Aborted appends should disappear");
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}

#[test]
fn test_concurrent_appends() {
    let mut engine = create_engine();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    engine.begin(tx1);
    engine.begin(tx2);

    // Both transactions append
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::Str("tx1_data".to_string())],
        },
        tx1,
    );

    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::Str("tx2_data".to_string())],
        },
        tx2,
    );

    // Commit in order: tx1 then tx2
    engine.commit(tx1);
    engine.commit(tx2);

    // Verify consecutive positions (gap-free)
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx3,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 2);
            // Positions should be 1 and 2 (consecutive)
            assert_eq!(entries[0].0, 1);
            assert_eq!(entries[1].0, 2);
            // Values in commit order
            assert_eq!(entries[0].1, Value::Str("tx1_data".to_string()));
            assert_eq!(entries[1].1, Value::Str("tx2_data".to_string()));
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx3);
}

#[test]
fn test_read_from_various_positions() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append 10 values
    let values: Vec<Value> = (1..=10).map(Value::I64).collect();
    engine.apply_operation(StreamOperation::Append { values }, tx);

    engine.commit(tx);

    let tx2 = create_tx_id();
    engine.begin(tx2);

    // Read from position 1
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 3,
        },
        tx2,
    );
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], (1, Value::I64(1)));
            assert_eq!(entries[1], (2, Value::I64(2)));
            assert_eq!(entries[2], (3, Value::I64(3)));
        }
        _ => panic!("Expected Read response"),
    }

    // Read from middle
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 5,
            limit: 3,
        },
        tx2,
    );
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0], (5, Value::I64(5)));
            assert_eq!(entries[1], (6, Value::I64(6)));
            assert_eq!(entries[2], (7, Value::I64(7)));
        }
        _ => panic!("Expected Read response"),
    }

    // Read beyond end (should return partial result)
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 9,
            limit: 5,
        },
        tx2,
    );
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 2); // Only 9 and 10 exist
            assert_eq!(entries[0], (9, Value::I64(9)));
            assert_eq!(entries[1], (10, Value::I64(10)));
        }
        _ => panic!("Expected Read response"),
    }

    // Read from position 0 (empty - stream is 1-indexed)
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 0,
            limit: 5,
        },
        tx2,
    );
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Position 0 should return empty");
        }
        _ => panic!("Expected Read response"),
    }

    // Read from position beyond tail
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 100,
            limit: 5,
        },
        tx2,
    );
    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Beyond tail should return empty");
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}

#[test]
fn test_read_returns_position_value_tuples() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    engine.apply_operation(
        StreamOperation::Append {
            values: vec![
                Value::Str("a".to_string()),
                Value::Str("b".to_string()),
                Value::Str("c".to_string()),
            ],
        },
        tx,
    );

    engine.commit(tx);

    // Read after commit to see the data
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 3);
            // Verify tuples have correct positions
            assert_eq!(entries[0].0, 1);
            assert_eq!(entries[1].0, 2);
            assert_eq!(entries[2].0, 3);
            // Verify values
            assert_eq!(entries[0].1, Value::Str("a".to_string()));
            assert_eq!(entries[1].1, Value::Str("b".to_string()));
            assert_eq!(entries[2].1, Value::Str("c".to_string()));
        }
        _ => panic!("Expected Read response with tuples"),
    }

    engine.commit(tx2);
}

#[test]
fn test_get_latest_position() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Empty stream should return 0
    let result = engine.apply_operation(StreamOperation::GetLatestPosition, tx);
    assert!(matches!(
        result,
        OperationResult::Complete(StreamResponse::Position(0))
    ));

    // Append 5 values
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(1), Value::I64(2), Value::I64(3)],
        },
        tx,
    );

    engine.commit(tx);

    // New transaction should see latest position = 3
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(StreamOperation::GetLatestPosition, tx2);
    assert!(matches!(
        result,
        OperationResult::Complete(StreamResponse::Position(3))
    ));

    // Append more
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(4), Value::I64(5)],
        },
        tx2,
    );

    engine.commit(tx2);

    // Latest should now be 5
    let tx3 = create_tx_id();
    engine.begin(tx3);

    let result = engine.apply_operation(StreamOperation::GetLatestPosition, tx3);
    assert!(matches!(
        result,
        OperationResult::Complete(StreamResponse::Position(5))
    ));

    engine.commit(tx3);
}

#[test]
fn test_empty_append() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append empty vector
    let result = engine.apply_operation(StreamOperation::Append { values: vec![] }, tx);

    assert!(matches!(
        result,
        OperationResult::Complete(StreamResponse::Appended)
    ));

    // Stream should still be empty
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0);
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx);
}

#[test]
fn test_large_batch_append() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append 1000 values
    let values: Vec<Value> = (1..=1000).map(Value::I64).collect();
    engine.apply_operation(StreamOperation::Append { values }, tx);

    engine.commit(tx);

    // Verify all values are present with consecutive positions
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 1000,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 1000);
            // Verify positions are consecutive
            for (idx, (pos, val)) in entries.iter().enumerate() {
                assert_eq!(*pos, (idx + 1) as u64);
                assert_eq!(*val, Value::I64((idx + 1) as i64));
            }
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}

#[test]
fn test_interleaved_reads_and_appends() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append first batch
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(1), Value::I64(2)],
        },
        tx,
    );

    // Read BEFORE commit - should NOT see uncommitted appends (commit-time sequencing)
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Uncommitted appends not visible");
        }
        _ => panic!("Expected Read response"),
    }

    // Append more
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![Value::I64(3), Value::I64(4)],
        },
        tx,
    );

    // Read again before commit - still should NOT see uncommitted
    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 0, "Uncommitted appends not visible");
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx);

    // After commit - should see all values
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 4);
            assert_eq!(entries[0], (1, Value::I64(1)));
            assert_eq!(entries[1], (2, Value::I64(2)));
            assert_eq!(entries[2], (3, Value::I64(3)));
            assert_eq!(entries[3], (4, Value::I64(4)));
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}

#[test]
fn test_various_value_types() {
    let mut engine = create_engine();
    let tx = create_tx_id();

    engine.begin(tx);

    // Append different value types
    engine.apply_operation(
        StreamOperation::Append {
            values: vec![
                Value::I64(42),
                Value::Str("hello".to_string()),
                Value::Bool(true),
                Value::F64(std::f64::consts::PI),
            ],
        },
        tx,
    );

    engine.commit(tx);

    // Read after commit to see the values
    let tx2 = create_tx_id();
    engine.begin(tx2);

    let result = engine.apply_operation(
        StreamOperation::ReadFrom {
            position: 1,
            limit: 10,
        },
        tx2,
    );

    match result {
        OperationResult::Complete(StreamResponse::Read { entries }) => {
            assert_eq!(entries.len(), 4);
            assert_eq!(entries[0].1, Value::I64(42));
            assert_eq!(entries[1].1, Value::Str("hello".to_string()));
            assert_eq!(entries[2].1, Value::Bool(true));
            assert_eq!(entries[3].1, Value::F64(std::f64::consts::PI));
        }
        _ => panic!("Expected Read response"),
    }

    engine.commit(tx2);
}
