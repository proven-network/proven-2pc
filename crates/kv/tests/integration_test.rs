//! Integration tests for the KV engine

use proven_hlc::{HlcTimestamp, NodeId};
use proven_kv::{KvOperation, KvResponse, KvTransactionEngine, Value};
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

fn timestamp(n: u64) -> HlcTimestamp {
    HlcTimestamp::new(n, 0, NodeId::new(0))
}

// ============================================================================
// Basic KV Operations Tests
// ============================================================================

#[test]
fn test_basic_get_put_delete() {
    let mut engine = KvTransactionEngine::new();
    let tx = timestamp(100);

    engine.begin(tx, 1);

    // Test PUT
    let put_op = KvOperation::Put {
        key: "test_key".to_string(),
        value: Value::String("test_value".to_string()),
    };
    let result = engine.apply_operation(put_op, tx, 2);
    match result {
        OperationResult::Complete(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, None);
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }

    // Test GET - should retrieve the value
    let get_op = KvOperation::Get {
        key: "test_key".to_string(),
    };
    let result = engine.apply_operation(get_op.clone(), tx, 3);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "test_key");
            assert_eq!(value, Some(Value::String("test_value".to_string())));
        }
        _ => panic!("Expected successful get, got {:?}", result),
    }

    // Test PUT with overwrite
    let put_op = KvOperation::Put {
        key: "test_key".to_string(),
        value: Value::String("new_value".to_string()),
    };
    let result = engine.apply_operation(put_op, tx, 4);
    match result {
        OperationResult::Complete(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, Some(Value::String("test_value".to_string())));
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }

    // Test DELETE
    let delete_op = KvOperation::Delete {
        key: "test_key".to_string(),
    };
    let result = engine.apply_operation(delete_op, tx, 5);
    match result {
        OperationResult::Complete(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "test_key");
            assert!(deleted);
        }
        _ => panic!("Expected successful delete, got {:?}", result),
    }

    // Test GET after DELETE - should return None
    let result = engine.apply_operation(get_op, tx, 6);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "test_key");
            assert_eq!(value, None);
        }
        _ => panic!("Expected successful get, got {:?}", result),
    }

    engine.commit(tx, 7);
}

#[test]
fn test_transaction_isolation() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);

    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Put a value
    let put_op = KvOperation::Put {
        key: "isolated_key".to_string(),
        value: Value::Integer(42),
    };
    let result = engine.apply_operation(put_op, tx1, 3);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to read the same key (should be blocked due to write lock)
    let get_op = KvOperation::Get {
        key: "isolated_key".to_string(),
    };
    let result = engine.apply_operation(get_op.clone(), tx2, 4);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    // Commit TX1
    engine.commit(tx1, 5);

    // TX2: Now should be able to read
    let result = engine.apply_operation(get_op, tx2, 6);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "isolated_key");
            assert_eq!(value, Some(Value::Integer(42)));
        }
        _ => panic!("Expected successful get after commit"),
    }

    engine.commit(tx2, 7);
}

#[test]
fn test_transaction_abort_rollback() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);

    engine.begin(tx1, 1);

    // Put some values
    for i in 0..3 {
        let put_op = KvOperation::Put {
            key: format!("key_{}", i),
            value: Value::Integer(i as i64),
        };
        engine.apply_operation(put_op, tx1, 2);
    }

    // Abort the transaction
    engine.abort(tx1, 3);

    // Start new transaction - should not see aborted values
    let tx2 = timestamp(200);
    engine.begin(tx2, 4);

    for i in 0..3 {
        let get_op = KvOperation::Get {
            key: format!("key_{}", i),
        };
        let result = engine.apply_operation(get_op, tx2, 5);
        match result {
            OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, None);
            }
            _ => panic!("Expected successful get"),
        }
    }

    engine.commit(tx2, 6);
}

#[test]
fn test_different_value_types() {
    let mut engine = KvTransactionEngine::new();
    let tx = timestamp(100);

    engine.begin(tx, 1);

    // Test different value types
    let test_cases = vec![
        ("string_key", Value::String("hello world".to_string())),
        ("int_key", Value::Integer(123456)),
        ("float_key", Value::Float("3.14159".to_string())),
        ("bool_key", Value::Boolean(true)),
        ("bytes_key", Value::Bytes(vec![1, 2, 3, 4, 5])),
    ];

    // Put all values
    for (key, value) in &test_cases {
        let put_op = KvOperation::Put {
            key: key.to_string(),
            value: value.clone(),
        };
        engine.apply_operation(put_op, tx, 2);
    }

    // Get and verify all values
    for (key, expected_value) in &test_cases {
        let get_op = KvOperation::Get {
            key: key.to_string(),
        };
        let result = engine.apply_operation(get_op, tx, 3);
        match result {
            OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, Some(expected_value.clone()));
            }
            _ => panic!("Expected successful get for key {}", key),
        }
    }

    engine.commit(tx, 4);
}

#[test]
fn test_concurrent_reads_with_shared_locks() {
    let mut engine = KvTransactionEngine::new();

    // First, create some data
    let tx_setup = timestamp(50);
    engine.begin(tx_setup, 1);
    let put_op = KvOperation::Put {
        key: "shared_key".to_string(),
        value: Value::String("shared_data".to_string()),
    };
    engine.apply_operation(put_op, tx_setup, 2);
    engine.commit(tx_setup, 3);

    // Now test concurrent reads
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);
    let tx3 = timestamp(300);

    engine.begin(tx1, 4);
    engine.begin(tx2, 5);
    engine.begin(tx3, 6);

    // All transactions should be able to read concurrently (shared locks)
    let get_op = KvOperation::Get {
        key: "shared_key".to_string(),
    };

    let result1 = engine.apply_operation(get_op.clone(), tx1, 7);
    assert!(matches!(result1, OperationResult::Complete(_)));

    let result2 = engine.apply_operation(get_op.clone(), tx2, 8);
    assert!(matches!(result2, OperationResult::Complete(_)));

    let result3 = engine.apply_operation(get_op, tx3, 9);
    assert!(matches!(result3, OperationResult::Complete(_)));

    engine.commit(tx1, 10);
    engine.commit(tx2, 11);
    engine.commit(tx3, 12);
}

#[test]
fn test_write_write_conflict() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);

    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Write to key
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::String("value1".to_string()),
    };
    let result = engine.apply_operation(put_op, tx1, 3);
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to write to same key (should be blocked)
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::String("value2".to_string()),
    };
    let result = engine.apply_operation(put_op.clone(), tx2, 4);
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock for write-write conflict"),
    }

    // Commit TX1
    engine.commit(tx1, 5);

    // TX2: Retry write (should now succeed)
    let result = engine.apply_operation(put_op, tx2, 6);
    assert!(matches!(result, OperationResult::Complete(_)));

    engine.commit(tx2, 7);
}

#[test]
fn test_delete_non_existent_key() {
    let mut engine = KvTransactionEngine::new();
    let tx = timestamp(100);

    engine.begin(tx, 1);

    // Delete non-existent key
    let delete_op = KvOperation::Delete {
        key: "non_existent".to_string(),
    };
    let result = engine.apply_operation(delete_op, tx, 2);
    match result {
        OperationResult::Complete(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "non_existent");
            assert!(!deleted); // Should indicate nothing was deleted
        }
        _ => panic!("Expected successful delete operation"),
    }

    engine.commit(tx, 3);
}

// ============================================================================
// Prepare/Commit Tests (Read Lock Release)
// ============================================================================

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Read key1 (acquires shared lock)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
        3,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to write to key1 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value2".to_string()),
        },
        tx2,
        4,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::Prepare); // Can retry after prepare
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (should release read lock)
    engine.prepare(tx1, 5);

    // TX2: Retry write (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value2".to_string()),
        },
        tx2,
        6,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Write to key1 (acquires exclusive lock)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        },
        tx1,
        3,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Try to read key1 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
        4,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (write lock should NOT be released)
    engine.prepare(tx1, 5);

    // TX2: Retry read (should still be blocked)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
        6,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }

    // TX1: Commit (should release write lock)
    engine.commit(tx1, 7);

    // TX2: Retry read (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
        8,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "key1");
            assert_eq!(value, Some(Value::String("value1".to_string())));
        }
        _ => panic!("Expected successful read after commit, got {:?}", result),
    }
}

#[test]
fn test_multiple_reads_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Read multiple keys
    for i in 1..=3 {
        let result = engine.apply_operation(
            KvOperation::Get {
                key: format!("key{}", i),
            },
            tx1,
            3,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
    }

    // TX2: Try to write to key2 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("new_value".to_string()),
        },
        tx2,
        4,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::Prepare);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (should release all read locks)
    engine.prepare(tx1, 5);

    // TX2: Retry write to key2 (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("new_value".to_string()),
        },
        tx2,
        6,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Also write to key1 and key3 (should succeed)
    for key in ["key1", "key3"] {
        let result = engine.apply_operation(
            KvOperation::Put {
                key: key.to_string(),
                value: Value::String("updated".to_string()),
            },
            tx2,
            7,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
    }
}

#[test]
fn test_mixed_locks_partial_release() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin(tx1, 1);
    engine.begin(tx2, 2);

    // TX1: Read key1 (shared lock)
    engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
        3,
    );

    // TX1: Write key2 (exclusive lock)
    engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("value".to_string()),
        },
        tx1,
        4,
    );

    // TX1: Read key3 (shared lock)
    engine.apply_operation(
        KvOperation::Get {
            key: "key3".to_string(),
        },
        tx1,
        5,
    );

    // TX2: Try to write key1 (blocked by read lock)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("new".to_string()),
        },
        tx2,
        6,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::Prepare)
    ));

    // TX2: Try to read key2 (blocked by write lock)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
        7,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::CommitOrAbort)
    ));

    // TX1: Prepare (releases read locks on key1 and key3, keeps write lock on key2)
    engine.prepare(tx1, 8);

    // TX2: Retry write to key1 (should succeed - read lock released)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("new".to_string()),
        },
        tx2,
        8,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Write to key3 (should succeed - read lock released)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key3".to_string(),
            value: Value::String("new3".to_string()),
        },
        tx2,
        9,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Retry read of key2 (should still be blocked - write lock not released)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
        10,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::CommitOrAbort)
    ));
}

// ============================================================================
// Snapshot Read Tests (read_at_timestamp)
// ============================================================================

#[test]
fn test_snapshot_read_doesnt_block_write() {
    let mut engine = KvTransactionEngine::new();
    let read_ts = timestamp(2);
    let write_tx = timestamp(3);

    // Begin write transaction
    engine.begin(write_tx, 1);

    // Write to key1
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("write_value".to_string()),
        },
        write_tx,
        2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Snapshot read at earlier timestamp - should NOT be blocked
    // (write is from timestamp 3, read is at timestamp 2)
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        3,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            // Should see no value since write hasn't committed and is from a later timestamp
            assert_eq!(value, None);
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}

#[test]
fn test_snapshot_read_blocks_on_earlier_write() {
    let mut engine = KvTransactionEngine::new();
    let write_tx = timestamp(1);
    let read_ts = timestamp(2);

    // Begin write transaction at earlier timestamp
    engine.begin(write_tx, 1);

    // Write to key1
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("write_value".to_string()),
        },
        write_tx,
        2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Snapshot read at later timestamp - SHOULD be blocked
    // (write is from timestamp 1, read is at timestamp 2)
    // The read needs to wait to see if the write commits
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        3,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, write_tx);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // Commit the write
    engine.commit(write_tx, 4);

    // Retry the snapshot read - should now succeed and see the written value
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        5,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::String("write_value".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}

#[test]
fn test_snapshot_read_doesnt_take_locks() {
    let mut engine = KvTransactionEngine::new();
    let read_ts = timestamp(1);
    let write_tx = timestamp(2);

    // Perform a snapshot read first
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        1,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Begin write transaction
    engine.begin(write_tx, 2);

    // Write to the same key - should NOT be blocked by the snapshot read
    // (snapshot reads don't take locks)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("write_value".to_string()),
        },
        write_tx,
        3,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
}

#[test]
fn test_multiple_snapshot_reads_concurrent() {
    let mut engine = KvTransactionEngine::new();
    let read_ts1 = timestamp(1);
    let read_ts2 = timestamp(2);
    let read_ts3 = timestamp(3);

    // Set up initial data
    let setup_tx = timestamp(0);
    engine.begin(setup_tx, 1);
    engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("initial_value".to_string()),
        },
        setup_tx,
        2,
    );
    engine.commit(setup_tx, 3);

    // Multiple snapshot reads at different timestamps - all should succeed
    // without blocking each other
    let result1 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts1,
        4,
    );
    let result2 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts2,
        5,
    );
    let result3 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts3,
        6,
    );

    // All should complete successfully
    assert!(matches!(result1, OperationResult::Complete(_)));
    assert!(matches!(result2, OperationResult::Complete(_)));
    assert!(matches!(result3, OperationResult::Complete(_)));
}

#[test]
fn test_snapshot_read_sees_committed_writes() {
    let mut engine = KvTransactionEngine::new();

    // Write and commit at timestamp 1
    let write_tx1 = timestamp(1);
    engine.begin(write_tx1, 1);
    engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        },
        write_tx1,
        2,
    );
    engine.commit(write_tx1, 3);

    // Write and commit at timestamp 3
    let write_tx2 = timestamp(3);
    engine.begin(write_tx2, 4);
    engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value2".to_string()),
        },
        write_tx2,
        5,
    );
    engine.commit(write_tx2, 6);

    // Snapshot read at timestamp 2 should see value1 (not value2)
    let read_ts = timestamp(2);
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        7,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::String("value1".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }

    // Snapshot read at timestamp 4 should see value2
    let read_ts2 = timestamp(4);
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts2,
        8,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::String("value2".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}

#[test]
fn test_snapshot_read_ignores_aborted_writes() {
    let mut engine = KvTransactionEngine::new();

    // Write at timestamp 1 but abort
    let write_tx1 = timestamp(1);
    engine.begin(write_tx1, 1);
    engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("aborted_value".to_string()),
        },
        write_tx1,
        2,
    );
    engine.abort(write_tx1, 3);

    // Snapshot read at timestamp 2 should NOT see the aborted write
    let read_ts = timestamp(2);
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
        4,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, None);
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}
