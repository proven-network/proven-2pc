//! Integration tests for the KV engine

use proven_common::TransactionId;
use proven_kv::{KvOperation, KvResponse, KvTransactionEngine, Value};
use proven_stream::{OperationResult, RetryOn, TransactionEngine};

fn create_tx_id() -> TransactionId {
    TransactionId::new()
}

// ============================================================================
// Basic KV Operations Tests
// ============================================================================

#[test]
fn test_basic_get_put_delete() {
    let mut engine = KvTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Test PUT
    let put_op = KvOperation::Put {
        key: "test_key".to_string(),
        value: Value::Str("test_value".to_string()),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op, tx);
    match result {
        OperationResult::Complete(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, None);
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }
    engine.commit_batch(batch, 2);

    // Test GET - should retrieve the value
    let get_op = KvOperation::Get {
        key: "test_key".to_string(),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, get_op.clone(), tx);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "test_key");
            assert_eq!(value, Some(Value::Str("test_value".to_string())));
        }
        _ => panic!("Expected successful get, got {:?}", result),
    }
    engine.commit_batch(batch, 3);

    // Test PUT with overwrite
    let put_op = KvOperation::Put {
        key: "test_key".to_string(),
        value: Value::Str("new_value".to_string()),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op, tx);
    match result {
        OperationResult::Complete(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, Some(Value::Str("test_value".to_string())));
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }
    engine.commit_batch(batch, 4);

    // Test DELETE
    let delete_op = KvOperation::Delete {
        key: "test_key".to_string(),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, delete_op, tx);
    match result {
        OperationResult::Complete(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "test_key");
            assert!(deleted);
        }
        _ => panic!("Expected successful delete, got {:?}", result),
    }
    engine.commit_batch(batch, 5);

    // Test GET after DELETE - should return None
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, get_op, tx);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "test_key");
            assert_eq!(value, None);
        }
        _ => panic!("Expected successful get, got {:?}", result),
    }
    engine.commit_batch(batch, 6);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 7);
}

#[test]
fn test_transaction_isolation() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 2);

    // TX1: Put a value
    let put_op = KvOperation::Put {
        key: "isolated_key".to_string(),
        value: Value::I64(42),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 3);

    // TX2: Try to read the same key (should be blocked due to write lock)
    let get_op = KvOperation::Get {
        key: "isolated_key".to_string(),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, get_op.clone(), tx2);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));
    engine.commit_batch(batch, 4);

    // Commit TX1
    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 5);

    // TX2: Now should be able to read
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, get_op, tx2);
    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "isolated_key");
            assert_eq!(value, Some(Value::I64(42)));
        }
        _ => panic!("Expected successful get after commit"),
    }
    engine.commit_batch(batch, 6);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 7);
}

#[test]
fn test_transaction_abort_rollback() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    // Put some values
    for i in 0..3 {
        let put_op = KvOperation::Put {
            key: format!("key_{}", i),
            value: Value::I64(i as i64),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, put_op, tx1);
        engine.commit_batch(batch, 2);
    }

    // Abort the transaction
    let mut batch = engine.start_batch();
    engine.abort(&mut batch, tx1);
    engine.commit_batch(batch, 3);

    // Start new transaction - should not see aborted values
    let tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 4);

    for i in 0..3 {
        let get_op = KvOperation::Get {
            key: format!("key_{}", i),
        };
        let mut batch = engine.start_batch();
        let result = engine.apply_operation(&mut batch, get_op, tx2);
        match result {
            OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, None);
            }
            _ => panic!("Expected successful get"),
        }
        engine.commit_batch(batch, 5);
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 6);
}

#[test]
fn test_different_value_types() {
    let mut engine = KvTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Test different value types
    let test_cases = vec![
        ("string_key", Value::Str("hello world".to_string())),
        ("int_key", Value::I64(123456)),
        ("float_key", Value::F64(std::f64::consts::PI)),
        ("bool_key", Value::Bool(true)),
        ("bytes_key", Value::Bytea(vec![1, 2, 3, 4, 5])),
    ];

    // Put all values
    for (key, value) in &test_cases {
        let put_op = KvOperation::Put {
            key: key.to_string(),
            value: value.clone(),
        };
        let mut batch = engine.start_batch();
        engine.apply_operation(&mut batch, put_op, tx);
        engine.commit_batch(batch, 2);
    }

    // Get and verify all values
    for (key, expected_value) in &test_cases {
        let get_op = KvOperation::Get {
            key: key.to_string(),
        };
        let mut batch = engine.start_batch();
        let result = engine.apply_operation(&mut batch, get_op, tx);
        match result {
            OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, Some(expected_value.clone()));
            }
            _ => panic!("Expected successful get for key {}", key),
        }
        engine.commit_batch(batch, 3);
    }

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 4);
}

#[test]
fn test_concurrent_reads_with_shared_locks() {
    let mut engine = KvTransactionEngine::new();

    // First, create some data
    let tx_setup = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx_setup);
    engine.commit_batch(batch, 1);

    let put_op = KvOperation::Put {
        key: "shared_key".to_string(),
        value: Value::Str("shared_data".to_string()),
    };
    let mut batch = engine.start_batch();
    engine.apply_operation(&mut batch, put_op, tx_setup);
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx_setup);
    engine.commit_batch(batch, 3);

    // Now test concurrent reads
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();
    let tx3 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 4);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx3);
    engine.commit_batch(batch, 6);

    // All transactions should be able to read concurrently (shared locks)
    let get_op = KvOperation::Get {
        key: "shared_key".to_string(),
    };

    let mut batch = engine.start_batch();
    let result1 = engine.apply_operation(&mut batch, get_op.clone(), tx1);
    assert!(matches!(result1, OperationResult::Complete(_)));
    engine.commit_batch(batch, 7);

    let mut batch = engine.start_batch();
    let result2 = engine.apply_operation(&mut batch, get_op.clone(), tx2);
    assert!(matches!(result2, OperationResult::Complete(_)));
    engine.commit_batch(batch, 8);

    let mut batch = engine.start_batch();
    let result3 = engine.apply_operation(&mut batch, get_op, tx3);
    assert!(matches!(result3, OperationResult::Complete(_)));
    engine.commit_batch(batch, 9);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 10);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 11);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx3);
    engine.commit_batch(batch, 12);
}

#[test]
fn test_write_write_conflict() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 2);

    // TX1: Write to key
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::Str("value1".to_string()),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op, tx1);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 3);

    // TX2: Try to write to same key (should be blocked)
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::Str("value2".to_string()),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op.clone(), tx2);
    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock for write-write conflict"),
    }
    engine.commit_batch(batch, 4);

    // Commit TX1
    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 5);

    // TX2: Retry write (should now succeed)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, put_op, tx2);
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 6);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 7);
}

#[test]
fn test_delete_non_existent_key() {
    let mut engine = KvTransactionEngine::new();
    let tx = create_tx_id();

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx);
    engine.commit_batch(batch, 1);

    // Delete non-existent key
    let delete_op = KvOperation::Delete {
        key: "non_existent".to_string(),
    };
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(&mut batch, delete_op, tx);
    match result {
        OperationResult::Complete(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "non_existent");
            assert!(!deleted); // Should indicate nothing was deleted
        }
        _ => panic!("Expected successful delete operation"),
    }
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx);
    engine.commit_batch(batch, 3);
}

// ============================================================================
// Prepare/Commit Tests (Read Lock Release)
// ============================================================================

#[test]
fn test_read_lock_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    // Begin both transactions
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 2);

    // TX1: Read key1 (acquires shared lock)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 3);

    // TX2: Try to write to key1 (should be blocked)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("value2".to_string()),
        },
        tx2,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::Prepare); // Can retry after prepare
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }
    engine.commit_batch(batch, 4);

    // TX1: Prepare (should release read lock)
    let mut batch = engine.start_batch();
    engine.prepare(&mut batch, tx1);
    engine.commit_batch(batch, 5);

    // TX2: Retry write (should now succeed)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("value2".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 6);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 7);
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    // Begin both transactions
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.begin(&mut batch, tx2);
    engine.commit_batch(batch, 2);

    // TX1: Write to key1 (acquires exclusive lock)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("value1".to_string()),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 3);

    // TX2: Try to read key1 (should be blocked)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort); // Must wait for commit/abort
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }
    engine.commit_batch(batch, 4);

    // TX1: Prepare (write lock should NOT be released)
    let mut batch = engine.start_batch();
    engine.prepare(&mut batch, tx1);
    engine.commit_batch(batch, 5);

    // TX2: Retry read (should still be blocked)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

    match result {
        OperationResult::WouldBlock { blockers } => {
            assert_eq!(blockers.len(), 1);
            assert_eq!(blockers[0].txn, tx1);
            assert_eq!(blockers[0].retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock after prepare, got {:?}", result),
    }
    engine.commit_batch(batch, 6);

    // TX1: Commit (should release write lock)
    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx1);
    engine.commit_batch(batch, 7);

    // TX2: Retry read (should now succeed)
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "key1");
            assert_eq!(value, Some(Value::Str("value1".to_string())));
        }
        _ => panic!("Expected successful read after commit, got {:?}", result),
    }
    engine.commit_batch(batch, 8);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, tx2);
    engine.commit_batch(batch, 9);
}

#[test]
fn test_multiple_reads_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    // Begin both transactions
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    let mut batch2 = engine.start_batch();
    engine.begin(&mut batch2, tx2);

    // TX1: Read multiple keys
    for i in 1..=3 {
        let result = engine.apply_operation(
            &mut batch1,
            KvOperation::Get {
                key: format!("key{}", i),
            },
            tx1,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
    }

    // TX2: Try to write to key2 (should be blocked)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::Str("new_value".to_string()),
        },
        tx2,
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
    engine.prepare(&mut batch1, tx1);
    engine.commit_batch(batch1, 5);

    // TX2: Retry write to key2 (should now succeed)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::Str("new_value".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Also write to key1 and key3 (should succeed)
    for key in ["key1", "key3"] {
        let result = engine.apply_operation(
            &mut batch2,
            KvOperation::Put {
                key: key.to_string(),
                value: Value::Str("updated".to_string()),
            },
            tx2,
        );
        assert!(matches!(result, OperationResult::Complete(_)));
    }
    engine.commit(&mut batch2, tx2);
    engine.commit_batch(batch2, 7);
}

#[test]
fn test_mixed_locks_partial_release() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = create_tx_id();
    let tx2 = create_tx_id();

    // Begin both transactions
    let mut batch1 = engine.start_batch();
    engine.begin(&mut batch1, tx1);
    let mut batch2 = engine.start_batch();
    engine.begin(&mut batch2, tx2);

    // TX1: Read key1 (shared lock)
    engine.apply_operation(
        &mut batch1,
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
    );

    // TX1: Write key2 (exclusive lock)
    engine.apply_operation(
        &mut batch1,
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::Str("value".to_string()),
        },
        tx1,
    );

    // TX1: Read key3 (shared lock)
    engine.apply_operation(
        &mut batch1,
        KvOperation::Get {
            key: "key3".to_string(),
        },
        tx1,
    );

    // TX2: Try to write key1 (blocked by read lock)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("new".to_string()),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::Prepare)
    ));

    // TX2: Try to read key2 (blocked by write lock)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::CommitOrAbort)
    ));

    // TX1: Prepare (releases read locks on key1 and key3, keeps write lock on key2)
    engine.prepare(&mut batch1, tx1);
    engine.commit_batch(batch1, 8);

    // TX2: Retry write to key1 (should succeed - read lock released)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("new".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Write to key3 (should succeed - read lock released)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Put {
            key: "key3".to_string(),
            value: Value::Str("new3".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // TX2: Retry read of key2 (should still be blocked - write lock not released)
    let result = engine.apply_operation(
        &mut batch2,
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock { blockers }
        if blockers.iter().any(|b| b.retry_on == RetryOn::CommitOrAbort)
    ));
    engine.commit(&mut batch2, tx2);
    engine.commit_batch(batch2, 10);
}

// ============================================================================
// Snapshot Read Tests (read_at_timestamp)
// ============================================================================

#[test]
fn test_snapshot_read_doesnt_block_write() {
    let mut engine = KvTransactionEngine::new();
    let read_ts = create_tx_id();
    let write_tx = create_tx_id();

    // Begin write transaction
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx);

    // Write to key1
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("write_value".to_string()),
        },
        write_tx,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Snapshot read at earlier timestamp - should NOT be blocked
    // (write is from timestamp 3, read is at timestamp 2)
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
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
    let write_tx = create_tx_id();
    let read_ts = create_tx_id();

    // Begin write transaction at earlier timestamp
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx);
    engine.commit_batch(batch, 1);

    // Write to key1
    let mut batch = engine.start_batch();
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("write_value".to_string()),
        },
        write_tx,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit_batch(batch, 2);

    // Snapshot read at later timestamp - SHOULD be blocked
    // (write is from timestamp 1, read is at timestamp 2)
    // The read needs to wait to see if the write commits
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
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
    let mut batch = engine.start_batch();
    engine.commit(&mut batch, write_tx);
    engine.commit_batch(batch, 4);

    // Retry the snapshot read - should now succeed and see the written value
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::Str("write_value".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}

#[test]
fn test_snapshot_read_doesnt_take_locks() {
    let mut engine = KvTransactionEngine::new();
    let read_ts = create_tx_id();
    let write_tx = create_tx_id();

    // Perform a snapshot read first
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Begin write transaction
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx);

    // Write to the same key - should NOT be blocked by the snapshot read
    // (snapshot reads don't take locks)
    let result = engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("write_value".to_string()),
        },
        write_tx,
    );
    assert!(matches!(result, OperationResult::Complete(_)));
    engine.commit(&mut batch, write_tx);
    engine.commit_batch(batch, 3);
}

#[test]
fn test_multiple_snapshot_reads_concurrent() {
    let mut engine = KvTransactionEngine::new();
    let read_ts1 = create_tx_id();
    let read_ts2 = create_tx_id();
    let read_ts3 = create_tx_id();

    // Set up initial data
    let setup_tx = create_tx_id();
    let mut batch_setup = engine.start_batch();
    engine.begin(&mut batch_setup, setup_tx);
    engine.apply_operation(
        &mut batch_setup,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("initial_value".to_string()),
        },
        setup_tx,
    );
    engine.commit(&mut batch_setup, setup_tx);
    engine.commit_batch(batch_setup, 3);

    // Multiple snapshot reads at different timestamps - all should succeed
    // without blocking each other
    let result1 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts1,
    );
    let result2 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts2,
    );
    let result3 = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts3,
    );

    // All should complete successfully
    assert!(matches!(result1, OperationResult::Complete(_)));
    assert!(matches!(result2, OperationResult::Complete(_)));
    assert!(matches!(result3, OperationResult::Complete(_)));
}

#[test]
fn test_snapshot_read_sees_committed_writes() {
    let mut engine = KvTransactionEngine::new();

    // Write and commit first value
    let write_tx1 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx1);
    engine.commit_batch(batch, 1);

    let mut batch = engine.start_batch();
    engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("value1".to_string()),
        },
        write_tx1,
    );
    engine.commit_batch(batch, 2);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, write_tx1);
    engine.commit_batch(batch, 3);

    // Create a snapshot transaction ID AFTER first write but BEFORE second write
    let read_ts = create_tx_id();

    // Write and commit second value AFTER snapshot
    let write_tx2 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx2);
    engine.commit_batch(batch, 4);

    let mut batch = engine.start_batch();
    engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("value2".to_string()),
        },
        write_tx2,
    );
    engine.commit_batch(batch, 5);

    let mut batch = engine.start_batch();
    engine.commit(&mut batch, write_tx2);
    engine.commit_batch(batch, 6);

    // Snapshot read at read_ts should see value1 (not value2) because read_ts < write_tx2
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::Str("value1".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }

    // Snapshot read AFTER second write should see value2
    let read_ts2 = create_tx_id();
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts2,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, Some(Value::Str("value2".to_string())));
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}

#[test]
fn test_snapshot_read_ignores_aborted_writes() {
    let mut engine = KvTransactionEngine::new();

    // Write at timestamp 1 but abort
    let write_tx1 = create_tx_id();
    let mut batch = engine.start_batch();
    engine.begin(&mut batch, write_tx1);
    engine.apply_operation(
        &mut batch,
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::Str("aborted_value".to_string()),
        },
        write_tx1,
    );
    engine.abort(&mut batch, write_tx1);
    engine.commit_batch(batch, 3);

    // Snapshot read at timestamp 2 should NOT see the aborted write
    let read_ts = create_tx_id();
    let result = engine.read_at_timestamp(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        read_ts,
    );

    match result {
        OperationResult::Complete(KvResponse::GetResult { value, .. }) => {
            assert_eq!(value, None);
        }
        _ => panic!("Expected Complete, got {:?}", result),
    }
}
