//! Integration tests for the KV engine

use proven_hlc::{HlcTimestamp, NodeId};
use proven_kv::stream::{KvOperation, KvResponse, KvTransactionEngine};
use proven_kv::types::Value;
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

    engine.begin_transaction(tx);

    // Test PUT
    let put_op = KvOperation::Put {
        key: "test_key".to_string(),
        value: Value::String("test_value".to_string()),
    };
    let result = engine.apply_operation(put_op, tx);
    match result {
        OperationResult::Success(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, None);
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }

    // Test GET - should retrieve the value
    let get_op = KvOperation::Get {
        key: "test_key".to_string(),
    };
    let result = engine.apply_operation(get_op.clone(), tx);
    match result {
        OperationResult::Success(KvResponse::GetResult { key, value }) => {
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
    let result = engine.apply_operation(put_op, tx);
    match result {
        OperationResult::Success(KvResponse::PutResult { key, previous }) => {
            assert_eq!(key, "test_key");
            assert_eq!(previous, Some(Value::String("test_value".to_string())));
        }
        _ => panic!("Expected successful put, got {:?}", result),
    }

    // Test DELETE
    let delete_op = KvOperation::Delete {
        key: "test_key".to_string(),
    };
    let result = engine.apply_operation(delete_op, tx);
    match result {
        OperationResult::Success(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "test_key");
            assert!(deleted);
        }
        _ => panic!("Expected successful delete, got {:?}", result),
    }

    // Test GET after DELETE - should return None
    let result = engine.apply_operation(get_op, tx);
    match result {
        OperationResult::Success(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "test_key");
            assert_eq!(value, None);
        }
        _ => panic!("Expected successful get, got {:?}", result),
    }

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_transaction_isolation() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);

    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Put a value
    let put_op = KvOperation::Put {
        key: "isolated_key".to_string(),
        value: Value::Integer(42),
    };
    let result = engine.apply_operation(put_op, tx1);
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to read the same key (should be blocked due to write lock)
    let get_op = KvOperation::Get {
        key: "isolated_key".to_string(),
    };
    let result = engine.apply_operation(get_op.clone(), tx2);
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    // Commit TX1
    assert!(engine.commit(tx1).is_ok());

    // TX2: Now should be able to read
    let result = engine.apply_operation(get_op, tx2);
    match result {
        OperationResult::Success(KvResponse::GetResult { key, value }) => {
            assert_eq!(key, "isolated_key");
            assert_eq!(value, Some(Value::Integer(42)));
        }
        _ => panic!("Expected successful get after commit"),
    }

    assert!(engine.commit(tx2).is_ok());
}

#[test]
fn test_transaction_abort_rollback() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);

    engine.begin_transaction(tx1);

    // Put some values
    for i in 0..3 {
        let put_op = KvOperation::Put {
            key: format!("key_{}", i),
            value: Value::Integer(i as i64),
        };
        engine.apply_operation(put_op, tx1);
    }

    // Abort the transaction
    assert!(engine.abort(tx1).is_ok());

    // Start new transaction - should not see aborted values
    let tx2 = timestamp(200);
    engine.begin_transaction(tx2);

    for i in 0..3 {
        let get_op = KvOperation::Get {
            key: format!("key_{}", i),
        };
        let result = engine.apply_operation(get_op, tx2);
        match result {
            OperationResult::Success(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, None);
            }
            _ => panic!("Expected successful get"),
        }
    }

    assert!(engine.commit(tx2).is_ok());
}

#[test]
fn test_different_value_types() {
    let mut engine = KvTransactionEngine::new();
    let tx = timestamp(100);

    engine.begin_transaction(tx);

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
        engine.apply_operation(put_op, tx);
    }

    // Get and verify all values
    for (key, expected_value) in &test_cases {
        let get_op = KvOperation::Get {
            key: key.to_string(),
        };
        let result = engine.apply_operation(get_op, tx);
        match result {
            OperationResult::Success(KvResponse::GetResult { value, .. }) => {
                assert_eq!(value, Some(expected_value.clone()));
            }
            _ => panic!("Expected successful get for key {}", key),
        }
    }

    assert!(engine.commit(tx).is_ok());
}

#[test]
fn test_concurrent_reads_with_shared_locks() {
    let mut engine = KvTransactionEngine::new();

    // First, create some data
    let tx_setup = timestamp(50);
    engine.begin_transaction(tx_setup);
    let put_op = KvOperation::Put {
        key: "shared_key".to_string(),
        value: Value::String("shared_data".to_string()),
    };
    engine.apply_operation(put_op, tx_setup);
    assert!(engine.commit(tx_setup).is_ok());

    // Now test concurrent reads
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);
    let tx3 = timestamp(300);

    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);
    engine.begin_transaction(tx3);

    // All transactions should be able to read concurrently (shared locks)
    let get_op = KvOperation::Get {
        key: "shared_key".to_string(),
    };

    let result1 = engine.apply_operation(get_op.clone(), tx1);
    assert!(matches!(result1, OperationResult::Success(_)));

    let result2 = engine.apply_operation(get_op.clone(), tx2);
    assert!(matches!(result2, OperationResult::Success(_)));

    let result3 = engine.apply_operation(get_op, tx3);
    assert!(matches!(result3, OperationResult::Success(_)));

    assert!(engine.commit(tx1).is_ok());
    assert!(engine.commit(tx2).is_ok());
    assert!(engine.commit(tx3).is_ok());
}

#[test]
fn test_write_write_conflict() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(100);
    let tx2 = timestamp(200);

    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Write to key
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::String("value1".to_string()),
    };
    let result = engine.apply_operation(put_op, tx1);
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to write to same key (should be blocked)
    let put_op = KvOperation::Put {
        key: "conflict_key".to_string(),
        value: Value::String("value2".to_string()),
    };
    let result = engine.apply_operation(put_op.clone(), tx2);
    match result {
        OperationResult::WouldBlock {
            blocking_txn,
            retry_on,
        } => {
            assert_eq!(blocking_txn, tx1);
            assert_eq!(retry_on, RetryOn::CommitOrAbort);
        }
        _ => panic!("Expected WouldBlock for write-write conflict"),
    }

    // Commit TX1
    assert!(engine.commit(tx1).is_ok());

    // TX2: Retry write (should now succeed)
    let result = engine.apply_operation(put_op, tx2);
    assert!(matches!(result, OperationResult::Success(_)));

    assert!(engine.commit(tx2).is_ok());
}

#[test]
fn test_delete_non_existent_key() {
    let mut engine = KvTransactionEngine::new();
    let tx = timestamp(100);

    engine.begin_transaction(tx);

    // Delete non-existent key
    let delete_op = KvOperation::Delete {
        key: "non_existent".to_string(),
    };
    let result = engine.apply_operation(delete_op, tx);
    match result {
        OperationResult::Success(KvResponse::DeleteResult { key, deleted }) => {
            assert_eq!(key, "non_existent");
            assert!(!deleted); // Should indicate nothing was deleted
        }
        _ => panic!("Expected successful delete operation"),
    }

    assert!(engine.commit(tx).is_ok());
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
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Read key1 (acquires shared lock)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to write to key1 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value2".to_string()),
        },
        tx2,
    );

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

    // TX2: Retry write (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value2".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));
}

#[test]
fn test_write_lock_not_released_on_prepare() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Write to key1 (acquires exclusive lock)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("value1".to_string()),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Try to read key1 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

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

    // TX2: Retry read (should still be blocked)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

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

    // TX2: Retry read (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx2,
    );

    match result {
        OperationResult::Success(KvResponse::GetResult { key, value }) => {
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
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Read multiple keys
    for i in 1..=3 {
        let result = engine.apply_operation(
            KvOperation::Get {
                key: format!("key{}", i),
            },
            tx1,
        );
        assert!(matches!(result, OperationResult::Success(_)));
    }

    // TX2: Try to write to key2 (should be blocked)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("new_value".to_string()),
        },
        tx2,
    );

    match result {
        OperationResult::WouldBlock {
            blocking_txn,
            retry_on,
        } => {
            assert_eq!(blocking_txn, tx1);
            assert_eq!(retry_on, RetryOn::Prepare);
        }
        _ => panic!("Expected WouldBlock, got {:?}", result),
    }

    // TX1: Prepare (should release all read locks)
    engine.prepare(tx1).expect("Prepare should succeed");

    // TX2: Retry write to key2 (should now succeed)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("new_value".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Also write to key1 and key3 (should succeed)
    for key in ["key1", "key3"] {
        let result = engine.apply_operation(
            KvOperation::Put {
                key: key.to_string(),
                value: Value::String("updated".to_string()),
            },
            tx2,
        );
        assert!(matches!(result, OperationResult::Success(_)));
    }
}

#[test]
fn test_mixed_locks_partial_release() {
    let mut engine = KvTransactionEngine::new();
    let tx1 = timestamp(1);
    let tx2 = timestamp(2);

    // Begin both transactions
    engine.begin_transaction(tx1);
    engine.begin_transaction(tx2);

    // TX1: Read key1 (shared lock)
    engine.apply_operation(
        KvOperation::Get {
            key: "key1".to_string(),
        },
        tx1,
    );

    // TX1: Write key2 (exclusive lock)
    engine.apply_operation(
        KvOperation::Put {
            key: "key2".to_string(),
            value: Value::String("value".to_string()),
        },
        tx1,
    );

    // TX1: Read key3 (shared lock)
    engine.apply_operation(
        KvOperation::Get {
            key: "key3".to_string(),
        },
        tx1,
    );

    // TX2: Try to write key1 (blocked by read lock)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("new".to_string()),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock {
            retry_on: RetryOn::Prepare,
            ..
        }
    ));

    // TX2: Try to read key2 (blocked by write lock)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock {
            retry_on: RetryOn::CommitOrAbort,
            ..
        }
    ));

    // TX1: Prepare (releases read locks on key1 and key3, keeps write lock on key2)
    engine.prepare(tx1).expect("Prepare should succeed");

    // TX2: Retry write to key1 (should succeed - read lock released)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key1".to_string(),
            value: Value::String("new".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Write to key3 (should succeed - read lock released)
    let result = engine.apply_operation(
        KvOperation::Put {
            key: "key3".to_string(),
            value: Value::String("new3".to_string()),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // TX2: Retry read of key2 (should still be blocked - write lock not released)
    let result = engine.apply_operation(
        KvOperation::Get {
            key: "key2".to_string(),
        },
        tx2,
    );
    assert!(matches!(
        result,
        OperationResult::WouldBlock {
            retry_on: RetryOn::CommitOrAbort,
            ..
        }
    ));
}
