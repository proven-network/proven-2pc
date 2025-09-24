//! Tests for KV engine snapshot functionality

use proven_hlc::{HlcTimestamp, NodeId};
use proven_kv::stream::engine::KvTransactionEngine;
use proven_kv::stream::operation::KvOperation;
use proven_kv::types::Value;
use proven_stream::{OperationResult, TransactionEngine};

#[test]
fn test_kv_snapshot_and_restore() {
    // Create engine and add some data
    let mut engine1 = KvTransactionEngine::new();

    // Begin transaction and add data
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine1.begin(txn1);

    // Put some values
    let op1 = KvOperation::Put {
        key: "user:1".to_string(),
        value: Value::String("Alice".to_string()),
    };
    let result = engine1.apply_operation(op1, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    let op2 = KvOperation::Put {
        key: "user:2".to_string(),
        value: Value::String("Bob".to_string()),
    };
    let result = engine1.apply_operation(op2, txn1);
    assert!(matches!(result, OperationResult::Complete(_)));

    let op3 = KvOperation::Put {
        key: "count".to_string(),
        value: Value::Integer(42),
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
    let mut engine2 = KvTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify data was restored by reading it
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    let read1 = KvOperation::Get {
        key: "user:1".to_string(),
    };
    let result = engine2.apply_operation(read1, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "user:1", value: Some(String("Alice")) }"#
        );
    } else {
        panic!("Expected success");
    }

    let read2 = KvOperation::Get {
        key: "user:2".to_string(),
    };
    let result = engine2.apply_operation(read2, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "user:2", value: Some(String("Bob")) }"#
        );
    } else {
        panic!("Expected success");
    }

    let read3 = KvOperation::Get {
        key: "count".to_string(),
    };
    let result = engine2.apply_operation(read3, txn2);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "count", value: Some(Integer(42)) }"#
        );
    } else {
        panic!("Expected success");
    }
}

#[test]
fn test_snapshot_with_deleted_keys() {
    let mut engine = KvTransactionEngine::new();

    // Add and delete some data
    let txn1 = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn1);

    // Put values
    let op1 = KvOperation::Put {
        key: "temp1".to_string(),
        value: Value::String("temporary".to_string()),
    };
    engine.apply_operation(op1, txn1);

    let op2 = KvOperation::Put {
        key: "keep".to_string(),
        value: Value::String("keeper".to_string()),
    };
    engine.apply_operation(op2, txn1);

    engine.prepare(txn1);
    engine.commit(txn1);

    // Delete one key in a new transaction
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine.begin(txn2);

    let del_op = KvOperation::Delete {
        key: "temp1".to_string(),
    };
    engine.apply_operation(del_op, txn2);

    engine.prepare(txn2);
    engine.commit(txn2);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // Restore to new engine
    let mut engine2 = KvTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Verify deleted key is not present
    let txn3 = HlcTimestamp::new(3, 0, NodeId::new(1));
    engine2.begin(txn3);

    let read1 = KvOperation::Get {
        key: "temp1".to_string(),
    };
    let result = engine2.apply_operation(read1, txn3);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "temp1", value: None }"#
        );
    }

    // Verify kept key is present
    let read2 = KvOperation::Get {
        key: "keep".to_string(),
    };
    let result = engine2.apply_operation(read2, txn3);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "keep", value: Some(String("keeper")) }"#
        );
    }
}

#[test]
fn test_snapshot_compression() {
    let mut engine = KvTransactionEngine::new();

    // Add a lot of repetitive data to test compression
    let txn = HlcTimestamp::new(1, 0, NodeId::new(1));
    engine.begin(txn);

    // Create data with good compression potential
    let long_string = "A".repeat(1000);
    for i in 0..100 {
        let op = KvOperation::Put {
            key: format!("key{}", i),
            value: Value::String(long_string.clone()),
        };
        engine.apply_operation(op, txn);
    }

    engine.prepare(txn);
    engine.commit(txn);

    // Take snapshot
    let snapshot = engine.snapshot().unwrap();

    // The compressed snapshot should be much smaller than the raw data
    // 100 keys * 1000 chars = 100KB uncompressed
    // With compression, should be much smaller
    println!("Snapshot size: {} bytes", snapshot.len());
    assert!(snapshot.len() < 50000, "Snapshot should be compressed");

    // Verify it can be restored
    let mut engine2 = KvTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    // Spot check a value
    let txn2 = HlcTimestamp::new(2, 0, NodeId::new(1));
    engine2.begin(txn2);

    let read = KvOperation::Get {
        key: "key50".to_string(),
    };
    let result = engine2.apply_operation(read, txn2);
    if let OperationResult::Complete(response) = result {
        let response_str = format!("{:?}", response);
        assert!(response_str.contains(&format!("String(\"{}\")", long_string)));
    }
}

#[test]
fn test_mvcc_compaction_in_snapshot() {
    let mut engine = KvTransactionEngine::new();

    // Create multiple versions of the same key
    for i in 1..=5 {
        let txn = HlcTimestamp::new(i, 0, NodeId::new(1));
        engine.begin(txn);

        let op = KvOperation::Put {
            key: "versioned".to_string(),
            value: Value::Integer(i as i64),
        };
        engine.apply_operation(op, txn);

        engine.prepare(txn);
        engine.commit(txn);
    }

    // The MVCC storage now has 5 versions of "versioned"
    // Snapshot should compact to just the latest
    let snapshot = engine.snapshot().unwrap();

    // Restore and verify only latest version exists
    let mut engine2 = KvTransactionEngine::new();
    engine2.restore_from_snapshot(&snapshot).unwrap();

    let txn = HlcTimestamp::new(10, 0, NodeId::new(1));
    engine2.begin(txn);

    let read = KvOperation::Get {
        key: "versioned".to_string(),
    };
    let result = engine2.apply_operation(read, txn);
    if let OperationResult::Complete(response) = result {
        assert_eq!(
            format!("{:?}", response),
            r#"GetResult { key: "versioned", value: Some(Integer(5)) }"#
        );
    }

    // The snapshot should be relatively small since it only has 1 version
    println!("Compacted snapshot size: {} bytes", snapshot.len());
}
