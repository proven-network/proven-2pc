//! Tests for SQL snapshot read functionality

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::{SqlOperation, SqlResponse, SqlTransactionEngine};
use proven_stream::{OperationResult, TransactionEngine};

fn make_timestamp(n: u64) -> HlcTimestamp {
    HlcTimestamp::new(n, 0, NodeId::new(0))
}

#[test]
fn test_snapshot_read_doesnt_block_later_write() {
    let mut engine = SqlTransactionEngine::new();

    // Create table and insert data
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, tx1);

    let insert = SqlOperation::Execute {
        sql: "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)".to_string(),
        params: None,
    };
    engine.apply_operation(insert, tx1);

    engine.commit(tx1);

    // Start a write transaction at timestamp 300
    let tx_write = make_timestamp(300);
    engine.begin(tx_write);

    let update = SqlOperation::Execute {
        sql: "UPDATE users SET age = 31 WHERE id = 1".to_string(),
        params: None,
    };
    let result = engine.apply_operation(update, tx_write);
    assert!(matches!(result, OperationResult::Complete(_)));

    // Snapshot read at timestamp 250 should NOT block
    // (read is BEFORE the write transaction)
    let read_ts = make_timestamp(250);
    let select = SqlOperation::Query {
        sql: "SELECT age FROM users WHERE id = 1".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select, read_ts);

    // Should complete successfully
    assert!(matches!(result, OperationResult::Complete(_)));

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        // Should see the old value (30)
        assert_eq!(rows[0][0].to_string(), "30");
    } else {
        panic!("Expected query result");
    }

    engine.commit(tx_write);
}

#[test]
fn test_snapshot_read_blocks_on_earlier_write() {
    let mut engine = SqlTransactionEngine::new();

    // Create table and insert data
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, tx1);

    let insert = SqlOperation::Execute {
        sql: "INSERT INTO users (id, name, age) VALUES (1, 'Bob', 25)".to_string(),
        params: None,
    };
    engine.apply_operation(insert, tx1);

    engine.commit(tx1);

    // Start a write transaction at timestamp 200 (but don't commit)
    let tx_write = make_timestamp(200);
    engine.begin(tx_write);

    let update = SqlOperation::Execute {
        sql: "UPDATE users SET age = 26 WHERE id = 1".to_string(),
        params: None,
    };
    engine.apply_operation(update, tx_write);

    // Snapshot read at timestamp 250 SHOULD block
    // (there's a pending write from timestamp 200)
    let read_ts = make_timestamp(250);
    let select = SqlOperation::Query {
        sql: "SELECT age FROM users WHERE id = 1".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select.clone(), read_ts);

    // Should block
    assert!(matches!(result, OperationResult::WouldBlock { .. }));

    if let OperationResult::WouldBlock { blockers } = result {
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0].txn, tx_write);
    }

    // Commit the write
    engine.commit(tx_write);

    // Now the same read should succeed
    let result = engine.read_at_timestamp(select, read_ts);
    assert!(matches!(result, OperationResult::Complete(_)));

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows.len(), 1);
        // Should see the new value after commit (26)
        assert_eq!(rows[0][0].to_string(), "26");
    }
}

#[test]
fn test_snapshot_read_ignores_uncommitted_changes() {
    let mut engine = SqlTransactionEngine::new();

    // Create table and insert data
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, tx1);

    let insert1 = SqlOperation::Execute {
        sql: "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)".to_string(),
        params: None,
    };
    engine.apply_operation(insert1, tx1);

    let insert2 = SqlOperation::Execute {
        sql: "INSERT INTO products (id, name, price) VALUES (2, 'Gadget', 200)".to_string(),
        params: None,
    };
    engine.apply_operation(insert2, tx1);

    engine.commit(tx1);

    // Start a write transaction that will be aborted
    let tx_abort = make_timestamp(200);
    engine.begin(tx_abort);

    let update = SqlOperation::Execute {
        sql: "UPDATE products SET price = 999 WHERE id = 1".to_string(),
        params: None,
    };
    engine.apply_operation(update, tx_abort);

    let insert3 = SqlOperation::Execute {
        sql: "INSERT INTO products (id, name, price) VALUES (3, 'Doohickey', 300)".to_string(),
        params: None,
    };
    engine.apply_operation(insert3, tx_abort);

    // Abort the transaction
    engine.abort(tx_abort);

    // Snapshot read at timestamp 250 should see original data only
    let read_ts = make_timestamp(250);
    let select = SqlOperation::Query {
        sql: "SELECT id, price FROM products ORDER BY id".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select, read_ts);

    // Should NOT block (transaction was aborted)
    assert!(matches!(result, OperationResult::Complete(_)));

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        // Should only see the two original products
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].to_string(), "1");
        assert_eq!(rows[0][1].to_string(), "100"); // Original price, not 999
        assert_eq!(rows[1][0].to_string(), "2");
        assert_eq!(rows[1][1].to_string(), "200");
    } else {
        panic!("Expected query result");
    }
}

#[test]
fn test_snapshot_read_consistency_across_tables() {
    let mut engine = SqlTransactionEngine::new();

    // Create tables and initial data
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)".to_string(),
            params: None,
        },
        tx1,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "CREATE TABLE transactions (id INTEGER PRIMARY KEY, from_id INTEGER, to_id INTEGER, amount INTEGER)".to_string(),
            params: None,
        },
        tx1,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 500)".to_string(),
            params: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Perform a transfer at timestamp 200
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "UPDATE accounts SET balance = 900 WHERE id = 1".to_string(),
            params: None,
        },
        tx2,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "UPDATE accounts SET balance = 600 WHERE id = 2".to_string(),
            params: None,
        },
        tx2,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "INSERT INTO transactions (id, from_id, to_id, amount) VALUES (1, 1, 2, 100)"
                .to_string(),
            params: None,
        },
        tx2,
    );

    engine.commit(tx2);

    // Snapshot read at timestamp 150 should see state before transfer
    let read_ts1 = make_timestamp(150);

    let select_accounts = SqlOperation::Query {
        sql: "SELECT id, balance FROM accounts ORDER BY id".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select_accounts.clone(), read_ts1);

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].to_string(), "1000"); // Original balance
        assert_eq!(rows[1][1].to_string(), "500");
    } else {
        panic!("Expected query result");
    }

    let select_txns = SqlOperation::Query {
        sql: "SELECT COUNT(*) FROM transactions".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select_txns.clone(), read_ts1);

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows[0][0].to_string(), "0"); // No transactions yet
    } else {
        panic!("Expected query result");
    }

    // Snapshot read at timestamp 250 should see state after transfer
    let read_ts2 = make_timestamp(250);

    let result = engine.read_at_timestamp(select_accounts, read_ts2);

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].to_string(), "900"); // After transfer
        assert_eq!(rows[1][1].to_string(), "600");
    } else {
        panic!("Expected query result");
    }

    let result = engine.read_at_timestamp(select_txns, read_ts2);

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        assert_eq!(rows[0][0].to_string(), "1"); // One transaction
    } else {
        panic!("Expected query result");
    }
}

#[test]
fn test_snapshot_read_with_index_scan() {
    let mut engine = SqlTransactionEngine::new();

    // Create indexed table
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)"
                .to_string(),
            params: None,
        },
        tx1,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "CREATE INDEX idx_category ON items(category)".to_string(),
            params: None,
        },
        tx1,
    );

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "INSERT INTO items VALUES (1, 'electronics', 500), (2, 'books', 20), (3, 'electronics', 300)".to_string(),
            params: None,
        },
        tx1,
    );

    engine.commit(tx1);

    // Update at timestamp 200
    let tx2 = make_timestamp(200);
    engine.begin(tx2);

    engine.apply_operation(
        SqlOperation::Execute {
            sql: "INSERT INTO items VALUES (4, 'electronics', 800)".to_string(),
            params: None,
        },
        tx2,
    );

    engine.commit(tx2);

    // Snapshot read at timestamp 150 using index
    let read_ts = make_timestamp(150);
    let select = SqlOperation::Query {
        sql: "SELECT id FROM items WHERE category = 'electronics' ORDER BY id".to_string(),
        params: None,
    };

    let result = engine.read_at_timestamp(select, read_ts);

    if let OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) = result {
        // Should only see items 1 and 3, not 4 (added later)
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].to_string(), "1");
        assert_eq!(rows[1][0].to_string(), "3");
    } else {
        panic!("Expected query result");
    }
}

#[test]
#[should_panic(expected = "Must be read-only operation")]
fn test_snapshot_read_only_operations_allowed() {
    let mut engine = SqlTransactionEngine::new();

    // Create table
    let tx1 = make_timestamp(100);
    engine.begin(tx1);

    let create_table = SqlOperation::Execute {
        sql: "CREATE TABLE test (id INTEGER PRIMARY KEY)".to_string(),
        params: None,
    };
    engine.apply_operation(create_table, tx1);
    engine.commit(tx1);

    // Try to perform a write operation via snapshot read
    let read_ts = make_timestamp(200);

    let insert = SqlOperation::Execute {
        sql: "INSERT INTO test VALUES (1)".to_string(),
        params: None,
    };

    engine.read_at_timestamp(insert, read_ts);
}
