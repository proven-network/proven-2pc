//! Comprehensive integration tests for predicate-based locking
//!
//! These tests verify that the predicate-based conflict detection
//! correctly returns WouldBlock when transactions conflict and allows
//! non-conflicting transactions to proceed.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::{engine::SqlTransactionEngine, operation::SqlOperation};
use proven_stream::{OperationResult, TransactionEngine};

/// Helper to create a test engine
fn create_engine() -> SqlTransactionEngine {
    SqlTransactionEngine::new()
}

/// Helper to create a timestamp
fn timestamp(n: u64) -> HlcTimestamp {
    HlcTimestamp::new(n, 0, NodeId::new(0))
}

#[test]
fn test_read_write_conflict_same_table() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    assert!(matches!(result, OperationResult::Success(_)));
    engine.commit(tx1).unwrap();

    // Transaction 2: Start reading from users table
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM users".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to write to users table - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES (1, 'Alice')".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    // Commit tx2 to unblock tx3
    engine.commit(tx2).unwrap();

    // Now tx3 should succeed
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES (1, 'Alice')".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Success(_)));
    engine.commit(tx3).unwrap();
}

#[test]
fn test_write_write_conflict() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO accounts VALUES (1, 100)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Start updating account
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance + 50 WHERE id = 1".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to update same account - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance - 30 WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    // Commit tx2
    engine.commit(tx2).unwrap();

    // Now tx3 should succeed
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance - 30 WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Success(_)));
    engine.commit(tx3).unwrap();
}

#[test]
fn test_no_conflict_different_tables() {
    let mut engine = create_engine();

    // Create tables
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Write to users
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES (1, 'Alice')".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Write to products - should NOT block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO products VALUES (1, 'Widget')".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Both should commit successfully
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_no_conflict_different_rows_with_filter() {
    let mut engine = create_engine();

    // Create table with data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE items (id INT PRIMARY KEY, category TEXT, price INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO items VALUES (1, 'electronics', 100), (2, 'books', 20), (3, 'electronics', 200)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Update electronics items
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE items SET price = price * 2 WHERE category = 'electronics'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Update books items - should NOT block (different predicates)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE items SET price = price + 5 WHERE category = 'books'".to_string(),
        },
        tx3,
    );
    // These updates work on different rows (different category values)
    // so they don't conflict. This is correct behavior.
    assert!(matches!(result, OperationResult::Success(_)));

    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
}

#[test]
fn test_read_read_no_conflict() {
    let mut engine = create_engine();

    // Create table with data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE data (id INT PRIMARY KEY, value TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO data VALUES (1, 'test')".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Read data
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM data".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Also read data - should NOT block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM data WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_insert_insert_conflict_same_pk() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE unique_items (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Insert with id=1
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO unique_items VALUES (1, 'First')".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to insert with same id - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO unique_items VALUES (1, 'Second')".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
}

#[test]
fn test_delete_read_conflict() {
    let mut engine = create_engine();

    // Create table with data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE records (id INT PRIMARY KEY, active BOOLEAN)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO records VALUES (1, true), (2, false), (3, true)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Read all records (no filter for conservative conflict detection)
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM records".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to delete records - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "DELETE FROM records WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_prepare_releases_read_locks() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE test (id INT PRIMARY KEY, value INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO test VALUES (1, 100)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Read data
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM test".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to write - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    // Prepare tx2 - this should release read predicates
    engine.prepare(tx2).unwrap();

    // Now tx3 should succeed since read predicates were released
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE test SET value = 200 WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_complex_multi_statement_transaction() {
    let mut engine = create_engine();

    // Setup
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql:
                "CREATE TABLE transactions (id INT PRIMARY KEY, from_id INT, to_id INT, amount INT)"
                    .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO accounts VALUES (1, 1000), (2, 500)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Transfer money (multiple statements)
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);

    // Read source balance
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT balance FROM accounts WHERE id = 1".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Update source account
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance - 100 WHERE id = 1".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Update destination account
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance + 100 WHERE id = 2".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to read account 1 - should block (tx2 has write predicate)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM accounts WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    // Transaction 4: Read transactions table - should NOT block
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM transactions".to_string(),
        },
        tx4,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Clean up
    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
    engine.commit(tx4).unwrap();
}

#[test]
fn test_phantom_prevention() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT, amount INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES (1, 'pending', 100), (2, 'shipped', 200)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Read all orders (conservative approach for phantom prevention)
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM orders".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to insert a new order - should block
    // (This prevents phantoms - tx2's read predicate covers the entire table)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES (3, 'pending', 150)".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_aggregation_conflict() {
    let mut engine = create_engine();

    // Create table with data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE sales (id INT PRIMARY KEY, amount INT, region TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO sales VALUES (1, 100, 'west'), (2, 200, 'east'), (3, 150, 'west')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Calculate sum of sales
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT SUM(amount) FROM sales".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Success(_)));

    // Transaction 3: Try to insert new sale - should block
    // (Aggregation needs stable view of all data)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO sales VALUES (4, 300, 'north')".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blocking_txn, .. } if blocking_txn == tx2)
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}
