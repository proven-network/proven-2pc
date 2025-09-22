//! Comprehensive integration tests for predicate-based locking
//!
//! These tests verify that the predicate-based conflict detection
//! correctly returns WouldBlock when transactions conflict and allows
//! non-conflicting transactions to proceed.

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::{SqlOperation, SqlTransactionEngine};
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
    assert!(matches!(result, OperationResult::Complete(_)));
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

    // Update source account
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance - 100 WHERE id = 1".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Update destination account
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE accounts SET balance = balance + 100 WHERE id = 2".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
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
    assert!(matches!(result, OperationResult::Complete(_)));

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
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2))
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_non_overlapping_ranges_no_conflict() {
    let mut engine = create_engine();

    // Create table with data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE events (id INT PRIMARY KEY, timestamp INT, type TEXT)".to_string(),
        },
        tx1,
    );
    // Insert events across a wide time range
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO events VALUES
                  (1, 100, 'login'), (2, 200, 'click'), (3, 300, 'logout'),
                  (4, 1100, 'login'), (5, 1200, 'click'), (6, 1300, 'logout')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query old events (timestamp < 500)
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM events WHERE timestamp < 500".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Query recent events (timestamp > 1000) - should NOT block
    // These ranges don't overlap: (−∞, 500) vs (1000, +∞)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM events WHERE timestamp > 1000".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping ranges should not conflict"
    );

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_compound_predicates_different_columns() {
    let mut engine = create_engine();

    // Create table with composite data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    city TEXT,
                    status TEXT,
                    amount INT
                  )"
            .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES
                  (1, 'NYC', 'pending', 100),
                  (2, 'LA', 'pending', 200),
                  (3, 'NYC', 'shipped', 150),
                  (4, 'LA', 'shipped', 250)"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Update NYC pending orders
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE orders SET amount = amount * 1.1
                  WHERE city = 'NYC' AND status = 'pending'"
                .to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update LA shipped orders - should NOT block
    // Different combination: (NYC, pending) vs (LA, shipped)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE orders SET amount = amount * 0.9
                  WHERE city = 'LA' AND status = 'shipped'"
                .to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Different compound predicates should not conflict"
    );

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_between_ranges_no_overlap() {
    let mut engine = create_engine();

    // Create table with numeric data
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE measurements (id INT PRIMARY KEY, value INT, sensor TEXT)"
                .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO measurements VALUES
                  (1, 10, 'A'), (2, 25, 'B'), (3, 40, 'A'),
                  (4, 60, 'B'), (5, 75, 'A'), (6, 90, 'B')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Process low values (10-30)
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE measurements SET sensor = 'LOW'
                  WHERE value >= 10 AND value <= 30"
                .to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Process high values (70-100) - should NOT block
    // Ranges [10, 30] and [70, 100] don't overlap
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE measurements SET sensor = 'HIGH'
                  WHERE value >= 70 AND value <= 100"
                .to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping BETWEEN ranges should not conflict"
    );

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_in_list_predicates() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE tasks (id INT PRIMARY KEY, status TEXT, priority INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO tasks VALUES
                  (1, 'open', 1), (2, 'open', 2), (3, 'open', 3),
                  (4, 'done', 1), (5, 'done', 2), (6, 'done', 3)"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Update specific tasks by ID
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE tasks SET priority = 5 WHERE id IN (1, 2, 3)".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update different specific tasks - should NOT block
    // IN lists (1,2,3) and (4,5,6) don't overlap
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE tasks SET priority = 0 WHERE id IN (4, 5, 6)".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping IN lists should not conflict"
    );

    // Both can commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_overlapping_ranges_do_conflict() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE data (id INT PRIMARY KEY, value INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO data VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query values 20-40
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM data WHERE value >= 20 AND value <= 40".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update overlapping range 30-50 - SHOULD block
    // Ranges [20, 40] and [30, 50] overlap at [30, 40]
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE data SET value = value + 1 WHERE value >= 30 AND value <= 50".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Overlapping ranges should conflict"
    );

    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
}

#[test]
fn test_same_column_different_values_no_conflict() {
    let mut engine = create_engine();

    // Create indexed table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE users (id INT PRIMARY KEY, status TEXT, country TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE INDEX idx_status ON users(status)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES
                  (1, 'active', 'US'), (2, 'inactive', 'UK'),
                  (3, 'active', 'CA'), (4, 'pending', 'US')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query active users
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM users WHERE status = 'active'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update inactive users - should NOT block
    // Different equality predicates on indexed column
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE users SET country = 'GB' WHERE status = 'inactive'".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Different equality values on same column should not conflict"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_complex_and_predicates_partial_overlap() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE products (
                    id INT PRIMARY KEY,
                    category TEXT,
                    price INT,
                    in_stock BOOLEAN
                  )"
            .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO products VALUES
                  (1, 'electronics', 100, true),
                  (2, 'electronics', 200, false),
                  (3, 'books', 20, true),
                  (4, 'books', 30, true),
                  (5, 'electronics', 300, true)"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query expensive electronics
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM products
                  WHERE category = 'electronics' AND price > 150"
                .to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update cheap books - should NOT block
    // (electronics AND price > 150) doesn't overlap with (books AND price < 50)
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE products SET in_stock = false
                  WHERE category = 'books' AND price < 50"
                .to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping compound predicates should not conflict"
    );

    // Transaction 4: Update expensive electronics - SHOULD block
    // This overlaps with tx2's predicate
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE products SET price = price * 0.9
                  WHERE category = 'electronics' AND price > 200"
                .to_string(),
        },
        tx4,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Overlapping compound predicates should conflict"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
    engine.abort(tx4).unwrap();
}

#[test]
fn test_not_equal_predicate() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE items (id INT PRIMARY KEY, type TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO items VALUES
                  (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C'), (5, 'B')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query all non-A items
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM items WHERE type != 'A'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update type B items - SHOULD block
    // Since NOT EQUAL is not extracted as a specific predicate,
    // tx2 falls back to FullTable predicate, which conflicts with everything
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE items SET type = 'D' WHERE type = 'B'".to_string(),
        },
        tx3,
    );
    // FullTable predicate (from tx2) should block tx3's equality predicate
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "NOT EQUAL should use conservative FullTable predicate and block"
    );

    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
}

#[test]
fn test_like_predicates_no_conflict() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES
                  (1, 'alice@gmail.com', 'Alice'),
                  (2, 'bob@yahoo.com', 'Bob'),
                  (3, 'charlie@gmail.com', 'Charlie'),
                  (4, 'dave@outlook.com', 'Dave')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query gmail users
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM users WHERE email LIKE '%@gmail.com'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update yahoo users - should NOT block
    // Different LIKE patterns that don't overlap
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE users SET name = UPPER(name) WHERE email LIKE '%@yahoo.com'".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping LIKE patterns should not conflict"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_is_null_predicates() {
    let mut engine = create_engine();

    // Create table with nullable column
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    customer_id INT,
                    notes TEXT
                  )"
            .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES
                  (1, 100, 'Rush order'),
                  (2, 101, NULL),
                  (3, NULL, 'Guest checkout'),
                  (4, 102, NULL)"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query orders with notes
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM orders WHERE notes IS NOT NULL".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update orders without notes - should NOT block
    // IS NULL vs IS NOT NULL are disjoint predicates
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE orders SET notes = 'Pending review' WHERE notes IS NULL".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "IS NULL and IS NOT NULL should not conflict"
    );

    // Transaction 4: Query orders without customers
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM orders WHERE customer_id IS NULL".to_string(),
        },
        tx4,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // All should be able to commit
    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
    engine.commit(tx4).unwrap();
}

#[test]
fn test_like_prefix_patterns() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE files (id INT PRIMARY KEY, path TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO files VALUES
                  (1, '/home/user/doc1.txt'),
                  (2, '/var/log/app.log'),
                  (3, '/home/user/doc2.txt'),
                  (4, '/etc/config.conf')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query /home files
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM files WHERE path LIKE '/home/%'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update /var files - should NOT block
    // Prefix patterns '/home/%' and '/var/%' don't overlap
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE files SET path = '/var/log/archived/' || path WHERE path LIKE '/var/%'"
                .to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Non-overlapping LIKE prefixes should not conflict"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_like_vs_equals_no_conflict() {
    let mut engine = create_engine();

    // Create table
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO products VALUES
                  (1, 'apple'), (2, 'banana'), (3, 'apricot'), (4, 'orange')"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query products starting with 'ap'
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM products WHERE name LIKE 'ap%'".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Update 'banana' - should NOT block
    // 'banana' doesnt match pattern 'ap%'
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE products SET name = 'BANANA' WHERE name = 'banana'".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::Complete(_)),
        "Equality predicate 'banana' should not conflict with LIKE 'ap%'"
    );

    // Transaction 4: Update 'apple' - SHOULD block
    // 'apple' matches pattern 'ap%'
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE products SET name = 'APPLE' WHERE name = 'apple'".to_string(),
        },
        tx4,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Equality predicate 'apple' should conflict with LIKE 'ap%'"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
    engine.abort(tx4).unwrap();
}

// ==================== SUBQUERY PREDICATE TESTS ====================

#[test]
fn test_subquery_read_predicates() {
    let mut engine = create_engine();

    // Create tables
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, dept_id INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1)"
                .to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Query with subquery - should lock both tables
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM users WHERE dept_id IN (SELECT id FROM departments WHERE name = 'Engineering')".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Try to modify departments - should block
    // because tx2's subquery is reading from departments
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE departments SET name = 'Dev' WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Subquery should lock departments table for reading"
    );

    // Transaction 4: Try to modify users - should also block
    // because tx2's outer query is reading from users
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE users SET name = 'ALICE' WHERE id = 1".to_string(),
        },
        tx4,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Outer query should lock users table for reading"
    );

    // Commit tx2 to release locks
    engine.commit(tx2).unwrap();

    // Now tx3 and tx4 should succeed
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE departments SET name = 'Dev' WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE users SET name = 'ALICE' WHERE id = 1".to_string(),
        },
        tx4,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    engine.commit(tx3).unwrap();
    engine.commit(tx4).unwrap();
}

#[test]
fn test_exists_subquery_predicates() {
    let mut engine = create_engine();

    // Create tables
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT)"
                .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, vip BOOLEAN)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO customers VALUES (1, 'Alice', true), (2, 'Bob', false)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: EXISTS subquery
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = 1)".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Try to insert into orders - should block
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO orders VALUES (3, 2, 300)".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "EXISTS subquery should lock orders table"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_scalar_subquery_predicates() {
    let mut engine = create_engine();

    // Create tables
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE products (id INT PRIMARY KEY, price INT, category_id INT)"
                .to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE categories (id INT PRIMARY KEY, name TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books')".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO products VALUES (1, 100, 1), (2, 200, 1), (3, 30, 2)".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Scalar subquery
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM products WHERE price > (SELECT MIN(price) FROM products WHERE category_id = 2)".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Try to update products in category 2 - should block
    // because the scalar subquery is reading products with category_id = 2
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE products SET price = 25 WHERE category_id = 2".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Scalar subquery should lock products table"
    );

    engine.commit(tx2).unwrap();
    engine.commit(tx3).unwrap();
}

#[test]
fn test_nested_subquery_predicates() {
    let mut engine = create_engine();

    // Create tables
    let tx1 = timestamp(1);
    engine.begin_transaction(tx1);
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE t1 (id INT PRIMARY KEY, val INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE t2 (id INT PRIMARY KEY, ref_id INT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "CREATE TABLE t3 (id INT PRIMARY KEY, data TEXT)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO t1 VALUES (1, 10), (2, 20)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO t2 VALUES (1, 1), (2, 2)".to_string(),
        },
        tx1,
    );
    engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "INSERT INTO t3 VALUES (1, 'data1'), (2, 'data2')".to_string(),
        },
        tx1,
    );
    engine.commit(tx1).unwrap();

    // Transaction 2: Nested subqueries
    let tx2 = timestamp(2);
    engine.begin_transaction(tx2);
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: "SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2 WHERE id IN (SELECT id FROM t3))".to_string(),
        },
        tx2,
    );
    assert!(matches!(result, OperationResult::Complete(_)));

    // Transaction 3: Try to modify t3 - should block
    // because the innermost subquery reads from t3
    let tx3 = timestamp(3);
    engine.begin_transaction(tx3);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "DELETE FROM t3 WHERE id = 1".to_string(),
        },
        tx3,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Nested subquery should lock t3 table"
    );

    // Transaction 4: Try to modify t2 - should also block
    let tx4 = timestamp(4);
    engine.begin_transaction(tx4);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE t2 SET ref_id = 3 WHERE id = 1".to_string(),
        },
        tx4,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Nested subquery should lock t2 table"
    );

    // Transaction 5: Try to modify t1 - should also block
    let tx5 = timestamp(5);
    engine.begin_transaction(tx5);
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: "UPDATE t1 SET val = 30 WHERE id = 1".to_string(),
        },
        tx5,
    );
    assert!(
        matches!(result, OperationResult::WouldBlock { blockers } if blockers.iter().any(|b| b.txn == tx2)),
        "Outer query should lock t1 table"
    );

    engine.commit(tx2).unwrap();
    engine.abort(tx3).unwrap();
    engine.abort(tx4).unwrap();
    engine.abort(tx5).unwrap();
}
