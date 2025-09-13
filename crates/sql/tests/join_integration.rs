//! Integration tests for JOIN operations
//!
//! Tests various JOIN types through the SQL engine interface

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::stream::{
    engine::SqlTransactionEngine, operation::SqlOperation, response::SqlResponse,
};
use proven_stream::{OperationResult, TransactionEngine};

/// Helper to create a test engine
fn create_engine() -> SqlTransactionEngine {
    SqlTransactionEngine::new()
}

/// Helper to create a timestamp
fn timestamp(n: u64) -> HlcTimestamp {
    HlcTimestamp::new(n, 0, NodeId::new(0))
}

/// Helper to execute SQL and check success
fn execute_sql(engine: &mut SqlTransactionEngine, sql: &str, tx: HlcTimestamp) {
    let result = engine.apply_operation(
        SqlOperation::Execute {
            params: None,
            sql: sql.to_string(),
        },
        tx,
    );

    match result {
        OperationResult::Complete(_) => {}
        _ => panic!("SQL execution failed: {}", sql),
    }
}

/// Helper to query and get results
fn query_sql(engine: &mut SqlTransactionEngine, sql: &str, tx: HlcTimestamp) -> Vec<Vec<String>> {
    let result = engine.apply_operation(
        SqlOperation::Query {
            params: None,
            sql: sql.to_string(),
        },
        tx,
    );

    match result {
        OperationResult::Complete(SqlResponse::QueryResult { rows, .. }) => rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| format!("{:?}", v)).collect())
            .collect(),
        OperationResult::Complete(SqlResponse::Error(err)) => {
            panic!("Query failed: {} - Error: {}", sql, err)
        }
        other => panic!("Query failed: {} - Result: {:?}", sql, other),
    }
}

#[test]
fn test_inner_join() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create tables
    execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status TEXT)",
        tx,
    );

    // Insert test data
    execute_sql(
        &mut engine,
        "INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)",
        tx,
    );
    execute_sql(
        &mut engine,
        "INSERT INTO orders VALUES (1, 1, 100, 'pending'), (2, 1, 200, 'shipped'), (3, 2, 150, 'delivered')",
        tx,
    );

    // Test INNER JOIN
    let results = query_sql(
        &mut engine,
        "SELECT u.name, o.amount, o.status FROM users u INNER JOIN orders o ON u.id = o.user_id ORDER BY o.amount",
        tx,
    );

    assert_eq!(results.len(), 3);
    assert_eq!(results[0][0], "String(\"Alice\")");
    assert_eq!(results[0][1], "Integer(100)");
    assert_eq!(results[1][0], "String(\"Bob\")");
    assert_eq!(results[1][1], "Integer(150)");
    assert_eq!(results[2][0], "String(\"Alice\")");
    assert_eq!(results[2][1], "Integer(200)");

    engine.commit(tx).unwrap();
}

#[test]
fn test_left_join() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create tables
    execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)",
        tx,
    );

    // Insert test data - note user 3 has no orders
    execute_sql(
        &mut engine,
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        tx,
    );
    execute_sql(
        &mut engine,
        "INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)",
        tx,
    );

    // Test LEFT JOIN
    let results = query_sql(
        &mut engine,
        "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.id",
        tx,
    );

    assert_eq!(results.len(), 3);
    assert_eq!(results[0][0], "String(\"Alice\")");
    assert_eq!(results[0][1], "Integer(100)");
    assert_eq!(results[1][0], "String(\"Bob\")");
    assert_eq!(results[1][1], "Integer(200)");
    assert_eq!(results[2][0], "String(\"Charlie\")");
    assert_eq!(results[2][1], "Null"); // No matching order

    engine.commit(tx).unwrap();
}

#[test]
fn test_cross_join() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create tables
    execute_sql(
        &mut engine,
        "CREATE TABLE colors (id INT PRIMARY KEY, name TEXT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE sizes (id INT PRIMARY KEY, name TEXT)",
        tx,
    );

    // Insert test data
    execute_sql(
        &mut engine,
        "INSERT INTO colors VALUES (1, 'Red'), (2, 'Blue')",
        tx,
    );
    execute_sql(
        &mut engine,
        "INSERT INTO sizes VALUES (1, 'Small'), (2, 'Large')",
        tx,
    );

    // Test CROSS JOIN
    let results = query_sql(
        &mut engine,
        "SELECT c.name, s.name FROM colors c CROSS JOIN sizes s ORDER BY c.id, s.id",
        tx,
    );

    assert_eq!(results.len(), 4); // 2 colors × 2 sizes
    assert_eq!(results[0][0], "String(\"Red\")");
    assert_eq!(results[0][1], "String(\"Small\")");
    assert_eq!(results[1][0], "String(\"Red\")");
    assert_eq!(results[1][1], "String(\"Large\")");
    assert_eq!(results[2][0], "String(\"Blue\")");
    assert_eq!(results[2][1], "String(\"Small\")");
    assert_eq!(results[3][0], "String(\"Blue\")");
    assert_eq!(results[3][1], "String(\"Large\")");

    engine.commit(tx).unwrap();
}

#[test]
fn test_join_with_aggregation() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create tables
    execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, department TEXT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE sales (id INT PRIMARY KEY, user_id INT, amount INT)",
        tx,
    );

    // Insert test data
    execute_sql(
        &mut engine,
        "INSERT INTO users VALUES (1, 'Alice', 'Sales'), (2, 'Bob', 'Sales'), (3, 'Charlie', 'Marketing')",
        tx,
    );
    execute_sql(
        &mut engine,
        "INSERT INTO sales VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 250)",
        tx,
    );

    // Test JOIN with GROUP BY and aggregation
    // Note: ORDER BY with aggregates might not be fully supported yet
    let results = query_sql(
        &mut engine,
        "SELECT u.name, SUM(s.amount) FROM users u INNER JOIN sales s ON u.id = s.user_id GROUP BY u.id, u.name ORDER BY u.name",
        tx,
    );

    assert_eq!(results.len(), 2);

    // Results should be ordered by name (Alice, Bob)
    assert_eq!(results[0][0], "String(\"Alice\")");
    assert_eq!(results[0][1], "Integer(300)"); // 100 + 200
    assert_eq!(results[1][0], "String(\"Bob\")");
    assert_eq!(results[1][1], "Integer(400)"); // 150 + 250

    engine.commit(tx).unwrap();
}

#[test]
fn test_self_join() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create employee table with manager relationships
    execute_sql(
        &mut engine,
        "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, manager_id INT)",
        tx,
    );

    // Insert test data
    execute_sql(
        &mut engine,
        "INSERT INTO employees VALUES (1, 'CEO', NULL), (2, 'Manager1', 1), (3, 'Manager2', 1), (4, 'Employee1', 2), (5, 'Employee2', 2)",
        tx,
    );

    // Test SELF JOIN to find employees and their managers
    let results = query_sql(
        &mut engine,
        "SELECT e.name as employee, m.name as manager FROM employees e INNER JOIN employees m ON e.manager_id = m.id ORDER BY e.id",
        tx,
    );

    assert_eq!(results.len(), 4); // CEO has no manager, so not in results
    assert_eq!(results[0][0], "String(\"Manager1\")");
    assert_eq!(results[0][1], "String(\"CEO\")");
    assert_eq!(results[1][0], "String(\"Manager2\")");
    assert_eq!(results[1][1], "String(\"CEO\")");
    assert_eq!(results[2][0], "String(\"Employee1\")");
    assert_eq!(results[2][1], "String(\"Manager1\")");
    assert_eq!(results[3][0], "String(\"Employee2\")");
    assert_eq!(results[3][1], "String(\"Manager1\")");

    engine.commit(tx).unwrap();
}
