//! Integration tests for DDL operations
//!
//! Tests CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX through the SQL engine

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
fn execute_sql(engine: &mut SqlTransactionEngine, sql: &str, tx: HlcTimestamp) -> SqlResponse {
    let result = engine.apply_operation(
        SqlOperation::Execute {
            sql: sql.to_string(),
            params: None,
        },
        tx,
    );

    match result {
        OperationResult::Success(response) => response,
        _ => panic!("SQL execution failed: {}", sql),
    }
}

/// Helper to execute SQL that should fail
fn execute_sql_expect_error(
    engine: &mut SqlTransactionEngine,
    sql: &str,
    tx: HlcTimestamp,
) -> String {
    let result = engine.apply_operation(
        SqlOperation::Execute {
            sql: sql.to_string(),
            params: None,
        },
        tx,
    );

    match result {
        OperationResult::Error(msg) => msg,
        _ => panic!("Expected error for SQL: {}", sql),
    }
}

#[test]
fn test_create_table_basic() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create a basic table
    let response = execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)",
        tx,
    );

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
            assert!(message.unwrap().contains("users"));
        }
        _ => panic!("Expected ExecuteResult for CREATE TABLE"),
    }

    // Insert data to verify table exists
    execute_sql(&mut engine, "INSERT INTO users VALUES (1, 'Alice', 25)", tx);

    engine.commit(tx).unwrap();
}

#[test]
fn test_create_table_if_not_exists() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create table first time
    execute_sql(
        &mut engine,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)",
        tx,
    );

    // Try to create again without IF NOT EXISTS - should fail
    let error = execute_sql_expect_error(
        &mut engine,
        "CREATE TABLE products (id INT PRIMARY KEY, name TEXT)",
        tx,
    );
    assert!(error.contains("already exists") || error.contains("products"));

    // Create with IF NOT EXISTS - should succeed
    let response = execute_sql(
        &mut engine,
        "CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY, name TEXT)",
        tx,
    );

    match response {
        SqlResponse::ExecuteResult { .. } => {}
        _ => panic!("Expected ExecuteResult for CREATE TABLE IF NOT EXISTS"),
    }

    engine.commit(tx).unwrap();
}

#[test]
fn test_drop_table() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create and populate table
    execute_sql(
        &mut engine,
        "CREATE TABLE temp_data (id INT PRIMARY KEY, value TEXT)",
        tx,
    );
    execute_sql(&mut engine, "INSERT INTO temp_data VALUES (1, 'test')", tx);

    // Drop the table
    let response = execute_sql(&mut engine, "DROP TABLE temp_data", tx);

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
            assert!(message.unwrap().contains("temp_data"));
        }
        _ => panic!("Expected ExecuteResult for DROP TABLE"),
    }

    // Try to insert - should fail
    let error =
        execute_sql_expect_error(&mut engine, "INSERT INTO temp_data VALUES (2, 'fail')", tx);
    assert!(error.contains("not found") || error.contains("temp_data"));

    engine.commit(tx).unwrap();
}

#[test]
fn test_drop_table_if_exists() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Try to drop non-existent table without IF EXISTS - should fail
    let error = execute_sql_expect_error(&mut engine, "DROP TABLE nonexistent", tx);
    assert!(error.contains("not found") || error.contains("nonexistent"));

    // Drop with IF EXISTS - should succeed
    let response = execute_sql(&mut engine, "DROP TABLE IF EXISTS nonexistent", tx);

    match response {
        SqlResponse::ExecuteResult { .. } => {}
        _ => panic!("Expected ExecuteResult for DROP TABLE IF EXISTS"),
    }

    engine.commit(tx).unwrap();
}

#[test]
fn test_create_index() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create table
    execute_sql(
        &mut engine,
        "CREATE TABLE products (id INT PRIMARY KEY, category TEXT, price INT)",
        tx,
    );

    // Create index
    let response = execute_sql(
        &mut engine,
        "CREATE INDEX idx_category ON products(category)",
        tx,
    );

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
            let msg = message.unwrap();
            assert!(msg.contains("idx_category") || msg.contains("index"));
        }
        _ => panic!("Expected ExecuteResult for CREATE INDEX"),
    }

    // Create composite index
    let response = execute_sql(
        &mut engine,
        "CREATE INDEX idx_cat_price ON products(category, price)",
        tx,
    );

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
        }
        _ => panic!("Expected ExecuteResult for CREATE INDEX"),
    }

    engine.commit(tx).unwrap();
}

#[test]
fn test_create_unique_index() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create table
    execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, username TEXT)",
        tx,
    );

    // Create unique index
    let response = execute_sql(
        &mut engine,
        "CREATE UNIQUE INDEX idx_email ON users(email)",
        tx,
    );

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
        }
        _ => panic!("Expected ExecuteResult for CREATE UNIQUE INDEX"),
    }

    // Insert data
    execute_sql(
        &mut engine,
        "INSERT INTO users VALUES (1, 'alice@example.com', 'alice')",
        tx,
    );

    // Try to insert duplicate email - should fail due to unique index
    let error = execute_sql_expect_error(
        &mut engine,
        "INSERT INTO users VALUES (2, 'alice@example.com', 'bob')",
        tx,
    );
    assert!(
        error.contains("unique") || error.contains("duplicate") || error.contains("constraint")
    );

    engine.commit(tx).unwrap();
}

#[test]
fn test_drop_index() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create table and index
    execute_sql(
        &mut engine,
        "CREATE TABLE data (id INT PRIMARY KEY, value INT)",
        tx,
    );
    execute_sql(&mut engine, "CREATE INDEX idx_value ON data(value)", tx);

    // Drop the index
    let response = execute_sql(&mut engine, "DROP INDEX idx_value", tx);

    match response {
        SqlResponse::ExecuteResult { message, .. } => {
            assert!(message.is_some());
            assert!(message.unwrap().contains("idx_value"));
        }
        _ => panic!("Expected ExecuteResult for DROP INDEX"),
    }

    engine.commit(tx).unwrap();
}

#[test]
fn test_drop_index_if_exists() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Try to drop non-existent index without IF EXISTS - should fail
    let error = execute_sql_expect_error(&mut engine, "DROP INDEX nonexistent_idx", tx);
    assert!(error.contains("not found") || error.contains("nonexistent_idx"));

    // Drop with IF EXISTS - should succeed
    let response = execute_sql(&mut engine, "DROP INDEX IF EXISTS nonexistent_idx", tx);

    match response {
        SqlResponse::ExecuteResult { .. } => {}
        _ => panic!("Expected ExecuteResult for DROP INDEX IF EXISTS"),
    }

    engine.commit(tx).unwrap();
}

#[test]
fn test_multiple_ddl_operations() {
    let mut engine = create_engine();
    let tx = timestamp(1);

    engine.begin_transaction(tx);

    // Create multiple tables
    execute_sql(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, content TEXT)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE TABLE comments (id INT PRIMARY KEY, post_id INT, text TEXT)",
        tx,
    );

    // Create indexes
    execute_sql(
        &mut engine,
        "CREATE INDEX idx_posts_user ON posts(user_id)",
        tx,
    );
    execute_sql(
        &mut engine,
        "CREATE INDEX idx_comments_post ON comments(post_id)",
        tx,
    );

    // Insert some data
    execute_sql(&mut engine, "INSERT INTO users VALUES (1, 'Alice')", tx);
    execute_sql(
        &mut engine,
        "INSERT INTO posts VALUES (1, 1, 'First post')",
        tx,
    );
    execute_sql(
        &mut engine,
        "INSERT INTO comments VALUES (1, 1, 'Nice post!')",
        tx,
    );

    // Drop an index
    execute_sql(&mut engine, "DROP INDEX idx_comments_post", tx);

    // Drop a table
    execute_sql(&mut engine, "DROP TABLE comments", tx);

    // Verify remaining tables still work
    execute_sql(&mut engine, "INSERT INTO users VALUES (2, 'Bob')", tx);
    execute_sql(
        &mut engine,
        "INSERT INTO posts VALUES (2, 2, 'Another post')",
        tx,
    );

    engine.commit(tx).unwrap();
}
