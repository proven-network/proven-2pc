//! Common test utilities for SQL integration tests
#![allow(dead_code)]

use proven_hlc::{HlcTimestamp, NodeId};
use proven_sql::{SqlOperation, SqlResponse, SqlStorageConfig, SqlTransactionEngine};
use proven_stream::{OperationResult, TransactionEngine};
use proven_value::Value;
use std::collections::HashMap;

/// Test context that manages engine, transactions, and provides helper methods
pub struct TestContext {
    pub engine: SqlTransactionEngine,
    current_timestamp: u64,
    current_log_index: u64,
    in_transaction: bool,
    current_tx: Option<HlcTimestamp>,
}

impl TestContext {
    /// Create a new test context
    pub fn new() -> Self {
        // Use current system time in microseconds for realistic timestamps
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        Self {
            engine: SqlTransactionEngine::new(SqlStorageConfig::default()),
            current_timestamp: now_micros,
            current_log_index: 0,
            in_transaction: false,
            current_tx: None,
        }
    }

    /// Get the next timestamp
    pub fn next_timestamp(&mut self) -> HlcTimestamp {
        let ts = HlcTimestamp::new(self.current_timestamp, 0, NodeId::new(0));
        self.current_timestamp += 1;
        ts
    }

    /// Get and increment log index
    fn next_log_index(&mut self) -> u64 {
        let index = self.current_log_index;
        self.current_log_index += 1;
        index
    }

    /// Begin a new transaction
    pub fn begin(&mut self) {
        let tx = self.next_timestamp();
        let log_index = self.next_log_index();
        self.engine.begin(tx, log_index);
        self.in_transaction = true;
        self.current_tx = Some(tx);
    }

    /// Commit the current transaction
    pub fn commit(&mut self) {
        if let Some(tx) = self.current_tx {
            let log_index = self.next_log_index();
            self.engine.commit(tx, log_index);
            self.in_transaction = false;
            self.current_tx = None;
        }
    }

    /// Abort the current transaction
    pub fn abort(&mut self) {
        if let Some(tx) = self.current_tx {
            let log_index = self.next_log_index();
            self.engine.abort(tx, log_index);
            self.in_transaction = false;
            self.current_tx = None;
        }
    }

    /// Execute SQL without expecting a result
    pub fn exec(&mut self, sql: &str) {
        let tx = self.current_tx.unwrap_or_else(|| self.next_timestamp());
        let log_index = self.next_log_index();

        let result = self.engine.apply_operation(
            SqlOperation::Execute {
                sql: sql.to_string(),
                params: None,
            },
            tx,
            log_index,
        );

        match result {
            OperationResult::Complete(SqlResponse::Error(err)) => {
                panic!("SQL execution failed: {} - Error: {}", sql, err)
            }
            OperationResult::Complete(_) => {}
            _ => panic!("SQL execution failed: {}", sql),
        }
    }

    /// Execute SQL and return the response
    pub fn exec_response(&mut self, sql: &str) -> SqlResponse {
        let tx = self.current_tx.unwrap_or_else(|| self.next_timestamp());
        let log_index = self.next_log_index();

        match self.engine.apply_operation(
            SqlOperation::Execute {
                sql: sql.to_string(),
                params: None,
            },
            tx,
            log_index,
        ) {
            OperationResult::Complete(response) => response,
            _ => panic!("SQL execution failed: {}", sql),
        }
    }

    /// Query SQL and return raw results
    pub fn query(&mut self, sql: &str) -> Vec<HashMap<String, Value>> {
        let tx = self.current_tx.unwrap_or_else(|| self.next_timestamp());
        let log_index = self.next_log_index();

        let result = self.engine.apply_operation(
            SqlOperation::Execute {
                sql: sql.to_string(),
                params: None,
            },
            tx,
            log_index,
        );

        match result {
            OperationResult::Complete(SqlResponse::QueryResult { columns, rows }) => {
                // Convert rows to HashMap with column names as keys
                rows.into_iter()
                    .map(|row| {
                        columns
                            .iter()
                            .zip(row.iter())
                            .map(|(col, val)| (col.clone(), val.clone()))
                            .collect()
                    })
                    .collect()
            }
            OperationResult::Complete(SqlResponse::Error(err)) => {
                panic!("Query failed: {} - Error: {}", sql, err)
            }
            _ => panic!("Query failed: {}", sql),
        }
    }

    /// Query and return count of rows
    pub fn query_count(&mut self, sql: &str) -> usize {
        self.query(sql).len()
    }

    /// Execute SQL expecting an error
    pub fn exec_error(&mut self, sql: &str) -> String {
        let tx = self.current_tx.unwrap_or_else(|| self.next_timestamp());
        let log_index = self.next_log_index();

        match self.engine.apply_operation(
            SqlOperation::Execute {
                sql: sql.to_string(),
                params: None,
            },
            tx,
            log_index,
        ) {
            OperationResult::Complete(SqlResponse::Error(err)) => err,
            _ => panic!("Expected error for SQL: {}", sql),
        }
    }

    /// Assert query returns expected number of rows
    pub fn assert_row_count(&mut self, sql: &str, expected: usize) {
        let count = self.query_count(sql);
        assert_eq!(
            count, expected,
            "Query '{}' returned {} rows, expected {}",
            sql, count, expected
        );
    }

    /// Assert query result contains expected value
    pub fn assert_query_value(&mut self, sql: &str, column: &str, expected: Value) {
        let results = self.query(sql);
        assert!(!results.is_empty(), "Query '{}' returned no results", sql);

        let value = results[0]
            .get(column)
            .unwrap_or_else(|| panic!("Column '{}' not found in results", column));

        assert_eq!(
            value, &expected,
            "Query '{}' column '{}' = '{:?}', expected '{:?}'",
            sql, column, value, expected
        );
    }

    /// Assert error contains expected text
    pub fn assert_error_contains(&mut self, sql: &str, expected: &str) {
        let error = self.exec_error(sql);
        assert!(
            error.contains(expected),
            "Error '{}' does not contain '{}'",
            error,
            expected
        );
    }

    /// Assert that a query uses an index scan (checks EXPLAIN output)
    pub fn assert_uses_index(&mut self, sql: &str, index_name: &str) {
        let explain_sql = format!("EXPLAIN {}", sql);
        let plan = self.exec_response(&explain_sql);
        let plan_text = plan.as_explain_plan()
            .expect("Expected EXPLAIN plan response");

        assert!(
            plan_text.contains("Index scan") || plan_text.contains("Index range scan"),
            "Query '{}' does not use index scan.\nPlan:\n{}",
            sql, plan_text
        );

        assert!(
            plan_text.contains(index_name),
            "Query '{}' does not use index '{}'.\nPlan:\n{}",
            sql, index_name, plan_text
        );
    }

    /// Assert that a query does NOT use an index scan (checks EXPLAIN output)
    pub fn assert_no_index_scan(&mut self, sql: &str) {
        let explain_sql = format!("EXPLAIN {}", sql);
        let plan = self.exec_response(&explain_sql);
        let plan_text = plan.as_explain_plan()
            .expect("Expected EXPLAIN plan response");

        assert!(
            !plan_text.contains("Index scan") && !plan_text.contains("Index range scan"),
            "Query '{}' unexpectedly uses index scan.\nPlan:\n{}",
            sql, plan_text
        );
    }
}

impl Default for TestContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating test tables with data
pub struct TableBuilder<'a> {
    ctx: &'a mut TestContext,
    table_name: String,
}

impl<'a> TableBuilder<'a> {
    pub fn new(ctx: &'a mut TestContext, table_name: &str) -> Self {
        Self {
            ctx,
            table_name: table_name.to_string(),
        }
    }

    /// Create a simple test table
    pub fn create_simple(&mut self, columns: &str) -> &mut Self {
        self.ctx
            .exec(&format!("CREATE TABLE {} ({})", self.table_name, columns));
        self
    }

    /// Create standard test table with id, name, value
    pub fn create_standard(&mut self) -> &mut Self {
        self.create_simple("id INTEGER PRIMARY KEY, name TEXT, value INTEGER")
    }

    /// Insert test data using VALUES
    pub fn insert_values(&mut self, values: &str) -> &mut Self {
        self.ctx.exec(&format!(
            "INSERT INTO {} VALUES {}",
            self.table_name, values
        ));
        self
    }

    /// Insert multiple rows of test data
    pub fn insert_rows(&mut self, count: usize) -> &mut Self {
        let values: Vec<String> = (1..=count)
            .map(|i| format!("({}, 'item_{}', {})", i, i, i * 10))
            .collect();
        self.insert_values(&values.join(", "))
    }
}

/// Helper to create a test context with standard setup
pub fn setup_test() -> TestContext {
    let mut ctx = TestContext::new();
    ctx.begin();
    ctx
}

/// Helper to create test context with tables
pub fn setup_with_tables() -> TestContext {
    let mut ctx = setup_test();

    // Create common test tables
    TableBuilder::new(&mut ctx, "users")
        .create_simple("id INTEGER PRIMARY KEY, name TEXT, age INTEGER")
        .insert_values("(1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35)");

    TableBuilder::new(&mut ctx, "orders")
        .create_simple("id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER")
        .insert_values("(1, 1, 100), (2, 1, 200), (3, 2, 150)");

    ctx
}

/// Assertion helpers for common patterns
#[macro_export]
macro_rules! assert_query_eq {
    ($ctx:expr, $sql:expr, $expected:expr) => {
        let results = $ctx.query($sql);
        assert_eq!(results, $expected, "Query '{}' results don't match", $sql);
    };
}

#[macro_export]
macro_rules! assert_rows {
    ($ctx:expr, $sql:expr, $count:expr) => {
        $ctx.assert_row_count($sql, $count);
    };
}

#[macro_export]
macro_rules! assert_error {
    ($ctx:expr, $sql:expr) => {
        $ctx.exec_error($sql);
    };
    ($ctx:expr, $sql:expr, $contains:expr) => {
        $ctx.assert_error_contains($sql, $contains);
    };
}

/// Test data generators
pub mod data {
    /// Generate test integers
    pub fn ints(start: i32, end: i32) -> Vec<String> {
        (start..=end).map(|i| i.to_string()).collect()
    }

    /// Generate test strings
    pub fn strings(prefix: &str, count: usize) -> Vec<String> {
        (1..=count).map(|i| format!("'{}{}'", prefix, i)).collect()
    }

    /// Generate test rows for INSERT
    pub fn rows(count: usize) -> String {
        let rows: Vec<String> = (1..=count)
            .map(|i| format!("({}, 'item_{}', {})", i, i, i * 10))
            .collect();
        rows.join(", ")
    }
}
