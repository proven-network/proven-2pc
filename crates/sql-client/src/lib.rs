//! SQL client for coordinator-based transactions

use proven_coordinator::Transaction;
use proven_sql::{SqlOperation, SqlResponse, Value};

/// SQL client that works with coordinator transactions
#[derive(Clone)]
pub struct SqlClient {
    /// The transaction this client is associated with
    transaction: Transaction,
}

impl SqlClient {
    /// Create a new SQL client for a transaction
    pub fn new(transaction: Transaction) -> Self {
        Self { transaction }
    }

    /// Execute a SQL query and return results
    pub async fn query(
        &self,
        stream_name: impl Into<String>,
        sql: impl Into<String>,
    ) -> Result<SqlResult, SqlError> {
        let operation = SqlOperation::Query {
            sql: sql.into(),
            params: None,
        };

        let response = self
            .execute_operation(stream_name.into(), operation)
            .await?;

        match response {
            SqlResponse::QueryResult { columns, rows } => Ok(SqlResult { columns, rows }),
            SqlResponse::Error(e) => Err(SqlError::QueryError(e)),
            _ => Err(SqlError::UnexpectedResponse),
        }
    }

    /// Execute a SQL statement (INSERT, UPDATE, DELETE, etc.)
    pub async fn execute(
        &self,
        stream_name: impl Into<String>,
        sql: impl Into<String>,
    ) -> Result<u64, SqlError> {
        let operation = SqlOperation::Execute {
            sql: sql.into(),
            params: None,
        };

        let response = self
            .execute_operation(stream_name.into(), operation)
            .await?;

        match response {
            SqlResponse::ExecuteResult { rows_affected, .. } => {
                Ok(rows_affected.unwrap_or(0) as u64)
            }
            SqlResponse::Error(e) => Err(SqlError::ExecuteError(e)),
            _ => Err(SqlError::UnexpectedResponse),
        }
    }

    /// Execute a DDL statement (CREATE TABLE, DROP TABLE, etc.)
    pub async fn execute_ddl(
        &self,
        stream_name: impl Into<String>,
        sql: impl Into<String>,
    ) -> Result<(), SqlError> {
        let operation = SqlOperation::Execute {
            sql: sql.into(),
            params: None,
        };

        let response = self
            .execute_operation(stream_name.into(), operation)
            .await?;

        match response {
            SqlResponse::ExecuteResult { .. } => Ok(()),
            SqlResponse::Error(e) => Err(SqlError::DdlError(e)),
            _ => Err(SqlError::UnexpectedResponse),
        }
    }

    /// Insert a single row and return rows affected
    pub async fn insert(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        columns: &[&str],
        values: &[&str],
    ) -> Result<u64, SqlError> {
        let stream_name = stream_name.into();
        if columns.len() != values.len() {
            return Err(SqlError::InvalidParameters(
                "Column count doesn't match value count".to_string(),
            ));
        }

        let columns_str = columns.join(", ");
        let values_str = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns_str, values_str
        );

        self.execute(stream_name, sql).await
    }

    /// Insert a single row using parameterized query
    pub async fn insert_with_params(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        columns: &[&str],
        values: Vec<Value>,
    ) -> Result<u64, SqlError> {
        let stream_name = stream_name.into();
        if columns.len() != values.len() {
            return Err(SqlError::InvalidParameters(
                "Column count doesn't match value count".to_string(),
            ));
        }

        let columns_str = columns.join(", ");
        let placeholders = vec!["?"; values.len()].join(", ");

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns_str, placeholders
        );

        self.execute_with_params(stream_name, sql, values).await
    }

    /// Update rows in a table
    pub async fn update(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        set_clause: &str,
        where_clause: Option<&str>,
    ) -> Result<u64, SqlError> {
        let stream_name = stream_name.into();
        let sql = if let Some(where_clause) = where_clause {
            format!("UPDATE {} SET {} WHERE {}", table, set_clause, where_clause)
        } else {
            format!("UPDATE {} SET {}", table, set_clause)
        };

        self.execute(stream_name, sql).await
    }

    /// Delete rows from a table
    pub async fn delete(
        &self,
        stream: impl Into<String>,
        table: &str,
        where_clause: Option<&str>,
    ) -> Result<u64, SqlError> {
        let stream = stream.into();
        let sql = if let Some(where_clause) = where_clause {
            format!("DELETE FROM {} WHERE {}", table, where_clause)
        } else {
            format!("DELETE FROM {}", table)
        };

        self.execute(stream, sql).await
    }

    /// Select from a table with optional WHERE clause
    pub async fn select(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        columns: &[&str],
        where_clause: Option<&str>,
    ) -> Result<SqlResult, SqlError> {
        let stream_name = stream_name.into();
        let columns_str = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };

        let sql = if let Some(where_clause) = where_clause {
            format!(
                "SELECT {} FROM {} WHERE {}",
                columns_str, table, where_clause
            )
        } else {
            format!("SELECT {} FROM {}", columns_str, table)
        };

        self.query(stream_name, sql).await
    }

    /// Execute a parameterized SQL query
    pub async fn query_with_params(
        &self,
        stream_name: impl Into<String>,
        sql: impl Into<String>,
        params: Vec<Value>,
    ) -> Result<SqlResult, SqlError> {
        let operation = SqlOperation::Query {
            sql: sql.into(),
            params: Some(params),
        };

        let response = self
            .execute_operation(stream_name.into(), operation)
            .await?;

        match response {
            SqlResponse::QueryResult { columns, rows } => Ok(SqlResult { columns, rows }),
            SqlResponse::Error(e) => Err(SqlError::QueryError(e)),
            _ => Err(SqlError::UnexpectedResponse),
        }
    }

    /// Execute a parameterized SQL statement
    pub async fn execute_with_params(
        &self,
        stream_name: impl Into<String>,
        sql: impl Into<String>,
        params: Vec<Value>,
    ) -> Result<u64, SqlError> {
        let operation = SqlOperation::Execute {
            sql: sql.into(),
            params: Some(params),
        };

        let response = self
            .execute_operation(stream_name.into(), operation)
            .await?;

        match response {
            SqlResponse::ExecuteResult { rows_affected, .. } => {
                Ok(rows_affected.unwrap_or(0) as u64)
            }
            SqlResponse::Error(e) => Err(SqlError::ExecuteError(e)),
            _ => Err(SqlError::UnexpectedResponse),
        }
    }

    /// Count rows in a table with optional WHERE clause
    pub async fn count(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        where_clause: Option<&str>,
    ) -> Result<u64, SqlError> {
        let stream_name = stream_name.into();
        let sql = if let Some(where_clause) = where_clause {
            format!("SELECT COUNT(*) FROM {} WHERE {}", table, where_clause)
        } else {
            format!("SELECT COUNT(*) FROM {}", table)
        };

        let result = self.query(stream_name, sql).await?;

        // Extract count from result
        if let Some(row) = result.rows.first()
            && let Some(value) = row.first()
        {
            // Try to extract integer value
            if let Value::I64(i) = value {
                return Ok(*i as u64);
            }
            // Try to parse string representation
            let s = value.to_string();
            if let Ok(count) = s.parse::<u64>() {
                return Ok(count);
            }
        }

        Err(SqlError::InvalidResponse(
            "Failed to parse count result".to_string(),
        ))
    }

    /// Create a table
    pub async fn create_table(
        &self,
        stream_name: impl Into<String>,
        table: &str,
        columns: &str,
    ) -> Result<(), SqlError> {
        let stream = stream_name.into();
        let sql = format!("CREATE TABLE {} ({})", table, columns);
        self.execute_ddl(stream, sql).await
    }

    /// Drop a table
    pub async fn drop_table(
        &self,
        stream_name: impl Into<String>,
        table: &str,
    ) -> Result<(), SqlError> {
        let stream_name = stream_name.into();
        let sql = format!("DROP TABLE {}", table);
        self.execute_ddl(stream_name, sql).await
    }

    /// Create an index
    pub async fn create_index(
        &self,
        stream_name: impl Into<String>,
        index_name: &str,
        table: &str,
        columns: &[&str],
    ) -> Result<(), SqlError> {
        let stream_name = stream_name.into();
        let columns_str = columns.join(", ");
        let sql = format!("CREATE INDEX {} ON {} ({})", index_name, table, columns_str);
        self.execute_ddl(stream_name, sql).await
    }

    /// Execute an operation and deserialize the response
    async fn execute_operation(
        &self,
        stream_name: String,
        operation: SqlOperation,
    ) -> Result<SqlResponse, SqlError> {
        // Serialize the operation
        let operation_bytes = serde_json::to_vec(&operation)
            .map_err(|e| SqlError::SerializationError(e.to_string()))?;

        // Execute through the transaction
        let response_bytes = self
            .transaction
            .execute(stream_name, operation_bytes)
            .await
            .map_err(|e| SqlError::CoordinatorError(e.to_string()))?;

        // Deserialize the response
        let response = serde_json::from_slice(&response_bytes)
            .map_err(|e| SqlError::DeserializationError(e.to_string()))?;

        Ok(response)
    }
}

/// Result of a SQL query
#[derive(Debug, Clone)]
pub struct SqlResult {
    /// Column names
    pub columns: Vec<String>,
    /// Rows of data (each row is a vector of Values)
    pub rows: Vec<Vec<Value>>,
}

impl SqlResult {
    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Get the number of rows
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Get a single value from the first row and column as string
    pub fn single_value(&self) -> Option<String> {
        self.rows
            .first()
            .and_then(|row| row.first())
            .map(|v| v.to_string())
    }

    /// Get values from a specific column as strings
    pub fn column_values(&self, column_name: &str) -> Vec<String> {
        if let Some(col_index) = self.columns.iter().position(|c| c == column_name) {
            self.rows
                .iter()
                .filter_map(|row| row.get(col_index))
                .map(|v| v.to_string())
                .collect()
        } else {
            Vec::new()
        }
    }
}

/// SQL-specific error type
#[derive(Debug, thiserror::Error)]
pub enum SqlError {
    #[error("Coordinator error: {0}")]
    CoordinatorError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Execute error: {0}")]
    ExecuteError(String),

    #[error("DDL error: {0}")]
    DdlError(String),

    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Unexpected response type")]
    UnexpectedResponse,
}
