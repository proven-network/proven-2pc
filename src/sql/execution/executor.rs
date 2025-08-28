//! New executor with iterator-based execution model
//!
//! This executor works with the plan nodes and uses iterators for
//! efficient streaming execution.

use crate::error::{Error, Result};
use crate::sql::planner::plan::{self, Node, Plan, Rows};
use crate::sql::types::schema::Table;
use crate::sql::types::value::{Row, Value};
use crate::transaction::Transaction;
use crate::transaction_id::TransactionContext;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

/// SQL query executor
pub struct Executor {
    /// Registered table schemas
    schemas: Arc<HashMap<String, Table>>,
}

impl Executor {
    /// Create a new executor
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(HashMap::new()),
        }
    }

    /// Update schemas (called when tables are created/dropped)
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = Arc::new(schemas);
    }

    /// Execute a plan within a transaction
    pub fn execute(
        &self,
        plan: Plan,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Select(node) => {
                let rows = self.execute_node(&*node, tx, context)?;
                let mut collected = Vec::new();
                for row in rows {
                    collected.push(row?);
                }

                // Get column names from the top node
                let columns = self.get_column_names(&*node);

                Ok(ExecutionResult::Select {
                    columns,
                    rows: collected,
                })
            }

            Plan::Insert {
                table,
                columns,
                source,
            } => {
                let rows = self.execute_node(&*source, tx, context)?;
                let mut count = 0;

                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                for row_result in rows {
                    let row = row_result?;

                    // Reorder columns if needed
                    let final_row = if let Some(ref col_indices) = columns {
                        let mut reordered = vec![Value::Null; schema.columns.len()];
                        for (src_idx, &dest_idx) in col_indices.iter().enumerate() {
                            if src_idx < row.len() {
                                reordered[dest_idx] = row[src_idx].clone();
                            }
                        }
                        reordered
                    } else {
                        row.to_vec()
                    };

                    // Validate and insert
                    schema.validate_row(&final_row)?;
                    tx.insert(&table, final_row)?;
                    count += 1;
                }

                Ok(ExecutionResult::Modified(count))
            }

            Plan::Update {
                table,
                assignments,
                source,
            } => {
                let rows = self.execute_node(&*source, tx, context)?;
                let mut count = 0;

                for row_result in rows {
                    let row = row_result?;
                    let row_id = row[0].clone(); // Assume first column is row ID

                    // Apply assignments
                    let mut updated = row.to_vec();
                    for (col_idx, expr) in &assignments {
                        updated[*col_idx] = expr.evaluate(Some(&row), context)?;
                    }

                    // Update the row
                    if let Value::Integer(id) = row_id {
                        tx.write(&table, id as u64, updated)?;
                        count += 1;
                    }
                }

                Ok(ExecutionResult::Modified(count))
            }

            Plan::Delete { table, source } => {
                let rows = self.execute_node(&*source, tx, context)?;
                let mut count = 0;

                for row_result in rows {
                    let row = row_result?;
                    let row_id = row[0].clone(); // Assume first column is row ID

                    if let Value::Integer(id) = row_id {
                        tx.delete(&table, id as u64)?;
                        count += 1;
                    }
                }

                Ok(ExecutionResult::Modified(count))
            }

            Plan::CreateTable { name, schema: _ } => {
                // TODO: Persist schema
                Ok(ExecutionResult::DDL(format!("Table '{}' created", name)))
            }

            Plan::DropTable { name, if_exists } => {
                if !self.schemas.contains_key(&name) && !if_exists {
                    return Err(Error::TableNotFound(name));
                }

                // TODO: Drop table data and schema
                Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)))
            }

            Plan::Begin { read_only } => Ok(ExecutionResult::Transaction(
                if read_only {
                    "BEGIN READ ONLY"
                } else {
                    "BEGIN"
                }
                .to_string(),
            )),

            Plan::Commit => Ok(ExecutionResult::Transaction("COMMIT".to_string())),

            Plan::Rollback => Ok(ExecutionResult::Transaction("ROLLBACK".to_string())),
        }
    }

    /// Execute a plan node, returning a row iterator
    fn execute_node(
        &self,
        node: &Node,
        tx: &Transaction,
        context: &TransactionContext,
    ) -> Result<Rows> {
        match node {
            Node::Scan { table, .. } => {
                // Scan the table
                let rows = tx.scan(table, None, None)?;
                let iter = rows.into_iter().map(|(_, row)| Ok(Arc::new(row)));
                Ok(Box::new(iter))
            }

            Node::Filter { source, predicate } => {
                let source_rows = self.execute_node(source, tx, context)?;
                let ctx = context.clone();
                let pred = predicate.clone();

                let filtered = source_rows.filter_map(move |row_result| match row_result {
                    Ok(row) => match pred.evaluate(Some(&row), &ctx) {
                        Ok(Value::Boolean(true)) => Some(Ok(row)),
                        Ok(Value::Boolean(false)) | Ok(Value::Null) => None,
                        Ok(_) => Some(Err(Error::TypeMismatch {
                            expected: "boolean".into(),
                            found: "non-boolean".into(),
                        })),
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                });

                Ok(Box::new(filtered))
            }

            Node::Projection {
                source,
                expressions,
                ..
            } => {
                let source_rows = self.execute_node(source, tx, context)?;
                let ctx = context.clone();
                let exprs = expressions.clone();

                let projected = source_rows.map(move |row_result| {
                    let row = row_result?;
                    let mut new_row = Vec::new();
                    for expr in &exprs {
                        new_row.push(expr.evaluate(Some(&row), &ctx)?);
                    }
                    Ok(Arc::new(new_row))
                });

                Ok(Box::new(projected))
            }

            Node::Order { source, order_by } => {
                let source_rows = self.execute_node(source, tx, context)?;

                // Collect all rows for sorting
                let mut rows_with_keys: Vec<(Arc<Row>, Vec<Value>)> = Vec::new();
                for row_result in source_rows {
                    let row = row_result?;
                    let mut keys = Vec::new();
                    for (expr, _) in order_by {
                        keys.push(expr.evaluate(Some(&row), context)?);
                    }
                    rows_with_keys.push((row, keys));
                }

                // Sort by the keys
                rows_with_keys.sort_by(|a, b| {
                    for (i, (_, dir)) in order_by.iter().enumerate() {
                        let ordering = a.1[i].partial_cmp(&b.1[i]).unwrap_or(Ordering::Equal);

                        let ordering = match dir {
                            plan::Direction::Ascending => ordering,
                            plan::Direction::Descending => ordering.reverse(),
                        };

                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                    }
                    Ordering::Equal
                });

                // Return sorted rows
                let sorted = rows_with_keys.into_iter().map(|(row, _)| Ok(row));
                Ok(Box::new(sorted))
            }

            Node::Limit { source, limit } => {
                let source_rows = self.execute_node(source, tx, context)?;
                Ok(Box::new(source_rows.take(*limit)))
            }

            Node::Offset { source, offset } => {
                let source_rows = self.execute_node(source, tx, context)?;
                Ok(Box::new(source_rows.skip(*offset)))
            }

            Node::Values { rows } => {
                let ctx = context.clone();
                let rows = rows.clone();

                let values_iter = rows.into_iter().map(move |exprs| {
                    let mut row = Vec::new();
                    for expr in exprs {
                        row.push(expr.evaluate(None, &ctx)?);
                    }
                    Ok(Arc::new(row))
                });

                Ok(Box::new(values_iter))
            }

            Node::Aggregate {
                source,
                group_by,
                aggregates,
            } => {
                use super::aggregator::{Aggregate, Aggregator};

                let source_rows = self.execute_node(source, tx, context)?;

                // Convert plan aggregates to aggregator aggregates
                let agg_funcs = aggregates
                    .iter()
                    .map(|f| match f {
                        plan::AggregateFunc::Count(e) => Aggregate::Count(e.clone()),
                        plan::AggregateFunc::Sum(e) => Aggregate::Sum(e.clone()),
                        plan::AggregateFunc::Avg(e) => Aggregate::Avg(e.clone()),
                        plan::AggregateFunc::Min(e) => Aggregate::Min(e.clone()),
                        plan::AggregateFunc::Max(e) => Aggregate::Max(e.clone()),
                    })
                    .collect();

                // Create aggregator and add all rows
                let mut aggregator = Aggregator::new(group_by.clone(), agg_funcs);
                for row_result in source_rows {
                    aggregator.add(row_result?, context)?;
                }

                // Finalize and return results
                let result_rows = aggregator.finalize()?;
                Ok(Box::new(result_rows.into_iter().map(Ok)))
            }

            Node::HashJoin {
                left,
                right,
                left_col,
                right_col,
                join_type,
            } => {
                use super::join::HashJoiner;

                let left_rows = self.execute_node(left, tx, context)?;
                let right_rows = self.execute_node(right, tx, context)?;

                // Build phase: hash the left side
                let mut joiner = HashJoiner::new(*left_col, *right_col);
                let mut _left_count = 0;
                for row_result in left_rows {
                    joiner.build(row_result?)?;
                    _left_count += 1;
                }

                // Probe phase: join with right side
                let mut results = Vec::new();
                for row_result in right_rows {
                    let right_row = row_result?;
                    let joined = match join_type {
                        plan::JoinType::Inner => joiner.probe(&right_row),
                        plan::JoinType::Left => {
                            joiner.probe_outer(&right_row, left.column_count(&self.schemas))
                        }
                        _ => {
                            return Err(Error::ExecutionError(
                                "Only INNER and LEFT joins supported for hash join".into(),
                            ));
                        }
                    };
                    results.extend(joined);
                }

                Ok(Box::new(results.into_iter().map(Ok)))
            }

            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => {
                use super::join::NestedLoopJoiner;

                let left_rows = self.execute_node(left, tx, context)?;
                let right_rows = self.execute_node(right, tx, context)?;

                // Collect all left rows
                let mut joiner = NestedLoopJoiner::new();
                for row_result in left_rows {
                    joiner.add_left(row_result?);
                }

                // Join with each right row
                let mut results = Vec::new();
                let ctx = context.clone();
                let pred = predicate.clone();
                for row_result in right_rows {
                    let right_row = row_result?;

                    // Evaluate predicate for each left-right pair
                    let joined = match join_type {
                        plan::JoinType::Inner => {
                            let pred = pred.clone();
                            joiner.join_with(&right_row, |left_row, right_row| {
                                let mut combined = left_row.clone();
                                combined.extend_from_slice(right_row);
                                match pred.evaluate(Some(&combined), &ctx) {
                                    Ok(Value::Boolean(true)) => true,
                                    _ => false,
                                }
                            })
                        }
                        plan::JoinType::Left => {
                            let pred = pred.clone();
                            joiner.join_outer_with(
                                &right_row,
                                |left_row, right_row| {
                                    let mut combined = left_row.clone();
                                    combined.extend_from_slice(right_row);
                                    match pred.evaluate(Some(&combined), &ctx) {
                                        Ok(Value::Boolean(true)) => true,
                                        _ => false,
                                    }
                                },
                                left.column_count(&self.schemas),
                            )
                        }
                        _ => {
                            return Err(Error::ExecutionError(
                                "Only INNER and LEFT joins supported for nested loop join".into(),
                            ));
                        }
                    };
                    results.extend(joined);
                }

                Ok(Box::new(results.into_iter().map(Ok)))
            }

            Node::Nothing => Ok(Box::new(std::iter::empty())),
        }
    }

    /// Get column names for a node
    fn get_column_names(&self, node: &Node) -> Vec<String> {
        match node {
            Node::Projection {
                aliases,
                expressions,
                ..
            } => aliases
                .iter()
                .zip(expressions.iter())
                .enumerate()
                .map(|(i, (alias, _))| alias.clone().unwrap_or_else(|| format!("column_{}", i)))
                .collect(),
            Node::Scan { table, .. } => {
                if let Some(schema) = self.schemas.get(table) {
                    schema.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    vec![]
                }
            }
            Node::Filter { source, .. }
            | Node::Order { source, .. }
            | Node::Limit { source, .. }
            | Node::Offset { source, .. } => self.get_column_names(source),
            _ => vec![],
        }
    }
}

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecutionResult {
    /// SELECT query results with Arc-wrapped rows for streaming
    Select {
        columns: Vec<String>,
        rows: Vec<Arc<Row>>,
    },
    /// Number of rows affected by INSERT/UPDATE/DELETE
    Modified(usize),
    /// DDL operation completed
    DDL(String),
    /// Transaction control operation
    Transaction(String),
}

#[cfg(test)]
mod tests {
    use super::{ExecutionResult, Executor};
    use crate::error::Result;
    use crate::hlc::{HlcTimestamp, NodeId};
    use crate::lock::LockManager;
    use crate::sql::parser::Parser;
    use crate::sql::planner::Planner;
    use crate::sql::types::schema::{Column, Table};
    use crate::sql::types::value::{DataType, Value};
    use crate::storage::{Column as StorageColumn, Schema as StorageSchema, Storage};
    use crate::transaction::Transaction;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn setup_test_db() -> (Executor, Planner, Transaction) {
        // Create storage
        let storage = Arc::new(Storage::new());

        // Define users table schema
        let users_table = Table::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("name".to_string(), DataType::String),
                Column::new("email".to_string(), DataType::String).unique(),
                Column::new("age".to_string(), DataType::Integer).nullable(true),
            ],
        )
        .unwrap();

        // Create table in storage
        let storage_schema = StorageSchema::new(vec![
            StorageColumn::new("id".to_string(), DataType::Integer),
            StorageColumn::new("name".to_string(), DataType::String),
            StorageColumn::new("email".to_string(), DataType::String),
            StorageColumn::new("age".to_string(), DataType::Integer),
        ])
        .unwrap();
        storage
            .create_table("users".to_string(), storage_schema)
            .unwrap();

        // Create schemas map
        let mut schemas = HashMap::new();
        schemas.insert("users".to_string(), users_table);

        // Create executor and planner
        let mut executor = Executor::new();
        executor.update_schemas(schemas.clone());
        let planner = Planner::new(schemas);

        // Create lock manager
        let lock_manager = Arc::new(LockManager::new());

        // Create transaction
        let global_id = HlcTimestamp::new(1000, 0, NodeId::new(1));
        let tx = Transaction::new(global_id, lock_manager, storage);

        (executor, planner, tx)
    }

    fn execute_sql(
        executor: &Executor,
        planner: &Planner,
        tx: &Transaction,
        sql: &str,
    ) -> Result<ExecutionResult> {
        let stmt = Parser::parse(sql)?;
        let plan = planner.plan(stmt)?;
        executor.execute(plan, tx, &tx.context)
    }

    #[test]
    fn test_insert_and_select() {
        let (executor, planner, tx) = setup_test_db();

        // Insert a user
        let insert_sql =
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)";
        let result = execute_sql(&executor, &planner, &tx, insert_sql);
        if let Err(e) = &result {
            eprintln!("INSERT test failed: {:?}", e);
        }
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Modified(count)) = result {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Modified result");
        }

        // Select the user
        let select_sql = "SELECT id, name, email, age FROM users";
        let result = execute_sql(&executor, &planner, &tx, select_sql);
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { columns, rows }) = result {
            assert_eq!(columns, vec!["id", "name", "email", "age"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));
            assert_eq!(rows[0][2], Value::String("alice@example.com".to_string()));
            assert_eq!(rows[0][3], Value::Integer(30));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_where_clause() {
        let (executor, planner, tx) = setup_test_db();

        // Insert multiple users
        execute_sql(
            &executor,
            &planner,
            &tx,
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        )
        .unwrap();
        execute_sql(
            &executor,
            &planner,
            &tx,
            "INSERT INTO users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)",
        )
        .unwrap();
        execute_sql(&executor, &planner, &tx,
            "INSERT INTO users (id, name, email, age) VALUES (3, 'Charlie', 'charlie@example.com', 35)").unwrap();

        // Select with WHERE clause
        let select_sql = "SELECT name FROM users WHERE age > 25";
        let result = execute_sql(&executor, &planner, &tx, select_sql);
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 2);
            // Results should be Alice (30) and Charlie (35)
            let names: Vec<String> = rows
                .iter()
                .map(|r| r[0].clone())
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(names.contains(&"Alice".to_string()));
            assert!(names.contains(&"Charlie".to_string()));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_order_by() {
        let (executor, planner, tx) = setup_test_db();

        // Insert users
        execute_sql(&executor, &planner, &tx,
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Charlie', 'charlie@example.com', 35)").unwrap();
        execute_sql(
            &executor,
            &planner,
            &tx,
            "INSERT INTO users (id, name, email, age) VALUES (2, 'Alice', 'alice@example.com', 30)",
        )
        .unwrap();
        execute_sql(
            &executor,
            &planner,
            &tx,
            "INSERT INTO users (id, name, email, age) VALUES (3, 'Bob', 'bob@example.com', 25)",
        )
        .unwrap();

        // Order by name ASC
        let order_sql = "SELECT name FROM users ORDER BY name ASC";
        let result = execute_sql(&executor, &planner, &tx, order_sql);
        if let Err(e) = &result {
            eprintln!("ORDER BY test failed: {:?}", e);
        }
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], Value::String("Alice".to_string()));
            assert_eq!(rows[1][0], Value::String("Bob".to_string()));
            assert_eq!(rows[2][0], Value::String("Charlie".to_string()));
        } else {
            panic!("Expected Select result");
        }

        // Order by age DESC
        let order_sql = "SELECT name, age FROM users ORDER BY age DESC";
        let result = execute_sql(&executor, &planner, &tx, order_sql);
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], Value::String("Charlie".to_string()));
            assert_eq!(rows[0][1], Value::Integer(35));
            assert_eq!(rows[2][0], Value::String("Bob".to_string()));
            assert_eq!(rows[2][1], Value::Integer(25));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_limit_offset() {
        let (executor, planner, tx) = setup_test_db();

        // Insert 5 users
        for i in 1..=5 {
            let sql = format!(
                "INSERT INTO users (id, name, email, age) VALUES ({}, 'User{}', 'user{}@example.com', {})",
                i,
                i,
                i,
                20 + i
            );
            execute_sql(&executor, &planner, &tx, &sql).unwrap();
        }

        // Test LIMIT
        let limit_sql = "SELECT name FROM users LIMIT 3";
        let result = execute_sql(&executor, &planner, &tx, limit_sql);
        if let Err(e) = &result {
            eprintln!("LIMIT test failed: {:?}", e);
        }
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 3);
        }

        // Test OFFSET
        let offset_sql = "SELECT name FROM users OFFSET 2";
        let result = execute_sql(&executor, &planner, &tx, offset_sql);
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 3); // 5 total - 2 offset = 3
        }

        // Test LIMIT with OFFSET
        let both_sql = "SELECT name FROM users LIMIT 2 OFFSET 1";
        let result = execute_sql(&executor, &planner, &tx, both_sql);
        if let Err(e) = &result {
            eprintln!("LIMIT with OFFSET test failed: {:?}", e);
        }
        assert!(result.is_ok());

        if let Ok(ExecutionResult::Select { rows, .. }) = result {
            assert_eq!(rows.len(), 2);
        }
    }
}
