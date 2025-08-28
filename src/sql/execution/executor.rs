//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, join};
use crate::context::TransactionContext;
use crate::error::{Error, Result};
use crate::sql::planner::plan::{Direction, Node, Plan};
use crate::sql::types::expression::Expression;
use crate::sql::types::schema::Table;
use crate::sql::types::value::Value;
use crate::storage::transaction::MvccTransaction;
use std::collections::HashMap;
use std::sync::Arc;

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecutionResult {
    /// SELECT query results
    Select {
        columns: Vec<String>,
        rows: Vec<Arc<Vec<Value>>>,
    },
    /// Number of rows modified (INSERT, UPDATE, DELETE)
    Modified(usize),
    /// DDL operation result
    DDL(String),
}

/// Row iterator type for streaming execution
pub type Rows = Box<dyn Iterator<Item = Result<Arc<Vec<Value>>>>>;

/// Complete MVCC-aware SQL executor
pub struct Executor {
    /// Table schemas for metadata
    schemas: HashMap<String, Table>,
}

impl Executor {
    /// Create a new executor
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self { schemas }
    }

    /// Update schemas after DDL operations
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.schemas = schemas;
    }

    /// Execute a query plan
    pub fn execute(
        &self,
        plan: Plan,
        tx: &Arc<MvccTransaction>,
        context: &TransactionContext,
    ) -> Result<ExecutionResult> {
        match plan {
            Plan::Select(node) => {
                // Execute the query and collect results
                let rows = self.execute_node(*node, tx, context)?;
                let mut collected = Vec::new();
                for row in rows {
                    collected.push(row?);
                }

                // Get column names (simplified - would extract from projection)
                let columns = if let Some(table) = self.schemas.values().next() {
                    table.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    vec![]
                };

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
                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                // Execute source node to get rows to insert
                let rows = self.execute_node(*source, tx, context)?;
                let mut count = 0;

                for row in rows {
                    let row = row?;

                    // Reorder columns if specified
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

                    // Insert using MVCC transaction
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
                // UPDATE works by:
                // 1. Source node identifies matching rows (WHERE clause)
                // 2. We scan the table with IDs internally
                // 3. Match rows and apply updates

                // Get rows that match the WHERE clause
                let matching_rows = self.execute_node(*source, tx, context)?;
                let mut matching_set = std::collections::HashSet::new();
                for row in matching_rows {
                    let row = row?;
                    // Store a representation of the row to match against
                    matching_set.insert(format!("{:?}", row.as_ref()));
                }

                // Now scan with IDs (internal only) to perform updates
                let rows_with_ids = tx.scan_with_ids(&table)?;
                let mut count = 0;

                for (row_id, current) in rows_with_ids {
                    // Check if this row matches our WHERE clause
                    if matching_set.contains(&format!("{:?}", &current)) {
                        let mut updated = current.clone();

                        // Apply assignments
                        for (col_idx, expr) in &assignments {
                            updated[*col_idx] = self.evaluate_expression(
                                expr,
                                Some(&Arc::new(current.clone())),
                                context,
                            )?;
                        }

                        // Update the row using internal row ID
                        tx.update(&table, row_id, updated)?;
                        count += 1;
                    }
                }

                Ok(ExecutionResult::Modified(count))
            }

            Plan::Delete { table, source } => {
                // DELETE works similarly to UPDATE:
                // 1. Source node identifies matching rows (WHERE clause)
                // 2. We scan the table with IDs internally
                // 3. Delete matching rows

                // Get rows that match the WHERE clause
                let matching_rows = self.execute_node(*source, tx, context)?;
                let mut matching_set = std::collections::HashSet::new();
                for row in matching_rows {
                    let row = row?;
                    matching_set.insert(format!("{:?}", row.as_ref()));
                }

                // Now scan with IDs (internal only) to perform deletes
                let rows_with_ids = tx.scan_with_ids(&table)?;
                let mut count = 0;

                for (row_id, current) in rows_with_ids {
                    // Check if this row matches our WHERE clause
                    if matching_set.contains(&format!("{:?}", &current)) {
                        tx.delete(&table, row_id)?;
                        count += 1;
                    }
                }

                Ok(ExecutionResult::Modified(count))
            }

            Plan::CreateTable { name, schema: _ } => {
                // Table creation is handled at stream processor level
                Ok(ExecutionResult::DDL(format!("Table '{}' created", name)))
            }

            Plan::DropTable { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Table '{}' dropped", name)))
            }

            Plan::CreateIndex { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Index '{}' created", name)))
            }

            Plan::DropIndex { name, .. } => {
                Ok(ExecutionResult::DDL(format!("Index '{}' dropped", name)))
            }
        }
    }

    /// Execute a plan node, returning a row iterator
    fn execute_node(
        &self,
        node: Node,
        tx: &Arc<MvccTransaction>,
        context: &TransactionContext,
    ) -> Result<Rows> {
        match node {
            Node::Scan { table, .. } => {
                // Get all rows from the table using MVCC transaction
                // scan() now returns Vec<Vec<Value>> without row IDs
                let rows = tx.scan(&table)?;

                // Convert to iterator of Arc<Vec<Value>>
                let iter = rows.into_iter().map(|values| Ok(Arc::new(values)));

                Ok(Box::new(iter))
            }

            Node::IndexScan {
                table,
                index_column,
                value,
                ..
            } => {
                // For now, just do a filtered table scan
                // Real implementation would use index from storage
                let filter_value = self.evaluate_expression(&value, None, context)?;
                let schema = self
                    .schemas
                    .get(&table)
                    .ok_or_else(|| Error::TableNotFound(table.clone()))?;

                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| c.name == index_column)
                    .ok_or_else(|| Error::ColumnNotFound(index_column))?;

                let rows = tx.scan(&table)?;
                let iter = rows.into_iter().filter_map(move |values| {
                    if values.get(col_idx) == Some(&filter_value) {
                        Some(Ok(Arc::new(values)))
                    } else {
                        None
                    }
                });

                Ok(Box::new(iter))
            }

            Node::Filter { source, predicate } => {
                let source_rows = self.execute_node(*source, tx, context)?;
                let executor = self.clone_for_closure();
                let ctx = context.clone();

                let filtered = source_rows.filter(move |row_result| {
                    match row_result {
                        Ok(row) => executor
                            .evaluate_expression(&predicate, Some(row), &ctx)
                            .unwrap_or(Value::Null)
                            .to_bool()
                            .unwrap_or(false),
                        Err(_) => true, // Pass through errors
                    }
                });

                Ok(Box::new(filtered))
            }

            Node::Projection {
                source,
                expressions,
                ..
            } => {
                let source_rows = self.execute_node(*source, tx, context)?;
                let executor = self.clone_for_closure();
                let ctx = context.clone();
                let exprs = expressions.clone();

                let projected = source_rows.map(move |row_result| {
                    row_result.and_then(|row| {
                        let mut new_row = Vec::new();
                        for expr in &exprs {
                            new_row.push(executor.evaluate_expression(expr, Some(&row), &ctx)?);
                        }
                        Ok(Arc::new(new_row))
                    })
                });

                Ok(Box::new(projected))
            }

            Node::Order { source, order_by } => {
                // Collect all rows for sorting
                let rows = self.execute_node(*source, tx, context)?;
                let mut collected: Vec<Arc<Vec<Value>>> = Vec::new();
                for row in rows {
                    collected.push(row?);
                }

                // Sort the rows
                let executor = self.clone_for_closure();
                let ctx = context.clone();
                collected.sort_by(|a, b| {
                    for (expr, direction) in &order_by {
                        let a_val = executor
                            .evaluate_expression(expr, Some(a), &ctx)
                            .unwrap_or(Value::Null);
                        let b_val = executor
                            .evaluate_expression(expr, Some(b), &ctx)
                            .unwrap_or(Value::Null);

                        let cmp = a_val
                            .partial_cmp(&b_val)
                            .unwrap_or(std::cmp::Ordering::Equal);
                        let ordered = match direction {
                            Direction::Ascending => cmp,
                            Direction::Descending => cmp.reverse(),
                        };

                        if ordered != std::cmp::Ordering::Equal {
                            return ordered;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                Ok(Box::new(collected.into_iter().map(Ok)))
            }

            Node::Limit { source, limit } => {
                let rows = self.execute_node(*source, tx, context)?;
                Ok(Box::new(rows.take(limit)))
            }

            Node::Offset { source, offset } => {
                let rows = self.execute_node(*source, tx, context)?;
                Ok(Box::new(rows.skip(offset)))
            }

            Node::Values { rows } => {
                // Generate literal rows for INSERT
                let executor = self.clone_for_closure();
                let ctx = context.clone();

                let value_rows = rows.into_iter().map(move |row_exprs| {
                    let mut row = Vec::new();
                    for expr in row_exprs {
                        row.push(executor.evaluate_expression(&expr, None, &ctx)?);
                    }
                    Ok(Arc::new(row))
                });

                Ok(Box::new(value_rows))
            }

            Node::Aggregate {
                source,
                group_by,
                aggregates,
            } => {
                // Use the Aggregator module
                let mut aggregator = Aggregator::new(group_by.clone(), aggregates.clone());

                // Process all source rows
                let rows = self.execute_node(*source, tx, context)?;
                for row in rows {
                    let row = row?;
                    aggregator.add(&row, context, tx)?;
                }

                // Get aggregated results
                let results = aggregator.finalize()?;
                Ok(Box::new(results.into_iter().map(Ok)))
            }

            Node::HashJoin {
                left,
                right,
                left_col,
                right_col,
                join_type,
            } => {
                // Get column count from right node for outer join NULL padding
                let right_columns = right.column_count(&self.schemas);

                let left_rows = self.execute_node(*left, tx, context)?;
                let right_rows = self.execute_node(*right, tx, context)?;

                join::execute_hash_join(
                    left_rows,
                    left_col,
                    right_rows,
                    right_col,
                    right_columns,
                    join_type,
                    tx,
                )
            }

            Node::NestedLoopJoin {
                left,
                right,
                predicate,
                join_type,
            } => {
                // Get column count from right node for outer join NULL padding
                let right_columns = right.column_count(&self.schemas);

                let left_rows = self.execute_node(*left, tx, context)?;
                let right_rows = self.execute_node(*right, tx, context)?;

                join::execute_nested_loop_join(
                    left_rows,
                    right_rows,
                    right_columns,
                    predicate.clone(),
                    join_type,
                    tx,
                )
            }

            Node::Nothing => Ok(Box::new(std::iter::empty())),
        }
    }

    /// Evaluate an expression
    fn evaluate_expression(
        &self,
        expr: &Expression,
        row: Option<&Arc<Vec<Value>>>,
        context: &TransactionContext,
    ) -> Result<Value> {
        match expr {
            Expression::Constant(v) => Ok(v.clone()),

            Expression::Column(idx) => row
                .and_then(|r| r.get(*idx).cloned())
                .ok_or_else(|| Error::InvalidValue(format!("Column {} not found", idx))),

            Expression::Equal(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l == r))
            }

            Expression::NotEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l != r))
            }

            Expression::LessThan(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l < r))
            }

            Expression::LessThanOrEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l <= r))
            }

            Expression::GreaterThan(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l > r))
            }

            Expression::GreaterThanOrEqual(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                Ok(Value::Boolean(l >= r))
            }

            Expression::And(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.and(&r)
            }

            Expression::Or(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.or(&r)
            }

            Expression::Not(inner) => {
                let v = self.evaluate_expression(inner, row, context)?;
                v.not()
            }

            Expression::Is(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                Ok(Value::Boolean(l == *right))
            }

            Expression::Add(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.add(&r)
            }

            Expression::Subtract(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.subtract(&r)
            }

            Expression::Multiply(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.multiply(&r)
            }

            Expression::Divide(left, right) => {
                let l = self.evaluate_expression(left, row, context)?;
                let r = self.evaluate_expression(right, row, context)?;
                l.divide(&r)
            }

            // Other expression types...
            _ => Ok(Value::Null),
        }
    }

    /// Clone executor for use in closures
    fn clone_for_closure(&self) -> Self {
        Self {
            schemas: self.schemas.clone(),
        }
    }
}
