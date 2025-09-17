//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, expression, join};
use crate::error::{Error, Result};
use crate::operators;
use crate::planning::plan::{Node, Plan};
use crate::storage::{MvccStorage, read_ops};
use crate::stream::TransactionContext;
use crate::types::query::Rows;
use crate::types::value::Value;
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
    Ddl(String),
}

/// Execute a query plan with parameters
pub fn execute_with_params(
    plan: Plan,
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    match plan {
        Plan::Select(node) => {
            // SELECT uses immutable storage reference
            super::select::execute_select(*node, storage, tx_ctx, params)
        }

        Plan::Insert {
            table,
            columns,
            source,
        } => {
            // INSERT uses write execution with phased approach
            super::insert::execute_insert(table, columns, *source, storage, tx_ctx, params)
        }
        Plan::Update {
            table,
            assignments,
            source,
        } => {
            // UPDATE uses write execution with phased approach
            super::update::execute_update(table, assignments, *source, storage, tx_ctx, params)
        }

        Plan::Delete { table, source } => {
            // DELETE uses write execution with phased approach
            super::delete::execute_delete(table, *source, storage, tx_ctx, params)
        }

        // DDL operations are delegated to storage
        Plan::CreateTable { .. }
        | Plan::DropTable { .. }
        | Plan::CreateIndex { .. }
        | Plan::DropIndex { .. } => {
            let result_msg = storage.execute_ddl(&plan, tx_ctx.id)?;
            Ok(ExecutionResult::Ddl(result_msg))
        }
    }
}

/// Execute a plan node for reading with immutable storage reference
pub fn execute_node_read<'a>(
    node: Node,
    storage: &'a MvccStorage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<Rows<'a>> {
    match node {
        Node::Scan { table, .. } => {
            // True streaming with immutable storage!
            let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);

            Ok(Box::new(iter))
        }

        Node::IndexScan {
            table,
            index_name,
            values,
            ..
        } => {
            // Evaluate the lookup values
            let mut filter_values = Vec::new();
            for value_expr in &values {
                filter_values.push(expression::evaluate(value_expr, None, tx_ctx, params)?);
            }

            // Try to use index lookup first
            if let Some(versioned_table) = storage.tables.get(&table) {
                // Check if this index exists
                if versioned_table.index_columns.contains_key(&index_name) {
                    // Use index lookup for O(log n) performance
                    let rows = versioned_table.index_lookup(
                        &index_name,
                        filter_values.clone(),
                        tx_ctx.id,
                        tx_ctx.timestamp,
                    );

                    // Convert to iterator format
                    let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                    return Ok(Box::new(iter));
                }
            }

            // Fall back to filtered table scan if no index exists
            // We need to check if we can resolve the index_name to columns
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Check if we can get column info from the versioned table's index_columns
            let column_names = if let Some(versioned_table) = storage.tables.get(&table) {
                versioned_table.index_columns.get(&index_name).cloned()
            } else {
                // Fallback: assume index_name is a column name for backward compatibility
                if schema.columns.iter().any(|c| c.name == index_name) {
                    Some(vec![index_name.clone()])
                } else {
                    None
                }
            };

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                        return Ok(Box::new(iter));
                    }
                }

                // Filter by checking all column values match
                if col_indices.len() == filter_values.len() {
                    let iter =
                        read_ops::scan_iter(storage, tx_ctx, &table)?.filter_map(move |row| {
                            // Check if all indexed columns match the filter values
                            for (idx, expected_val) in col_indices.iter().zip(&filter_values) {
                                if row.get(*idx) != Some(expected_val) {
                                    return None;
                                }
                            }
                            Some(Ok(row))
                        });
                    return Ok(Box::new(iter));
                }
            }

            // Can't determine columns, fall back to full scan
            let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
            Ok(Box::new(iter))
        }

        Node::IndexRangeScan {
            table,
            index_name,
            start,
            start_inclusive,
            end,
            end_inclusive,
            reverse,
            ..
        } => {
            // Evaluate range bounds
            let start_values = start
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| expression::evaluate(e, None, tx_ctx, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let end_values = end
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| expression::evaluate(e, None, tx_ctx, params))
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            // Try to use index range lookup
            if let Some(versioned_table) = storage.tables.get(&table)
                && versioned_table.index_columns.contains_key(&index_name)
            {
                // Use index range lookup for O(log n) performance
                let rows = versioned_table.index_range_lookup(
                    &index_name,
                    start_values,
                    start_inclusive,
                    end_values,
                    end_inclusive,
                    reverse,
                    tx_ctx.id,
                    tx_ctx.timestamp,
                );

                // Convert to iterator format
                let iter = rows.into_iter().map(|row| Ok(Arc::new(row.values.clone())));
                return Ok(Box::new(iter));
            }

            // Fall back to filtered range scan if no index exists
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Get column info from the versioned table's index_columns
            let column_names = if let Some(versioned_table) = storage.tables.get(&table) {
                versioned_table.index_columns.get(&index_name).cloned()
            } else {
                // Fallback: assume index_name is a column name
                if schema.columns.iter().any(|c| c.name == index_name) {
                    Some(vec![index_name.clone()])
                } else {
                    None
                }
            };

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
                        return Ok(Box::new(iter));
                    }
                }

                // Perform range filtering
                let start_vals = start_values.clone();
                let end_vals = end_values.clone();
                let start_incl = start_inclusive;
                let end_incl = end_inclusive;

                let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.filter_map(move |row| {
                    // Extract values for the indexed columns
                    let mut row_values = Vec::new();
                    for &idx in &col_indices {
                        if let Some(val) = row.get(idx) {
                            row_values.push(val.clone());
                        } else {
                            return None; // NULL in key, skip
                        }
                    }

                    // Check start bound
                    if let Some(ref start) = start_vals {
                        match operators::compare_composite(&row_values, start).ok() {
                            Some(std::cmp::Ordering::Less) => return None,
                            Some(std::cmp::Ordering::Equal) if !start_incl => return None,
                            _ => {}
                        }
                    }

                    // Check end bound
                    if let Some(ref end) = end_vals {
                        match operators::compare_composite(&row_values, end).ok() {
                            Some(std::cmp::Ordering::Greater) => return None,
                            Some(std::cmp::Ordering::Equal) if !end_incl => return None,
                            _ => {}
                        }
                    }

                    Some(Ok(row))
                });
                return Ok(Box::new(iter));
            }

            // Can't determine columns, fall back to full scan
            let iter = read_ops::scan_iter(storage, tx_ctx, &table)?.map(Ok);
            Ok(Box::new(iter))
        }

        Node::Filter { source, predicate } => {
            let source_rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Clone transaction context and params for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();

            let filtered = source_rows.filter_map(move |row| match row {
                Ok(row) => {
                    match expression::evaluate_with_arc(
                        &predicate,
                        Some(&row),
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                    ) {
                        Ok(v) if v.to_bool().unwrap_or(false) => Some(Ok(row)),
                        Ok(_) => None,
                        Err(e) => Some(Err(e)),
                    }
                }
                Err(e) => Some(Err(e)),
            });

            Ok(Box::new(filtered))
        }

        Node::Values { rows } => {
            // Values node contains literal rows - convert Expression to Value lazily
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();

            let values_iter = rows.into_iter().map(move |row| {
                let mut value_row = Vec::new();
                for expr in row {
                    value_row.push(expression::evaluate(
                        &expr,
                        None,
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                    )?);
                }
                Ok(Arc::new(value_row))
            });

            Ok(Box::new(values_iter))
        }

        Node::Projection {
            source,
            expressions,
            ..
        } => {
            let source_rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Clone transaction context and params for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();

            let projected = source_rows.map(move |row| match row {
                Ok(row) => {
                    let mut result = Vec::with_capacity(expressions.len());
                    for expr in &expressions {
                        result.push(expression::evaluate_with_arc(
                            expr,
                            Some(&row),
                            &tx_ctx_clone,
                            params_clone.as_ref(),
                        )?);
                    }
                    Ok(Arc::new(result))
                }
                Err(e) => Err(e),
            });

            Ok(Box::new(projected))
        }

        // Limit and Offset are trivial with iterators
        Node::Limit { source, limit } => {
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
            Ok(Box::new(rows.take(limit)))
        }

        Node::Offset { source, offset } => {
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
            Ok(Box::new(rows.skip(offset)))
        }

        // Aggregation works with immutable storage
        Node::Aggregate {
            source,
            group_by,
            aggregates,
        } => {
            // Use the Aggregator module
            let mut aggregator = Aggregator::new(group_by, aggregates);

            // Process all source rows
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;
            for row in rows {
                let row = row?;
                // Use the transaction context for the aggregator
                aggregator.add(&row, tx_ctx, storage)?;
            }

            // Get aggregated results
            let results = aggregator.finalize()?;
            Ok(Box::new(results.into_iter().map(Ok)))
        }

        // Hash Join can work with immutable storage since both sides can be read
        Node::HashJoin {
            left,
            right,
            left_col,
            right_col,
            join_type,
        } => {
            // Get column counts from both nodes for outer join NULL padding
            let schemas = storage.get_schemas();
            let left_columns = left.column_count(&schemas);
            let right_columns = right.column_count(&schemas);

            // Both can borrow storage immutably!
            let left_rows = execute_node_read(*left, storage, tx_ctx, params)?;
            let right_rows = execute_node_read(*right, storage, tx_ctx, params)?;

            join::execute_hash_join(
                left_rows,
                left_col,
                right_rows,
                right_col,
                left_columns,
                right_columns,
                join_type,
            )
        }

        // Nested Loop Join also works with immutable storage
        Node::NestedLoopJoin {
            left,
            right,
            predicate,
            join_type,
        } => {
            // Get column counts from both nodes for outer join NULL padding
            let schemas = storage.get_schemas();
            let left_columns = left.column_count(&schemas);
            let right_columns = right.column_count(&schemas);

            let left_rows = execute_node_read(*left, storage, tx_ctx, params)?;
            let right_rows = execute_node_read(*right, storage, tx_ctx, params)?;

            // Use the standalone function for nested loop join
            join::execute_nested_loop_join(
                left_rows,
                right_rows,
                left_columns,
                right_columns,
                predicate,
                join_type,
                storage,
            )
        }

        // Order By requires full materialization to sort
        Node::Order { source, order_by } => {
            let rows = execute_node_read(*source, storage, tx_ctx, params)?;

            // Must materialize to sort
            let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

            // Sort based on order_by expressions
            collected.sort_by(|a, b| {
                for (expr, direction) in &order_by {
                    let val_a = expression::evaluate_with_arc(expr, Some(a), tx_ctx, params)
                        .unwrap_or(Value::Null);
                    let val_b = expression::evaluate_with_arc(expr, Some(b), tx_ctx, params)
                        .unwrap_or(Value::Null);

                    let cmp = val_a
                        .partial_cmp(&val_b)
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if cmp != std::cmp::Ordering::Equal {
                        return match direction {
                            crate::planning::plan::Direction::Ascending => cmp,
                            crate::planning::plan::Direction::Descending => cmp.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });

            Ok(Box::new(collected.into_iter().map(Ok)))
        }

        Node::Nothing => Ok(Box::new(std::iter::empty())),
    }
}
