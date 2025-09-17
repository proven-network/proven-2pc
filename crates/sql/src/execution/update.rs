//! UPDATE statement execution
//!
//! This module handles the execution of UPDATE statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression};
use crate::planning::plan::Node;
use crate::semantic::BoundParameters;
use crate::storage::{MvccStorage, read_ops, write_ops};
use crate::stream::TransactionContext;
use crate::types::expression::Expression;

/// Execute UPDATE with phased read-then-write approach
pub fn execute_update(
    table: String,
    assignments: Vec<(usize, Expression)>,
    source: Node,
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    params: Option<&BoundParameters>,
) -> Result<ExecutionResult> {
    // Phase 1: Read rows with IDs that match the WHERE clause
    let rows_to_update = {
        let iter = read_ops::scan_iter_with_ids(storage, tx_ctx, &table)?;
        let mut to_update = Vec::new();

        for (row_id, row) in iter {
            let matches = match &source {
                Node::Filter { predicate, .. } => {
                    expression::evaluate_with_arc(predicate, Some(&row), tx_ctx, params)?
                        .to_bool()
                        .unwrap_or(false)
                }
                Node::Scan { .. } => true,
                _ => true,
            };

            if matches {
                to_update.push((row_id, row));
            }
        }
        to_update
    }; // Immutable borrow ends here

    // Phase 2: Apply updates (mutable borrow)
    // Get table schema for type coercion
    let schema = storage
        .tables
        .get(&table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?
        .schema
        .clone();

    let mut count = 0;
    for (row_id, current) in rows_to_update {
        let mut updated = current.to_vec();
        for &(col_idx, ref expr) in &assignments {
            updated[col_idx] = expression::evaluate_with_arc(expr, Some(&current), tx_ctx, params)?;
        }

        // Apply type coercion to match schema
        let coerced_row = crate::coercion::coerce_row(updated, &schema)?;

        write_ops::update(storage, tx_ctx, &table, row_id, coerced_row)?;
        count += 1;
    }

    Ok(ExecutionResult::Modified(count))
}
