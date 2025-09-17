//! DELETE statement execution
//!
//! This module handles the execution of DELETE statements with proper
//! MVCC transaction support.

use crate::error::Result;
use crate::execution::{ExecutionResult, expression};
use crate::planning::plan::Node;
use crate::semantic::BoundParameters;
use crate::storage::{MvccStorage, read_ops, write_ops};
use crate::stream::TransactionContext;

/// Execute DELETE with phased read-then-write approach
pub fn execute_delete(
    table: String,
    source: Node,
    storage: &mut MvccStorage,
    tx_ctx: &mut TransactionContext,
    params: Option<&BoundParameters>,
) -> Result<ExecutionResult> {
    // Phase 1: Read rows with IDs that match the WHERE clause
    let rows_to_delete = {
        let iter = read_ops::scan_iter_with_ids(storage, tx_ctx, &table)?;
        let mut to_delete = Vec::new();

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
                to_delete.push(row_id);
            }
        }
        to_delete
    }; // Immutable borrow ends here

    // Phase 2: Delete rows (mutable borrow)
    let mut count = 0;
    for row_id in rows_to_delete {
        write_ops::delete(storage, tx_ctx, &table, row_id)?;
        count += 1;
    }

    Ok(ExecutionResult::Modified(count))
}
