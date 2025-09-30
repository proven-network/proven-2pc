//! SELECT query execution
//!
//! This module handles the execution of SELECT statements, coordinating
//! with the node executor to process the query plan.

use crate::error::Result;
use crate::execution::ExecutionResult;
use crate::planning::plan::Node;
use crate::storage::Storage;
use crate::stream::TransactionContext;
use crate::types::value::Value;

/// Execute a SELECT query using immutable storage reference
pub fn execute_select(
    node: Node,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Get schemas from storage
    let schemas = storage.get_schemas();

    // Get column names from the node tree
    let columns = node.get_column_names(&schemas);

    // Execute using read-only node execution
    let rows = super::executor::execute_node_read(node, storage, tx_ctx, params)?;
    let mut collected = Vec::new();
    for row in rows {
        collected.push(row?);
    }

    Ok(ExecutionResult::Select {
        columns,
        rows: collected,
    })
}
