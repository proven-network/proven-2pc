//! SELECT query execution
//!
//! This module handles the execution of SELECT statements, coordinating
//! with the node executor to process the query plan.

use crate::error::Result;
use crate::execution::ExecutionResult;
use crate::planning::plan::Node;
use crate::storage::Storage;
use crate::types::context::ExecutionContext;
use crate::types::value::Value;

/// Execute a SELECT query using immutable storage reference
pub fn execute_select(
    node: Node,
    storage: &mut Storage,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
    column_names_override: Option<Vec<String>>,
) -> Result<ExecutionResult> {
    // Get schemas from storage
    let schemas = storage.get_schemas();

    // Get column names - use override from semantic analysis if available and non-empty,
    // otherwise reconstruct from node tree (handles aggregates, expressions, etc.)
    let columns = column_names_override
        .filter(|cols| !cols.is_empty())
        .unwrap_or_else(|| node.get_column_names(&schemas));

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
