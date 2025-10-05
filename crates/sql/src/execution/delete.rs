//! DELETE statement execution
//!
//! This module handles the execution of DELETE statements with proper
//! MVCC transaction support.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression, helpers};
use crate::planning::plan::Node;
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::value::Value;

/// Execute DELETE with phased read-then-write approach
pub fn execute_delete(
    table: String,
    source: Node,
    storage: &mut SqlStorage,
    batch: &mut fjall::Batch,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Get schema
    let schemas = storage.get_schemas();
    let schema = schemas
        .get(&table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?
        .clone();

    // Phase 1: Scan to find matching rows
    let matching_rows: Vec<(u64, Vec<Value>)> = {
        let mut matches = Vec::new();
        for result in storage.scan_table(&table, tx_ctx.txn_id)? {
            let (row_id, values) = result?;
            let row_arc = std::sync::Arc::new(values.clone());

            let is_match = match &source {
                Node::Filter { predicate, .. } => {
                    expression::evaluate(predicate, Some(&row_arc), tx_ctx, params)?
                        .to_bool()
                        .unwrap_or(false)
                }
                Node::Scan { .. } => true,
                _ => true,
            };

            if is_match {
                matches.push((row_id, values));
            }
        }
        matches
    };

    // Phase 2: Write deletes to batch (engine will commit with predicates)
    let all_indexes: Vec<_> = storage
        .get_table_indexes(&table)
        .into_iter()
        .cloned()
        .collect();

    for (row_id, values) in &matching_rows {
        // Delete row
        storage.delete_row(batch, &table, *row_id, tx_ctx.txn_id, tx_ctx.log_index)?;

        // Delete from all indexes
        for index in &all_indexes {
            let index_values = helpers::extract_index_values(values, index, &schema)?;
            storage.delete_index_entry(
                batch,
                &index.name,
                index_values,
                *row_id,
                tx_ctx.txn_id,
                tx_ctx.log_index,
            )?;
        }
    }

    // Engine will add predicates and commit batch
    Ok(ExecutionResult::Modified(matching_rows.len()))
}
