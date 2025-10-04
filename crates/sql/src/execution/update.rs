//! UPDATE statement execution
//!
//! This module handles the execution of UPDATE statements with proper
//! MVCC transaction support and type coercion.

use crate::error::{Error, Result};
use crate::execution::{ExecutionResult, expression};
use crate::parsing::ast::ddl::ReferentialAction;
use crate::planning::plan::Node;
use crate::storage::Storage;
use crate::stream::TransactionContext;
use crate::types::expression::Expression;
use crate::types::schema::Table;
use crate::types::value::{Row, Value};

/// Validate foreign key constraints for an updated row
fn validate_foreign_keys_on_update(
    row: &Row,
    _old_row: &Row,
    schema: &Table,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
) -> Result<Vec<UpdateCascadeOp>> {
    // Check each foreign key constraint on this table
    let schemas = storage.get_schemas();
    for fk in &schema.foreign_keys {
        // Get the referenced table schema
        let ref_schema = schemas.get(&fk.referenced_table).ok_or_else(|| {
            Error::ForeignKeyViolation(format!(
                "Referenced table '{}' does not exist",
                fk.referenced_table
            ))
        })?;

        // Build the foreign key values from the row
        let mut fk_values = Vec::new();
        for col_name in &fk.columns {
            let (col_idx, _) = schema
                .get_column(col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
            fk_values.push(&row[col_idx]);
        }

        // If all foreign key values are NULL, skip the check
        if fk_values.iter().all(|v| v.is_null()) {
            continue;
        }

        // Find the referenced column indices
        let mut ref_indices = Vec::new();
        for ref_col in &fk.referenced_columns {
            let (idx, _) = ref_schema
                .get_column(ref_col)
                .ok_or_else(|| Error::ColumnNotFound(ref_col.clone()))?;
            ref_indices.push(idx);
        }

        // Scan the referenced table to check if the value exists
        let mut found = false;
        let ref_iter = storage.iter(tx_ctx.id, &fk.referenced_table)?;

        for ref_row_result in ref_iter {
            let ref_row = ref_row_result?;
            let ref_values = &ref_row.values;

            // Check if all referenced columns match
            let mut all_match = true;
            for (i, &ref_idx) in ref_indices.iter().enumerate() {
                if &ref_values[ref_idx] != fk_values[i] {
                    all_match = false;
                    break;
                }
            }

            if all_match {
                found = true;
                break;
            }
        }

        if !found {
            return Err(Error::ForeignKeyViolation(format!(
                "Foreign key constraint violation: referenced value {:?} not found in {}.{:?}",
                fk_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>(),
                fk.referenced_table,
                fk.referenced_columns
            )));
        }
    }

    // Check for cascade operations if we're updating a primary key
    let cascade_ops = check_and_collect_update_cascades(row, row, schema, storage, tx_ctx)?;

    Ok(cascade_ops)
}

/// Cascading operation for UPDATE
#[derive(Debug, Clone)]
enum UpdateCascadeOp {
    Update {
        table: String,
        row_id: u64,
        col_idx: usize,
        new_value: Value,
    },
    SetNull {
        table: String,
        row_id: u64,
        col_idx: usize,
    },
    SetDefault {
        table: String,
        row_id: u64,
        col_idx: usize,
        default: Value,
    },
}

/// Helper function to update a single column value
fn update_column_value(
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    table: &str,
    row_id: u64,
    col_idx: usize,
    new_value: Value,
) -> Result<()> {
    // Get the current row
    let current_row = {
        let iter = storage.iter_with_ids(tx_ctx.id, table)?;
        let mut found_row = None;
        for result in iter {
            let (rid, row) = result?;
            if rid == row_id {
                found_row = Some(row.values.clone());
                break;
            }
        }
        found_row.ok_or_else(|| {
            Error::ExecutionError(format!("Row {} not found in table {}", row_id, table))
        })?
    };

    // Update the specific column
    let mut updated_row = current_row;
    updated_row[col_idx] = new_value;

    // Write the updated row
    storage.update(tx_ctx.id, table, row_id, updated_row, tx_ctx.log_index)?;
    Ok(())
}

/// Check if updating this table would violate foreign keys and collect cascade operations
fn check_and_collect_update_cascades(
    old_row: &Row,
    new_row: &Row,
    schema: &Table,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
) -> Result<Vec<UpdateCascadeOp>> {
    let mut cascade_ops = Vec::new();

    // If this table doesn't have a primary key, no other table can reference it
    let pk_idx = match schema.primary_key {
        Some(idx) => idx,
        None => return Ok(cascade_ops),
    };

    // Check if the primary key is being changed
    let pk_changed = old_row[pk_idx] != new_row[pk_idx];
    if !pk_changed {
        // If PK isn't changing, no cascades needed
        return Ok(cascade_ops);
    }

    let old_pk_value = &old_row[pk_idx];
    let new_pk_value = &new_row[pk_idx];

    // Check all tables that might have foreign keys pointing to this table
    let schemas = storage.get_schemas();
    for (other_table_name, other_schema) in schemas.iter() {
        for fk in &other_schema.foreign_keys {
            if fk.referenced_table == schema.name {
                // Find the foreign key column index
                let fk_col_idx = other_schema
                    .get_column(&fk.columns[0])
                    .ok_or_else(|| Error::ColumnNotFound(fk.columns[0].clone()))?
                    .0;

                // Find all rows that reference the old primary key value
                let ref_iter = storage.iter_with_ids(tx_ctx.id, other_table_name)?;
                for result in ref_iter {
                    let (ref_row_id, ref_row) = result?;
                    if &ref_row.values[fk_col_idx] == old_pk_value
                        && !ref_row.values[fk_col_idx].is_null()
                    {
                        // Found a referencing row - apply the referential action
                        match fk.on_update {
                            ReferentialAction::Restrict | ReferentialAction::NoAction => {
                                return Err(Error::ForeignKeyViolation(format!(
                                    "Cannot update primary key in '{}': referenced by foreign key in '{}'",
                                    schema.name, other_table_name
                                )));
                            }
                            ReferentialAction::Cascade => {
                                // CASCADE the update to the foreign key
                                cascade_ops.push(UpdateCascadeOp::Update {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                    col_idx: fk_col_idx,
                                    new_value: new_pk_value.clone(),
                                });
                            }
                            ReferentialAction::SetNull => {
                                // Set the foreign key column to NULL
                                cascade_ops.push(UpdateCascadeOp::SetNull {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                    col_idx: fk_col_idx,
                                });
                            }
                            ReferentialAction::SetDefault => {
                                // Set the foreign key column to its default value
                                let default_expr =
                                    other_schema.columns[fk_col_idx].default.clone().unwrap_or(
                                        crate::types::expression::DefaultExpression::Constant(
                                            Value::Null,
                                        ),
                                    );

                                // Convert to Expression and evaluate
                                let expr: Expression = default_expr.into();
                                let default_value =
                                    crate::execution::expression::evaluate_with_storage(
                                        &expr,
                                        None,
                                        tx_ctx,
                                        None,
                                        Some(storage),
                                    )?;

                                cascade_ops.push(UpdateCascadeOp::SetDefault {
                                    table: other_table_name.clone(),
                                    row_id: ref_row_id,
                                    col_idx: fk_col_idx,
                                    default: default_value,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(cascade_ops)
}

/// Execute UPDATE with phased read-then-write approach
pub fn execute_update(
    table: String,
    assignments: Vec<(usize, Expression)>,
    source: Node,
    storage: &mut Storage,
    tx_ctx: &mut TransactionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    // Phase 1: Read rows with IDs that match the WHERE clause
    let rows_to_update = {
        let iter = storage.iter_with_ids(tx_ctx.id, &table)?;
        let mut to_update = Vec::new();

        for result in iter {
            let (row_id, row) = result?;
            let row_arc = std::sync::Arc::new(row.values.clone());
            let matches = match &source {
                Node::Filter { predicate, .. } => {
                    expression::evaluate_with_arc(predicate, Some(&row_arc), tx_ctx, params)?
                        .to_bool()
                        .unwrap_or(false)
                }
                Node::Scan { .. } => true,
                _ => true,
            };

            if matches {
                to_update.push((row_id, row_arc));
            }
        }
        to_update
    }; // Immutable borrow ends here

    // Phase 2: Apply updates
    // Get table schema for type coercion
    let schemas = storage.get_schemas();
    let schema = schemas
        .get(&table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?
        .clone();

    let mut all_cascade_ops = Vec::new();
    let mut count = 0;

    // Phase 3: Update rows and collect cascade operations
    for (row_id, current) in rows_to_update {
        let mut updated = current.to_vec();
        for &(col_idx, ref expr) in &assignments {
            updated[col_idx] = expression::evaluate_with_arc(expr, Some(&current), tx_ctx, params)?;
        }

        // Apply type coercion to match schema
        let coerced_row = crate::coercion::coerce_row(updated, &schema)?;

        // Validate foreign key constraints and collect cascade operations
        let cascade_ops =
            validate_foreign_keys_on_update(&coerced_row, &current, &schema, storage, tx_ctx)?;
        all_cascade_ops.extend(cascade_ops);

        storage.update(tx_ctx.id, &table, row_id, coerced_row, tx_ctx.log_index)?;
        count += 1;
    }

    // Phase 4: Execute cascade operations
    for op in all_cascade_ops {
        match op {
            UpdateCascadeOp::Update {
                table: cascade_table,
                row_id: cascade_row_id,
                col_idx,
                new_value,
            } => {
                update_column_value(
                    storage,
                    tx_ctx,
                    &cascade_table,
                    cascade_row_id,
                    col_idx,
                    new_value,
                )?;
            }
            UpdateCascadeOp::SetNull {
                table: cascade_table,
                row_id: cascade_row_id,
                col_idx,
            } => {
                update_column_value(
                    storage,
                    tx_ctx,
                    &cascade_table,
                    cascade_row_id,
                    col_idx,
                    Value::Null,
                )?;
            }
            UpdateCascadeOp::SetDefault {
                table: cascade_table,
                row_id: cascade_row_id,
                col_idx,
                default,
            } => {
                update_column_value(
                    storage,
                    tx_ctx,
                    &cascade_table,
                    cascade_row_id,
                    col_idx,
                    default,
                )?;
            }
        }
    }

    Ok(ExecutionResult::Modified(count))
}
