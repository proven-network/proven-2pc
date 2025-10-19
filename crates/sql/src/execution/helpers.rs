//! Helper functions for executors using storage
//!
//! These functions encapsulate common patterns needed by INSERT, UPDATE, DELETE executors
//! when working with the lower-level storage API.

use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::index::{IndexColumn, IndexMetadata};
use crate::types::schema::Table as TableSchema;

/// Extract index values from a row based on index metadata
///
/// Given a row and an index, extract the values for the indexed columns
/// in the correct order. For expression indexes, evaluates the expression on the row.
pub fn extract_index_values(
    row: &[Value],
    index: &IndexMetadata,
    schema: &TableSchema,
    context: &ExecutionContext,
) -> Result<Vec<Value>> {
    let mut index_values = Vec::with_capacity(index.columns.len());

    for index_column in &index.columns {
        let value = match index_column {
            IndexColumn::Column(column_name) => {
                // Find column index in schema
                let (col_idx, _) = schema
                    .get_column(column_name)
                    .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?;

                if col_idx >= row.len() {
                    return Err(Error::Other(format!(
                        "Row has {} columns, but index references column at index {}",
                        row.len(),
                        col_idx
                    )));
                }

                row[col_idx].clone()
            }
            IndexColumn::Expression { expression, .. } => {
                // Evaluate the expression on the row
                use crate::execution::expression::evaluate;

                let expr = expression.as_ref().ok_or_else(|| {
                    Error::Other("Expression index missing expression".to_string())
                })?;
                evaluate(expr, Some(&row.to_vec()), context, None).map_err(|e| {
                    Error::Other(format!("Failed to evaluate expression index: {}", e))
                })?
            }
        };

        index_values.push(value);
    }

    Ok(index_values)
}
