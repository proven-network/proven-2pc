//! Helper functions for executors using storage
//!
//! These functions encapsulate common patterns needed by INSERT, UPDATE, DELETE executors
//! when working with the lower-level storage API.

use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::index::IndexMetadata;
use crate::types::schema::Table as TableSchema;

/// Extract index values from a row based on index metadata
///
/// Given a row and an index, extract the values for the indexed columns
/// in the correct order.
pub fn extract_index_values(
    row: &[Value],
    index: &IndexMetadata,
    schema: &TableSchema,
) -> Result<Vec<Value>> {
    let mut index_values = Vec::with_capacity(index.columns.len());

    for column_name in &index.columns {
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

        index_values.push(row[col_idx].clone());
    }

    Ok(index_values)
}
