//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, expression, helpers, join};
use crate::error::{Error, Result};
use crate::operators;
use crate::planning::plan::{Node, Plan};
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::query::Rows;
use crate::types::{Value, ValueExt};
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
    Ddl(String),
}

/// Execute a query plan with parameters and batch
pub fn execute_with_params(
    plan: Plan,
    storage: &mut SqlStorage,
    batch: &mut fjall::Batch,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<ExecutionResult> {
    match plan {
        Plan::Query {
            root,
            params: _,
            column_names,
        } => {
            // Query uses immutable storage reference (handles SELECT and VALUES)
            super::select::execute_select(*root, storage, tx_ctx, params, column_names.clone())
        }

        Plan::Insert {
            table,
            columns,
            source,
        } => {
            // INSERT uses write execution with phased approach
            super::insert::execute_insert(table, columns, *source, storage, batch, tx_ctx, params)
        }

        Plan::Update {
            table,
            assignments,
            source,
        } => {
            // UPDATE uses write execution with phased approach
            super::update::execute_update(
                table,
                assignments,
                *source,
                storage,
                batch,
                tx_ctx,
                params,
            )
        }

        Plan::Delete { table, source } => {
            // DELETE uses write execution with phased approach
            super::delete::execute_delete(table, *source, storage, batch, tx_ctx, params)
        }

        // DDL operations - execute directly (no execute_ddl wrapper!)
        Plan::CreateTable {
            name,
            schema,
            if_not_exists,
            ..
        } => {
            // Check if table exists
            if storage.get_schemas().contains_key(&name) {
                if if_not_exists {
                    return Ok(ExecutionResult::Ddl(format!(
                        "Table {} already exists (skipped)",
                        name
                    )));
                } else {
                    return Err(Error::DuplicateTable(name));
                }
            }

            storage.create_table(batch, name.clone(), schema)?;
            Ok(ExecutionResult::Ddl(format!("Created table {}", name)))
        }

        Plan::DropTable {
            names,
            if_exists,
            cascade: _,
        } => {
            // For now, only handle single table (names should have length 1)
            let name = &names[0];

            if !storage.get_schemas().contains_key(name) {
                if if_exists {
                    return Ok(ExecutionResult::Ddl(format!(
                        "Table {} does not exist (skipped)",
                        name
                    )));
                } else {
                    return Err(Error::TableNotFound(name.clone()));
                }
            }

            storage.drop_table(batch, name)?;
            Ok(ExecutionResult::Ddl(format!("Dropped table {}", name)))
        }

        Plan::CreateIndex {
            name,
            table,
            columns,
            unique,
            included_columns: _,
        } => {
            // Check if index exists (for now, ignore if_not_exists - always create)
            if storage.get_index_metadata().contains_key(&name) {
                return Err(Error::Other(format!("Index {} already exists", name)));
            }

            // Create the index structure (extract column names from IndexColumn)
            let column_names: Vec<String> = columns
                .iter()
                .filter_map(|col| {
                    // Extract column name from expression (assuming it's a simple column reference)
                    if let crate::parsing::ast::Expression::Column(_, col_name) = &col.expression {
                        Some(col_name.clone())
                    } else {
                        None
                    }
                })
                .collect();
            storage.create_index(name.clone(), table.clone(), column_names.clone(), unique)?;

            // Backfill index with existing data
            let schema = storage
                .get_schemas()
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?
                .clone();

            // Collect all rows first to avoid borrow conflicts
            let rows: Vec<_> = storage
                .scan_table(&table, tx_ctx.txn_id)?
                .collect::<Result<Vec<_>>>()?;

            let mut batch = storage.batch();
            for (row_id, values) in rows {
                let index_values = helpers::extract_index_values(
                    &values,
                    &crate::types::index::IndexMetadata {
                        name: name.clone(),
                        table: table.clone(),
                        columns: column_names.clone(),
                        unique,
                        index_type: crate::types::index::IndexType::BTree,
                    },
                    &schema,
                )?;
                storage.insert_index_entry(
                    &mut batch,
                    &name,
                    index_values,
                    row_id,
                    tx_ctx.txn_id,
                    tx_ctx.log_index,
                )?;
            }
            batch.commit()?;

            Ok(ExecutionResult::Ddl(format!("Created index {}", name)))
        }

        Plan::DropIndex { name, if_exists } => {
            if !storage.get_index_metadata().contains_key(&name) {
                if if_exists {
                    return Ok(ExecutionResult::Ddl(format!(
                        "Index {} does not exist (skipped)",
                        name
                    )));
                } else {
                    return Err(Error::IndexNotFound(name));
                }
            }

            storage.drop_index(&name)?;
            Ok(ExecutionResult::Ddl(format!("Dropped index {}", name)))
        }

        Plan::CreateTableAsValues {
            name,
            schema,
            values_plan,
            if_not_exists,
        } => {
            // First create the table
            if storage.get_schemas().contains_key(&name) {
                if if_not_exists {
                    return Ok(ExecutionResult::Ddl(format!(
                        "Table {} already exists (skipped)",
                        name
                    )));
                } else {
                    return Err(Error::DuplicateTable(name));
                }
            }

            storage.create_table(batch, name.clone(), schema)?;

            // Then execute the VALUES insertion
            if let Plan::Query {
                root,
                params: _,
                column_names: _,
            } = values_plan.as_ref()
            {
                let insert_result = super::insert::execute_insert(
                    name.clone(),
                    None,
                    *root.clone(),
                    storage,
                    batch,
                    tx_ctx,
                    params,
                )?;

                // Return success message with row count
                if let ExecutionResult::Modified(count) = insert_result {
                    Ok(ExecutionResult::Ddl(format!(
                        "Created table {} with {} rows inserted",
                        name, count
                    )))
                } else {
                    Ok(ExecutionResult::Ddl(format!("Created table {}", name)))
                }
            } else {
                Ok(ExecutionResult::Ddl(format!("Created table {}", name)))
            }
        }

        Plan::CreateTableAsSelect {
            name,
            schema,
            select_plan,
            if_not_exists,
        } => {
            // First create the table
            if storage.get_schemas().contains_key(&name) {
                if if_not_exists {
                    return Ok(ExecutionResult::Ddl(format!(
                        "Table {} already exists (skipped)",
                        name
                    )));
                } else {
                    return Err(Error::DuplicateTable(name));
                }
            }

            storage.create_table(batch, name.clone(), schema)?;

            // Then execute the SELECT insertion
            if let Plan::Query {
                root,
                params: _,
                column_names: _,
            } = select_plan.as_ref()
            {
                let insert_result = super::insert::execute_insert(
                    name.clone(),
                    None,
                    *root.clone(),
                    storage,
                    batch,
                    tx_ctx,
                    params,
                )?;

                // Return success message with row count
                if let ExecutionResult::Modified(count) = insert_result {
                    Ok(ExecutionResult::Ddl(format!(
                        "Created table {} with {} rows inserted",
                        name, count
                    )))
                } else {
                    Ok(ExecutionResult::Ddl(format!("Created table {}", name)))
                }
            } else {
                Ok(ExecutionResult::Ddl(format!("Created table {}", name)))
            }
        }

        Plan::AlterTable { name, operation } => {
            use crate::parsing::ast::ddl::AlterTableOperation;

            match operation {
                AlterTableOperation::AddColumn { column } => {
                    // Validate column doesn't already exist
                    let schemas = storage.get_schemas();
                    let table_schema = schemas
                        .get(&name)
                        .ok_or_else(|| Error::TableNotFound(name.clone()))?;

                    if table_schema.columns.iter().any(|c| c.name == column.name) {
                        return Err(Error::AlreadyExistingColumn(column.name.clone()));
                    }

                    // Validate unique constraint on float
                    if column.unique
                        && matches!(
                            column.data_type,
                            crate::types::data_type::DataType::F32
                                | crate::types::data_type::DataType::F64
                        )
                    {
                        return Err(Error::UnsupportedDataTypeForUniqueColumn(
                            column.data_type.to_string(),
                        ));
                    }

                    // Convert AST column to schema column
                    let mut schema_col = crate::types::schema::Column::new(
                        column.name.clone(),
                        column.data_type.clone(),
                    );

                    if column.primary_key {
                        return Err(Error::InvalidOperation(
                            "Cannot add PRIMARY KEY column via ALTER TABLE".into(),
                        ));
                    }

                    if let Some(nullable) = column.nullable {
                        schema_col = schema_col.nullable(nullable);
                    }

                    if column.unique {
                        schema_col = schema_col.unique();
                    }

                    if column.index {
                        schema_col = schema_col.with_index(true);
                    }

                    // Evaluate default expression
                    let default_value = if let Some(ref default_expr) = column.default {
                        // For ALTER TABLE, evaluate default immediately as a constant literal
                        use crate::parsing::ast::Expression;
                        use crate::types::data_type::DataType;
                        match default_expr {
                            Expression::Literal(lit) => {
                                use crate::parsing::ast::Literal;
                                match lit {
                                    Literal::Integer(i) => {
                                        // Convert based on column data type
                                        match &column.data_type {
                                            DataType::I8 => Value::I8(*i as i8),
                                            DataType::I16 => Value::I16(*i as i16),
                                            DataType::I32 => Value::I32(*i as i32),
                                            DataType::I64 => Value::I64(*i as i64),
                                            DataType::I128 => Value::I128(*i),
                                            DataType::U8 => Value::U8(*i as u8),
                                            DataType::U16 => Value::U16(*i as u16),
                                            DataType::U32 => Value::U32(*i as u32),
                                            DataType::U64 => Value::U64(*i as u64),
                                            DataType::U128 => Value::U128(*i as u128),
                                            _ => Value::I64(*i as i64), // Default fallback
                                        }
                                    }
                                    Literal::Float(f) => match &column.data_type {
                                        DataType::F32 => Value::F32(*f as f32),
                                        DataType::F64 => Value::F64(*f),
                                        _ => Value::F64(*f),
                                    },
                                    Literal::String(s) => Value::Str(s.clone()),
                                    Literal::Boolean(b) => Value::Bool(*b),
                                    Literal::Null => Value::Null,
                                    _ => {
                                        return Err(Error::InvalidOperation(
                                            "Unsupported DEFAULT value for ALTER TABLE ADD COLUMN"
                                                .into(),
                                        ));
                                    }
                                }
                            }
                            _ => return Err(Error::InvalidOperation(
                                "Expression type not supported for ALTER TABLE ADD COLUMN DEFAULT"
                                    .into(),
                            )),
                        }
                    } else if matches!(column.nullable, Some(false)) {
                        // NOT NULL without default
                        return Err(Error::DefaultValueRequired(column.name.clone()));
                    } else {
                        // NULL column without default gets NULL
                        Value::Null
                    };

                    // Execute ADD COLUMN
                    storage.alter_table_add_column(
                        batch,
                        &name,
                        schema_col,
                        default_value,
                        tx_ctx.txn_id,
                        tx_ctx.log_index,
                    )?;

                    Ok(ExecutionResult::Ddl(format!(
                        "Added column {} to table {}",
                        column.name, name
                    )))
                }

                AlterTableOperation::DropColumn {
                    column_name,
                    if_exists,
                } => {
                    storage.alter_table_drop_column(
                        batch,
                        &name,
                        &column_name,
                        if_exists,
                        tx_ctx.txn_id,
                        tx_ctx.log_index,
                    )?;

                    Ok(ExecutionResult::Ddl(format!(
                        "Dropped column {} from table {}",
                        column_name, name
                    )))
                }

                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    // Validate new column name doesn't exist
                    let schemas = storage.get_schemas();
                    let table_schema = schemas
                        .get(&name)
                        .ok_or_else(|| Error::TableNotFound(name.clone()))?;

                    if table_schema
                        .columns
                        .iter()
                        .any(|c| c.name == *new_column_name)
                    {
                        return Err(Error::AlreadyExistingColumn(new_column_name.clone()));
                    }

                    storage.alter_table_rename_column(
                        batch,
                        &name,
                        &old_column_name,
                        &new_column_name,
                    )?;

                    Ok(ExecutionResult::Ddl(format!(
                        "Renamed column {} to {} in table {}",
                        old_column_name, new_column_name, name
                    )))
                }

                AlterTableOperation::RenameTable { new_table_name } => {
                    storage.alter_table_rename_table(
                        batch,
                        &name,
                        &new_table_name,
                        tx_ctx.txn_id,
                        tx_ctx.log_index,
                    )?;

                    Ok(ExecutionResult::Ddl(format!(
                        "Renamed table {} to {}",
                        name, new_table_name
                    )))
                }
            }
        }
    }
}

/// Execute a plan node for reading with immutable storage reference
pub fn execute_node_read<'a>(
    node: Node,
    storage: &'a SqlStorage,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
) -> Result<Rows<'a>> {
    execute_node_read_with_outer(node, storage, tx_ctx, params, None)
}

/// Execute a plan node for reading with optional outer row (for correlated subqueries)
pub fn execute_node_read_with_outer<'a>(
    node: Node,
    storage: &'a SqlStorage,
    tx_ctx: &mut ExecutionContext,
    params: Option<&Vec<Value>>,
    outer_row: Option<&'a Arc<Vec<Value>>>,
) -> Result<Rows<'a>> {
    match node {
        Node::Scan { table, .. } => {
            // True streaming with immutable storage!
            let iter = storage
                .scan_table(&table, tx_ctx.txn_id)?
                .map(|result| result.map(|(_, values)| Arc::new(values)));

            Ok(Box::new(iter))
        }

        Node::SeriesScan { size, .. } => {
            // Evaluate the size expression
            let size_value =
                expression::evaluate_with_storage(&size, None, tx_ctx, params, Some(storage))?;

            // Convert size to i64
            let n = match size_value {
                Value::I32(v) => v as i64,
                Value::I64(v) => v,
                Value::I128(v) => v as i64,
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "SERIES size must be an integer, got {:?}",
                        size_value
                    )));
                }
            };

            // Validate size is non-negative
            if n < 0 {
                return Err(Error::ExecutionError(format!(
                    "SERIES size must be non-negative, got {}",
                    n
                )));
            }

            // Generate N rows with column "N" containing values 1..=N
            let rows: Vec<Arc<Vec<Value>>> =
                (1..=n).map(|i| Arc::new(vec![Value::I64(i)])).collect();

            Ok(Box::new(rows.into_iter().map(Ok)))
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
                filter_values.push(expression::evaluate_with_storage(
                    value_expr,
                    None,
                    tx_ctx,
                    params,
                    Some(storage),
                )?);
            }

            // Try to use index lookup first
            if storage.get_index_metadata().contains_key(&index_name) {
                // Type-coerce filter values based on column schema
                let schemas = storage.get_schemas();
                let mut coerced_values = filter_values.clone();
                if let Some(schema) = schemas.get(&table)
                    && let Some(column) = schema.columns.iter().find(|c| c.name == *index_name)
                {
                    for (i, value) in filter_values.iter().enumerate() {
                        if i < coerced_values.len() {
                            coerced_values[i] =
                                crate::coercion::coerce_value(value.clone(), &column.data_type)?;
                        }
                    }
                }

                // Use streaming index lookup - already returns complete rows!
                let rows = storage.index_lookup(&index_name, coerced_values, tx_ctx.txn_id)?;

                // Convert (row_id, values) to Arc<Vec<Value>>
                let iter = rows.map(|result| result.map(|(_, values)| Arc::new(values)));
                return Ok(Box::new(iter));
            }

            // Fall back to filtered table scan if no index exists
            // We need to check if we can resolve the index_name to columns
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Check if we can get column info from the index metadata
            let column_names = storage
                .get_index_metadata()
                .get(&index_name)
                .map(|meta| meta.columns.clone())
                .or_else(|| {
                    // Fallback: assume index_name is a column name for backward compatibility
                    if schema.columns.iter().any(|c| c.name == index_name) {
                        Some(vec![index_name.clone()])
                    } else {
                        None
                    }
                });

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = storage
                            .scan_table(&table, tx_ctx.txn_id)?
                            .map(|result| result.map(|(_, values)| Arc::new(values)));
                        return Ok(Box::new(iter));
                    }
                }

                // Filter by checking all column values match
                if col_indices.len() == filter_values.len() {
                    let iter =
                        storage
                            .scan_table(&table, tx_ctx.txn_id)?
                            .filter_map(move |result| {
                                match result {
                                    Ok((_, row_values)) => {
                                        let values: Arc<Vec<Value>> = Arc::new(row_values);
                                        // Check if all indexed columns match the filter values
                                        for (idx, expected_val) in
                                            col_indices.iter().zip(&filter_values)
                                        {
                                            if values.get(*idx) != Some(expected_val) {
                                                return None;
                                            }
                                        }
                                        Some(Ok(values))
                                    }
                                    Err(e) => Some(Err(e)),
                                }
                            });
                    return Ok(Box::new(iter));
                }
            }

            // Can't determine columns, fall back to full scan
            let iter = storage
                .scan_table(&table, tx_ctx.txn_id)?
                .map(|result| result.map(|(_, values)| Arc::new(values)));
            Ok(Box::new(iter))
        }

        Node::IndexRangeScan {
            table,
            index_name,
            start,
            start_inclusive,
            end,
            end_inclusive,
            ..
        } => {
            // Evaluate range bounds
            let start_values = start
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| {
                            expression::evaluate_with_storage(
                                e,
                                None,
                                tx_ctx,
                                params,
                                Some(storage),
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let end_values = end
                .as_ref()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|e| {
                            expression::evaluate_with_storage(
                                e,
                                None,
                                tx_ctx,
                                params,
                                Some(storage),
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;

            // Try to use index range scan
            if storage.get_index_metadata().contains_key(&index_name) {
                // Use streaming index range scan
                let rows = storage.index_range_scan(
                    &index_name,
                    start_values,
                    start_inclusive,
                    end_values,
                    end_inclusive,
                    tx_ctx.txn_id,
                )?;

                // Convert to iterator format (already returns rows!)
                let iter = rows.map(|result| result.map(|(_, values)| Arc::new(values)));
                // TODO: Handle reverse ordering
                return Ok(Box::new(iter));
            }

            // Fall back to filtered range scan if no index exists
            let schemas = storage.get_schemas();
            let schema = schemas
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;

            // Get column info from the index metadata
            let column_names = storage
                .get_index_metadata()
                .get(&index_name)
                .map(|meta| meta.columns.clone())
                .or_else(|| {
                    // Fallback: assume index_name is a column name
                    if schema.columns.iter().any(|c| c.name == index_name) {
                        Some(vec![index_name.clone()])
                    } else {
                        None
                    }
                });

            if let Some(columns) = column_names {
                // Get column indices
                let mut col_indices = Vec::new();
                for col_name in &columns {
                    if let Some(idx) = schema.columns.iter().position(|c| &c.name == col_name) {
                        col_indices.push(idx);
                    } else {
                        // Column not found, can't filter
                        let iter = storage
                            .scan_table(&table, tx_ctx.txn_id)?
                            .map(|result| result.map(|(_, values)| Arc::new(values)));
                        return Ok(Box::new(iter));
                    }
                }

                // Perform range filtering
                let start_vals = start_values.clone();
                let end_vals = end_values.clone();
                let start_incl = start_inclusive;
                let end_incl = end_inclusive;

                let iter = storage
                    .scan_table(&table, tx_ctx.txn_id)?
                    .filter_map(move |result| {
                        match result {
                            Ok((_, row_values_vec)) => {
                                let values: Arc<Vec<Value>> = Arc::new(row_values_vec);
                                // Extract values for the indexed columns
                                let mut row_values = Vec::new();
                                for &idx in &col_indices {
                                    if let Some(val) = values.get(idx) {
                                        row_values.push(val.clone());
                                    } else {
                                        return None; // NULL in key, skip
                                    }
                                }

                                // Check start bound
                                if let Some(ref start) = start_vals {
                                    match operators::compare_composite(&row_values, start).ok() {
                                        Some(std::cmp::Ordering::Less) => return None,
                                        Some(std::cmp::Ordering::Equal) if !start_incl => {
                                            return None;
                                        }
                                        _ => {}
                                    }
                                }

                                // Check end bound
                                if let Some(ref end) = end_vals {
                                    match operators::compare_composite(&row_values, end).ok() {
                                        Some(std::cmp::Ordering::Greater) => return None,
                                        Some(std::cmp::Ordering::Equal) if !end_incl => {
                                            return None;
                                        }
                                        _ => {}
                                    }
                                }

                                Some(Ok(values))
                            }
                            Err(e) => Some(Err(e)),
                        }
                    });
                return Ok(Box::new(iter));
            }

            // Can't determine columns, fall back to full scan
            let iter = storage
                .scan_table(&table, tx_ctx.txn_id)?
                .map(|result| result.map(|(_, values)| Arc::new(values)));
            Ok(Box::new(iter))
        }

        Node::Filter { source, predicate } => {
            let source_rows =
                execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;

            // Clone transaction context, params, and outer_row for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();
            let outer_row_clone = outer_row.cloned();

            let filtered = source_rows.filter_map(move |row| match row {
                Ok(row) => {
                    match expression::evaluate_with_arc_storage_and_outer(
                        &predicate,
                        Some(&row),
                        outer_row_clone.as_ref(),
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                        Some(storage),
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
                    value_row.push(expression::evaluate_with_storage(
                        &expr,
                        None,
                        &tx_ctx_clone,
                        params_clone.as_ref(),
                        Some(storage),
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
            let source_rows =
                execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;

            // Clone transaction context and params for use in closure
            let tx_ctx_clone = tx_ctx.clone();
            let params_clone = params.cloned();

            let projected = source_rows.map(move |row| match row {
                Ok(row) => {
                    let mut result = Vec::with_capacity(expressions.len());
                    for expr in &expressions {
                        result.push(expression::evaluate_with_arc_and_storage(
                            expr,
                            Some(&row),
                            &tx_ctx_clone,
                            params_clone.as_ref(),
                            Some(storage),
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
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;
            Ok(Box::new(rows.take(limit)))
        }

        Node::Offset { source, offset } => {
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;
            Ok(Box::new(rows.skip(offset)))
        }

        Node::Distinct { source } => {
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;

            // Collect all rows and deduplicate
            let mut seen = std::collections::HashSet::new();
            let mut distinct_rows = Vec::new();

            for row_result in rows {
                let row = row_result?;
                // Use HashableValue wrapper for proper hashing/equality
                let hash_key =
                    crate::execution::aggregator::HashableValue(Value::List(row.as_ref().clone()));
                if seen.insert(hash_key) {
                    distinct_rows.push(row);
                }
            }

            Ok(Box::new(distinct_rows.into_iter().map(Ok)))
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
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;
            for row in rows {
                let row = row?;
                // Use the transaction context for the aggregator
                aggregator.add(&row, tx_ctx)?;
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
            let schemas_arc = storage.get_schemas();
            let schemas: HashMap<String, crate::types::schema::Table> = schemas_arc
                .iter()
                .map(|(k, v)| (k.clone(), v.as_ref().clone()))
                .collect();
            let left_columns = left.column_count(&schemas);
            let right_columns = right.column_count(&schemas);

            // IMPORTANT: Do NOT pass outer_row to the join sources!
            // Inline views in FROM clauses are independent and should not see outer context.
            let left_rows = execute_node_read_with_outer(*left, storage, tx_ctx, params, None)?;
            let right_rows = execute_node_read_with_outer(*right, storage, tx_ctx, params, None)?;

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
            let schemas_arc = storage.get_schemas();
            let schemas: HashMap<String, crate::types::schema::Table> = schemas_arc
                .iter()
                .map(|(k, v)| (k.clone(), v.as_ref().clone()))
                .collect();
            let left_columns = left.column_count(&schemas);
            let right_columns = right.column_count(&schemas);

            // IMPORTANT: Do NOT pass outer_row to the join sources!
            // Inline views in FROM clauses are independent and should not see outer context.
            // Only pass outer_row for correlated subqueries in WHERE/SELECT clauses.
            let left_rows = execute_node_read_with_outer(*left, storage, tx_ctx, params, None)?;
            let right_rows = execute_node_read_with_outer(*right, storage, tx_ctx, params, None)?;

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
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;

            // Must materialize to sort
            let mut collected: Vec<_> = rows.collect::<Result<Vec<_>>>()?;

            // Sort based on order_by expressions
            collected.sort_by(|a, b| {
                for (expr, direction) in &order_by {
                    let val_a = expression::evaluate_with_arc_and_storage(
                        expr,
                        Some(a),
                        tx_ctx,
                        params,
                        Some(storage),
                    )
                    .unwrap_or(Value::Null);
                    let val_b = expression::evaluate_with_arc_and_storage(
                        expr,
                        Some(b),
                        tx_ctx,
                        params,
                        Some(storage),
                    )
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
