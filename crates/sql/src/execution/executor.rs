//! Complete MVCC-aware SQL executor
//!
//! This executor properly integrates with the planner, executing Plan/Node trees
//! using MVCC transactions for proper isolation and rollback support.

use super::{aggregator::Aggregator, expression, helpers, join};
use crate::error::{Error, Result};
use crate::operators;
use crate::storage::SqlStorage;
use crate::types::context::ExecutionContext;
use crate::types::plan::{Node, Plan};
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

/// Execute a read-only query plan (no batch needed)
pub fn query_with_params(
    plan: Plan,
    storage: &SqlStorage,
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
        _ => Err(crate::error::Error::Internal(
            "query_with_params can only execute Query plans".to_string(),
        )),
    }
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

            // Get schema for column resolution
            let schema = storage
                .get_schemas()
                .get(&table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?
                .clone();

            // Convert AST columns to IndexColumn enum
            let index_columns: Vec<crate::types::index::IndexColumn> = columns
                .iter()
                .map(|col| ast_column_to_index_column(&col.expression, &schema))
                .collect::<Result<Vec<_>>>()?;

            storage.create_index(name.clone(), table.clone(), index_columns.clone(), unique)?;

            // Backfill index with existing data
            // Collect all rows first to avoid borrow conflicts
            let rows: Vec<_> = storage
                .scan_table(&table, tx_ctx.txn_id)?
                .collect::<Result<Vec<_>>>()?;

            // Use the passed-in batch instead of creating a new one
            // This ensures all writes go through the transaction engine's batch mechanism
            for (row_id, values) in rows {
                let index_values = helpers::extract_index_values(
                    &values,
                    &crate::types::index::IndexMetadata {
                        name: name.clone(),
                        table: table.clone(),
                        columns: index_columns.clone(),
                        unique,
                        index_type: crate::types::index::IndexType::BTree,
                    },
                    &schema,
                    tx_ctx,
                )?;
                storage.insert_index_entry(batch, &name, index_values, row_id, tx_ctx.txn_id)?;
            }

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

/// Convert an AST expression to an IndexColumn
fn ast_column_to_index_column(
    expr: &crate::parsing::ast::Expression,
    schema: &crate::types::schema::Table,
) -> Result<crate::types::index::IndexColumn> {
    use crate::parsing::ast::Expression as AstExpr;
    use crate::types::index::IndexColumn;

    match expr {
        // Simple column reference
        AstExpr::Column(_, col_name) => Ok(IndexColumn::Column(col_name.clone())),

        // Expression - convert to runtime expression and extract dependencies
        _ => {
            // Convert AST expression to runtime expression
            let runtime_expr = convert_ast_to_runtime_expr(expr, schema)?;

            // Normalize the expression
            let normalized = crate::semantic::normalize::normalize_expression(&runtime_expr);

            // Extract column dependencies
            let dependencies = extract_column_dependencies(expr);

            Ok(IndexColumn::new_expression(normalized, dependencies))
        }
    }
}

/// Convert an AST expression to a runtime expression for index creation
fn convert_ast_to_runtime_expr(
    expr: &crate::parsing::ast::Expression,
    schema: &crate::types::schema::Table,
) -> Result<crate::types::expression::Expression> {
    use crate::parsing::ast::{Expression as AstExpr, Literal, Operator};
    use crate::types::expression::Expression as RuntimeExpr;

    match expr {
        AstExpr::Literal(lit) => {
            let value = match lit {
                Literal::Null => Value::Null,
                Literal::Boolean(b) => Value::Bool(*b),
                Literal::Integer(i) => {
                    if *i >= i32::MIN as i128 && *i <= i32::MAX as i128 {
                        Value::I32(*i as i32)
                    } else if *i >= i64::MIN as i128 && *i <= i64::MAX as i128 {
                        Value::I64(*i as i64)
                    } else {
                        Value::I128(*i)
                    }
                }
                Literal::Float(f) => Value::F64(*f),
                Literal::String(s) => Value::Str(s.clone()),
                Literal::Bytea(b) => Value::Bytea(b.clone()),
                Literal::Date(d) => Value::Date(*d),
                Literal::Time(t) => Value::Time(*t),
                Literal::Timestamp(t) => Value::Timestamp(*t),
                Literal::Interval(i) => Value::Interval(i.clone()),
            };
            Ok(RuntimeExpr::Constant(value))
        }

        AstExpr::Column(_, col_name) => {
            // Find column index in schema
            let (col_idx, _) = schema
                .get_column(col_name)
                .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
            Ok(RuntimeExpr::Column(col_idx))
        }

        AstExpr::Operator(op) => match op {
            Operator::Add(l, r) => Ok(RuntimeExpr::Add(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Subtract(l, r) => Ok(RuntimeExpr::Subtract(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Multiply(l, r) => Ok(RuntimeExpr::Multiply(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Divide(l, r) => Ok(RuntimeExpr::Divide(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Remainder(l, r) => Ok(RuntimeExpr::Remainder(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Concat(l, r) => Ok(RuntimeExpr::Concat(
                Box::new(convert_ast_to_runtime_expr(l, schema)?),
                Box::new(convert_ast_to_runtime_expr(r, schema)?),
            )),
            Operator::Negate(e) => Ok(RuntimeExpr::Negate(Box::new(convert_ast_to_runtime_expr(
                e, schema,
            )?))),
            Operator::Identity(e) => Ok(RuntimeExpr::Identity(Box::new(
                convert_ast_to_runtime_expr(e, schema)?,
            ))),
            _ => Err(Error::Internal(
                "Unsupported operator in expression index".to_string(),
            )),
        },

        _ => Err(Error::Internal(
            "Unsupported expression type in index".to_string(),
        )),
    }
}

/// Extract column names referenced in an expression
fn extract_column_dependencies(expr: &crate::parsing::ast::Expression) -> Vec<String> {
    use crate::parsing::ast::{Expression as AstExpr, Operator};

    let mut deps = Vec::new();

    match expr {
        AstExpr::Column(_, col_name) => {
            deps.push(col_name.clone());
        }
        AstExpr::Operator(op) => {
            use Operator::*;
            match op {
                Add(l, r)
                | Subtract(l, r)
                | Multiply(l, r)
                | Divide(l, r)
                | Remainder(l, r)
                | Concat(l, r) => {
                    deps.extend(extract_column_dependencies(l));
                    deps.extend(extract_column_dependencies(r));
                }
                Negate(e) | Identity(e) | Not(e) => {
                    deps.extend(extract_column_dependencies(e));
                }
                _ => {}
            }
        }
        _ => {}
    }

    deps
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

        Node::UnnestScan { array, .. } => {
            // UNNEST requires an outer row context to evaluate the array expression
            // If no outer_row is provided, this is an error
            if outer_row.is_none() {
                return Err(Error::ExecutionError(
                    "UNNEST must be used in a lateral context (e.g., CROSS JOIN)".into(),
                ));
            }

            // Evaluate the array expression with the outer row context
            let array_value = expression::evaluate_with_storage(
                &array,
                outer_row.map(|v| &**v),
                tx_ctx,
                params,
                Some(storage),
            )?;

            // Extract array/list elements
            let elements = match array_value {
                Value::Array(items) | Value::List(items) => items,
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "UNNEST requires an array or list, got {:?}",
                        array_value
                    )));
                }
            };

            // Convert each element into a row with a single column
            let rows: Vec<Arc<Vec<Value>>> = elements
                .into_iter()
                .map(|elem| Arc::new(vec![elem]))
                .collect();

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
                // Type-coerce filter values based on index column schema
                let schemas = storage.get_schemas();
                let mut coerced_values = filter_values.clone();

                // Get index metadata to find the actual column names
                if let Some(index_meta) = storage.get_index_metadata().get(&index_name)
                    && let Some(schema) = schemas.get(&table)
                {
                    // Get column names from the index
                    let column_names: Vec<&str> = index_meta
                        .columns
                        .iter()
                        .filter_map(|col| col.as_column())
                        .collect();

                    // Coerce each value to its corresponding column's data type
                    for (i, (value, col_name)) in
                        filter_values.iter().zip(column_names.iter()).enumerate()
                    {
                        if let Some(column) = schema.columns.iter().find(|c| &c.name == col_name) {
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
                .and_then(|meta| {
                    // Extract simple column names (skip expression indexes for now)
                    let names: Vec<String> = meta
                        .columns
                        .iter()
                        .filter_map(|col| col.as_column().map(|s| s.to_string()))
                        .collect();
                    if names.is_empty() { None } else { Some(names) }
                })
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
                // Type-coerce range values based on index column schema
                let schemas = storage.get_schemas();
                let mut coerced_start = start_values.clone();
                let mut coerced_end = end_values.clone();

                // Get index metadata to find the actual column names
                if let Some(index_meta) = storage.get_index_metadata().get(&index_name)
                    && let Some(schema) = schemas.get(&table)
                {
                    // Get column names from the index
                    let column_names: Vec<&str> = index_meta
                        .columns
                        .iter()
                        .filter_map(|col| col.as_column())
                        .collect();

                    // Coerce start values
                    if let Some(ref mut start_vals) = coerced_start {
                        for (value, col_name) in start_vals.iter_mut().zip(column_names.iter()) {
                            if let Some(column) =
                                schema.columns.iter().find(|c| &c.name == col_name)
                            {
                                *value = crate::coercion::coerce_value(
                                    value.clone(),
                                    &column.data_type,
                                )?;
                            }
                        }
                    }

                    // Coerce end values
                    if let Some(ref mut end_vals) = coerced_end {
                        for (value, col_name) in end_vals.iter_mut().zip(column_names.iter()) {
                            if let Some(column) =
                                schema.columns.iter().find(|c| &c.name == col_name)
                            {
                                *value = crate::coercion::coerce_value(
                                    value.clone(),
                                    &column.data_type,
                                )?;
                            }
                        }
                    }
                }

                // Use streaming index range scan with coerced values
                let rows = storage.index_range_scan(
                    &index_name,
                    coerced_start,
                    start_inclusive,
                    coerced_end,
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
                .and_then(|meta| {
                    // Extract simple column names (skip expression indexes for now)
                    let names: Vec<String> = meta
                        .columns
                        .iter()
                        .filter_map(|col| col.as_column().map(|s| s.to_string()))
                        .collect();
                    if names.is_empty() { None } else { Some(names) }
                })
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

        Node::DistinctOn { source, on } => {
            let rows = execute_node_read_with_outer(*source, storage, tx_ctx, params, outer_row)?;

            // DISTINCT ON keeps the first row for each distinct value of the ON expressions
            // PostgreSQL semantics: rows should be sorted by ON expressions first (via ORDER BY)
            let mut seen = std::collections::HashSet::new();
            let mut distinct_rows = Vec::new();

            for row_result in rows {
                let row = row_result?;

                // Evaluate the ON expressions for this row
                let mut on_values = Vec::new();
                for expr in &on {
                    let value = expression::evaluate_with_arc_and_storage(
                        expr,
                        Some(&row),
                        tx_ctx,
                        params,
                        Some(storage),
                    )?;
                    on_values.push(value);
                }

                // Check if we've seen this combination of ON values
                let hash_key = crate::execution::aggregator::HashableValue(Value::List(on_values));
                if seen.insert(hash_key) {
                    // First occurrence of this ON combination - keep it
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

            // Special handling for UNNEST (lateral join)
            // UNNEST needs to be re-executed for each left row
            if matches!(&*right, Node::UnnestScan { .. }) {
                // Execute left side once
                let left_rows = execute_node_read_with_outer(*left, storage, tx_ctx, params, None)?;

                // For each left row, execute UNNEST and combine results
                let mut result_rows: Vec<Arc<Vec<Value>>> = Vec::new();

                for left_row_result in left_rows {
                    let left_row = left_row_result?;

                    // Execute UNNEST with this left row as context
                    let unnest_rows = execute_node_read_with_outer(
                        (*right).clone(),
                        storage,
                        tx_ctx,
                        params,
                        Some(&left_row),
                    )?;

                    // Combine left row with each unnest result
                    for unnest_row_result in unnest_rows {
                        let unnest_row = unnest_row_result?;
                        let mut combined = Vec::with_capacity(left_columns + right_columns);
                        combined.extend(left_row.iter().cloned());
                        combined.extend(unnest_row.iter().cloned());
                        result_rows.push(Arc::new(combined));
                    }
                }

                Ok(Box::new(result_rows.into_iter().map(Ok)))
            } else {
                // IMPORTANT: Do NOT pass outer_row to the join sources!
                // Inline views in FROM clauses are independent and should not see outer context.
                // Only pass outer_row for correlated subqueries in WHERE/SELECT clauses.
                let left_rows = execute_node_read_with_outer(*left, storage, tx_ctx, params, None)?;
                let right_rows =
                    execute_node_read_with_outer(*right, storage, tx_ctx, params, None)?;

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
                            crate::types::plan::Direction::Ascending => cmp,
                            crate::types::plan::Direction::Descending => cmp.reverse(),
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
