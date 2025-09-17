//! Constraint validation
//!
//! Validates database constraints like NOT NULL, PRIMARY KEY, etc.

use crate::coercion::can_coerce;
use crate::error::{Error, Result};
use crate::parsing::ast::{DmlStatement, Expression, InsertSource, Statement};
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::{AnalyzedStatement, ExpressionId, SqlContext};

/// Validates constraints and updates parameter requirements
pub struct ConstraintValidator;

impl ConstraintValidator {
    /// Create a new constraint validator
    pub fn new() -> Self {
        Self
    }

    /// Validate constraints and update parameter requirements
    pub fn validate(
        &self,
        analyzed: &mut AnalyzedStatement,
        context: &AnalysisContext,
    ) -> Result<()> {
        match analyzed.ast.as_ref() {
            Statement::Dml(DmlStatement::Insert {
                table,
                columns,
                source,
            }) => {
                // Get table schema
                if let Some(schema) = context.schemas().get(table) {
                    // Determine target columns
                    let target_columns = if let Some(cols) = columns {
                        // When columns are explicitly specified, check for missing required columns
                        // that don't have defaults
                        for column in &schema.columns {
                            if !cols.contains(&column.name) {
                                // Column is not in the INSERT list
                                if !column.nullable && column.default.is_none() {
                                    return Err(Error::NullConstraintViolation(
                                        column.name.clone(),
                                    ));
                                }
                            }
                        }
                        cols.clone()
                    } else {
                        schema.columns.iter().map(|c| c.name.clone()).collect()
                    };

                    // Validate column count matches and types are compatible for VALUES
                    if let InsertSource::Values(rows) = source {
                        for (row_idx, row) in rows.iter().enumerate() {
                            if row.len() != target_columns.len() {
                                return Err(Error::ExecutionError(format!(
                                    "INSERT row {} has {} values but {} columns specified",
                                    row_idx + 1,
                                    row.len(),
                                    target_columns.len()
                                )));
                            }

                            // Check type compatibility for each value
                            for (col_idx, expr) in row.iter().enumerate() {
                                // Skip parameters since they're checked separately
                                if matches!(expr, Expression::Parameter(_)) {
                                    continue;
                                }

                                // Get column information
                                let column_name = &target_columns[col_idx];
                                if let Some(column) =
                                    schema.columns.iter().find(|c| &c.name == column_name)
                                {
                                    // Check NULL constraint
                                    let is_null = matches!(
                                        expr,
                                        Expression::Literal(crate::parsing::ast::Literal::Null)
                                    );
                                    if is_null && !column.nullable {
                                        return Err(Error::NullConstraintViolation(
                                            column_name.clone(),
                                        ));
                                    }

                                    // Check type compatibility (but allow coercion)
                                    if !is_null {
                                        let expr_id =
                                            ExpressionId::from_path(vec![5, row_idx, col_idx]);
                                        if let Some(type_info) = analyzed.get_type(&expr_id) {
                                            // Use can_coerce to check if the type can be converted
                                            if !can_coerce(&type_info.data_type, &column.datatype) {
                                                return Err(Error::TypeMismatch {
                                                    expected: format!("{:?}", column.datatype),
                                                    found: format!("{:?}", type_info.data_type),
                                                });
                                            }
                                        }
                                    }

                                    // Check foreign key constraints (skip for NULL values)
                                    if let Some(ref referenced_table) = column.references
                                        && !is_null
                                    {
                                        // Check if referenced table exists
                                        if let Some(ref_schema) =
                                            context.schemas().get(referenced_table)
                                        {
                                            // Verify the referenced table has a primary key
                                            if let Some(pk_idx) = ref_schema.primary_key {
                                                let pk_column = &ref_schema.columns[pk_idx];

                                                // Check type compatibility with referenced primary key
                                                let expr_id = ExpressionId::from_path(vec![
                                                    5, row_idx, col_idx,
                                                ]);
                                                if let Some(type_info) = analyzed.get_type(&expr_id)
                                                    && !can_coerce(
                                                        &type_info.data_type,
                                                        &pk_column.datatype,
                                                    )
                                                {
                                                    return Err(Error::ExecutionError(format!(
                                                        "Foreign key column '{}' type {:?} is incompatible with referenced primary key '{}' type {:?}",
                                                        column_name,
                                                        type_info.data_type,
                                                        pk_column.name,
                                                        pk_column.datatype
                                                    )));
                                                }
                                            } else {
                                                return Err(Error::ExecutionError(format!(
                                                    "Referenced table '{}' has no primary key",
                                                    referenced_table
                                                )));
                                            }
                                        } else {
                                            return Err(Error::ExecutionError(format!(
                                                "Foreign key references non-existent table '{}'",
                                                referenced_table
                                            )));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Check NOT NULL constraints for parameters
                    for slot in &mut analyzed.parameter_slots {
                        if let SqlContext::InsertValue { column_index } =
                            slot.coercion_context.sql_context
                            && column_index < schema.columns.len()
                        {
                            let col = &schema.columns[column_index];
                            if !col.nullable {
                                slot.coercion_context.nullable = false;
                                slot.description = if col.default.is_some() {
                                    format!("Parameter for column '{}' (has default)", col.name)
                                } else {
                                    format!("Parameter for non-nullable column '{}'", col.name)
                                };
                            }
                        }
                    }
                }
                Ok(())
            }

            Statement::Dml(DmlStatement::Update { table, set, .. }) => {
                // Get table schema
                if let Some(schema) = context.schemas().get(table) {
                    // Check type compatibility for UPDATE SET expressions
                    for (column_name, expr_opt) in set {
                        if let Some(expr) = expr_opt {
                            // Skip parameters since they're checked separately
                            if matches!(expr, Expression::Parameter(_)) {
                                continue;
                            }

                            // Find the column in schema
                            if let Some(column) =
                                schema.columns.iter().find(|c| &c.name == column_name)
                            {
                                // Get expression type from annotations
                                // UPDATE SET expressions have a different path structure
                                // We need to find the right expression ID
                                // For now, we'll skip if we can't find it
                                // TODO: Properly track UPDATE expression IDs

                                // Check NOT NULL constraint
                                if !column.nullable
                                    && matches!(
                                        expr,
                                        Expression::Literal(crate::parsing::ast::Literal::Null)
                                    )
                                {
                                    return Err(Error::NullConstraintViolation(
                                        column_name.clone(),
                                    ));
                                }

                                // Check foreign key constraints for UPDATE
                                if let Some(ref referenced_table) = column.references {
                                    // Check if referenced table exists
                                    if let Some(ref_schema) =
                                        context.schemas().get(referenced_table)
                                    {
                                        // Find the primary key column of the referenced table
                                        if let Some(_pk_idx) = ref_schema.primary_key {
                                            // Check NULL values for non-nullable foreign keys
                                            if !column.nullable
                                                && matches!(
                                                    expr,
                                                    Expression::Literal(
                                                        crate::parsing::ast::Literal::Null
                                                    )
                                                )
                                            {
                                                return Err(Error::NullConstraintViolation(
                                                    column_name.clone(),
                                                ));
                                            }
                                            // Note: Type checking for UPDATE expressions is harder without expression IDs
                                            // The TODO above mentions this limitation
                                        } else {
                                            return Err(Error::ExecutionError(format!(
                                                "Referenced table '{}' has no primary key",
                                                referenced_table
                                            )));
                                        }
                                    } else {
                                        return Err(Error::ExecutionError(format!(
                                            "Foreign key references non-existent table '{}'",
                                            referenced_table
                                        )));
                                    }
                                }
                            } else {
                                return Err(Error::ExecutionError(format!(
                                    "Column '{}' not found in table '{}'",
                                    column_name, table
                                )));
                            }
                        }
                    }
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }
}
