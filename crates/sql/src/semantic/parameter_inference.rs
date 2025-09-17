//! Parameter type inference from context
//!
//! This module infers parameter types based on their usage context,
//! such as comparison operators, function arguments, and column assignments.

use crate::error::Result;
use crate::parsing::ast::{Expression, Operator};
use crate::semantic::analyzed::{AnalyzedStatement, ExpressionId};
use crate::semantic::context::AnalysisContext;
use crate::types::data_type::DataType;

/// Infer acceptable types for a parameter based on its context
pub fn infer_parameter_types(
    expr_id: &ExpressionId,
    analyzed: &AnalyzedStatement,
    context: &AnalysisContext,
    parent_expr: Option<&Expression>,
) -> Result<Vec<DataType>> {
    // If parameter is in a comparison, infer from the other operand
    if let Some(parent) = parent_expr {
        if let Expression::Operator(op) = parent {
            return infer_from_operator(op, expr_id, analyzed, context);
        }
    }

    // Default: accept any type
    Ok(vec![])
}

/// Infer parameter types from operator context
fn infer_from_operator(
    op: &Operator,
    param_expr_id: &ExpressionId,
    analyzed: &AnalyzedStatement,
    context: &AnalysisContext,
) -> Result<Vec<DataType>> {
    match op {
        // Comparison operators - parameter should match the other operand's type
        Operator::Equal(left, right)
        | Operator::NotEqual(left, right)
        | Operator::LessThan(left, right)
        | Operator::LessThanOrEqual(left, right)
        | Operator::GreaterThan(left, right)
        | Operator::GreaterThanOrEqual(left, right) => {
            // Determine which operand is the parameter
            let other_expr = if matches!(left.as_ref(), Expression::Parameter(_)) {
                right
            } else {
                left
            };

            // Get the type of the other operand
            let other_expr_id = if param_expr_id.path().last() == Some(&0) {
                param_expr_id.parent().child(1) // Get sibling
            } else {
                param_expr_id.parent().child(0)
            };

            if let Some(type_info) = analyzed.get_type(&other_expr_id) {
                return Ok(vec![type_info.data_type.clone()]);
            }

            // If other operand is a column, get its type from context
            if let Expression::Column(table, column) = other_expr.as_ref() {
                if let Some(col_type) = resolve_column_type(table.as_deref(), column, context) {
                    return Ok(vec![col_type]);
                }
            }

            Ok(vec![])
        }

        // Boolean operators - parameters must be boolean
        Operator::And(_, _) | Operator::Or(_, _) => {
            Ok(vec![DataType::Bool])
        }

        // Arithmetic operators - parameters must be numeric
        Operator::Add(_, _)
        | Operator::Subtract(_, _)
        | Operator::Multiply(_, _)
        | Operator::Divide(_, _)
        | Operator::Remainder(_, _)
        | Operator::Exponentiate(_, _) => {
            Ok(vec![
                DataType::I8, DataType::I16, DataType::I32, DataType::I64,
                DataType::F32, DataType::F64
            ])
        }

        // String operators
        Operator::Like(left, _right) => {
            // LIKE requires text types
            Ok(vec![DataType::Text])
        }

        // IN operator - parameter should match list element types
        Operator::InList { expr, list, .. } => {
            // If parameter is the expr, it should match list element types
            if matches!(expr.as_ref(), Expression::Parameter(_)) {
                // Get types from list elements
                let mut types = Vec::new();
                for item in list {
                    if let Expression::Constant(val) = item {
                        let dt = val.data_type();
                        if !types.contains(&dt) {
                            types.push(dt);
                        }
                    }
                }
                return Ok(types);
            }

            // If parameter is in the list, it should match expr type
            // This would need the expr type
            Ok(vec![])
        }

        // BETWEEN operator
        Operator::Between { expr, low, high, .. } => {
            // All three expressions should have compatible types
            // For now, accept numeric types
            Ok(vec![
                DataType::I8, DataType::I16, DataType::I32, DataType::I64,
                DataType::F32, DataType::F64
            ])
        }

        _ => Ok(vec![])
    }
}

/// Resolve column type from context
fn resolve_column_type(
    table: Option<&str>,
    column: &str,
    context: &AnalysisContext,
) -> Option<DataType> {
    // Try to find column in context
    for table_info in context.tables() {
        if table.is_none() || table == Some(&table_info.name) || table == table_info.alias.as_deref() {
            for col in &table_info.schema.columns {
                if col.name == column {
                    return Some(col.datatype.clone());
                }
            }
        }
    }
    None
}

impl ExpressionId {
    /// Get parent expression ID
    pub fn parent(&self) -> ExpressionId {
        let mut path = self.path().to_vec();
        path.pop();
        ExpressionId::from_path(path)
    }
}