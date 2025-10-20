//! Type inference and checking phase of semantic analysis
//!
//! This module handles:
//! - Bottom-up type inference for expressions
//! - Type compatibility checking for operations
//! - Implicit type coercions
//! - Type annotation of all expressions

use super::statement::{ColumnResolutionMap, ExpressionId, TypeInfo};
use crate::error::{Error, Result};
use crate::functions;
use crate::operators;
use crate::parsing::ast::{DmlStatement, Expression, Literal, Operator, Statement};
use crate::types::data_type::DataType;
use std::collections::HashMap;
use std::sync::Arc;

/// Handles type inference and checking
pub struct TypeChecker {
    // Could add configuration for strictness levels, coercion rules, etc.
}

impl TypeChecker {
    /// Create a new type checker
    pub fn new() -> Self {
        Self {}
    }

    /// Infer type of a literal
    fn infer_literal_type(&self, lit: &Literal) -> TypeInfo {
        let data_type = match lit {
            Literal::Null => DataType::Null,
            Literal::Boolean(_) => DataType::Bool,
            Literal::Integer(n) => {
                // Choose appropriate integer type based on value
                if *n >= i32::MIN as i128 && *n <= i32::MAX as i128 {
                    DataType::I32
                } else if *n >= i64::MIN as i128 && *n <= i64::MAX as i128 {
                    DataType::I64
                } else {
                    DataType::I128
                }
            }
            Literal::Float(_) => DataType::F64,
            Literal::String(_) => DataType::Str,
            Literal::Bytea(_) => DataType::Bytea,
            Literal::Date(_) => DataType::Date,
            Literal::Time(_) => DataType::Time,
            Literal::Timestamp(_) => DataType::Timestamp,
            Literal::Interval(_) => DataType::Interval,
        };

        TypeInfo {
            data_type,
            nullable: matches!(lit, Literal::Null),
            is_aggregate: false,
        }
    }

    /// Infer types for all expressions in a statement (for new phase architecture)
    pub fn infer_all_types(
        &self,
        statement: &Arc<Statement>,
        resolution_view: &super::analyzer::ResolutionView,
        param_types: &[DataType],
    ) -> Result<HashMap<ExpressionId, TypeInfo>> {
        let mut type_map = HashMap::new();

        if let Statement::Dml(dml) = statement.as_ref() {
            match dml {
                DmlStatement::Select(select) => {
                    // Type check SELECT expressions
                    for (idx, (expr, _)) in select.select.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![idx]);
                        let type_info = self.infer_expr_type_new(
                            expr,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }

                    // Type check WHERE clause
                    if let Some(where_expr) = &select.r#where {
                        let expr_id = ExpressionId::from_path(vec![1000]);
                        let type_info = self.infer_expr_type_new(
                            where_expr,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }

                    // Type check GROUP BY
                    for (idx, expr) in select.group_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![2000 + idx]);
                        let type_info = self.infer_expr_type_new(
                            expr,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }

                    // Type check HAVING
                    if let Some(having) = &select.having {
                        let expr_id = ExpressionId::from_path(vec![3000]);
                        let type_info = self.infer_expr_type_new(
                            having,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }

                    // Type check ORDER BY
                    // Build alias map from SELECT clause for ORDER BY resolution
                    let mut alias_types: HashMap<String, TypeInfo> = HashMap::new();
                    for (idx, (_, alias_opt)) in select.select.iter().enumerate() {
                        if let Some(alias) = alias_opt {
                            // Get the type of the aliased expression from the type_map
                            let select_expr_id = ExpressionId::from_path(vec![idx]);
                            if let Some(type_info) = type_map.get(&select_expr_id) {
                                alias_types.insert(alias.clone(), type_info.clone());
                            }
                        }
                    }

                    for (idx, (expr, _)) in select.order_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![4000 + idx]);

                        // Check if this is a simple column reference that matches a SELECT alias
                        let type_info = if let Expression::Column(None, col_name) = expr {
                            if let Some(alias_type) = alias_types.get(col_name) {
                                // Use the type from the aliased SELECT expression
                                alias_type.clone()
                            } else {
                                // Not an alias, resolve normally
                                self.infer_expr_type_new(
                                    expr,
                                    &expr_id,
                                    resolution_view.column_map,
                                    param_types,
                                    &mut type_map,
                                )?
                            }
                        } else {
                            // Not a simple column reference, resolve normally
                            self.infer_expr_type_new(
                                expr,
                                &expr_id,
                                resolution_view.column_map,
                                param_types,
                                &mut type_map,
                            )?
                        };

                        type_map.insert(expr_id, type_info);
                    }
                }
                DmlStatement::Insert { source, .. } => {
                    use crate::parsing::ast::InsertSource;
                    match source {
                        InsertSource::Values(rows) => {
                            for (row_idx, row) in rows.iter().enumerate() {
                                for (col_idx, expr) in row.iter().enumerate() {
                                    let expr_id = ExpressionId::from_path(vec![row_idx, col_idx]);
                                    let type_info = self.infer_expr_type_new(
                                        expr,
                                        &expr_id,
                                        resolution_view.column_map,
                                        param_types,
                                        &mut type_map,
                                    )?;
                                    type_map.insert(expr_id, type_info);
                                }
                            }
                        }
                        InsertSource::Select(select) => {
                            // Recurse for inner SELECT (simplified for now)
                            for (idx, (expr, _)) in select.select.iter().enumerate() {
                                let expr_id = ExpressionId::from_path(vec![5000 + idx]);
                                let type_info = self.infer_expr_type_new(
                                    expr,
                                    &expr_id,
                                    resolution_view.column_map,
                                    param_types,
                                    &mut type_map,
                                )?;
                                type_map.insert(expr_id, type_info);
                            }
                        }
                        InsertSource::DefaultValues => {}
                    }
                }
                DmlStatement::Values(values) => {
                    // First, determine column types from VALUES rows
                    let mut column_types = Vec::new();
                    if !values.rows.is_empty() {
                        let num_cols = values.rows[0].len();

                        // Check all rows have same number of columns (validation already done)
                        // Infer type for each column across all rows
                        for col_idx in 0..num_cols {
                            let mut col_type = None;

                            for (row_idx, row) in values.rows.iter().enumerate() {
                                // Skip if this row doesn't have enough columns (caught by validation)
                                if col_idx >= row.len() {
                                    continue;
                                }

                                let expr_id =
                                    ExpressionId::from_path(vec![9000 + row_idx, col_idx]);
                                let type_info = self.infer_expr_type_new(
                                    &row[col_idx],
                                    &expr_id,
                                    resolution_view.column_map,
                                    param_types,
                                    &mut type_map,
                                )?;
                                type_map.insert(expr_id.clone(), type_info.clone());

                                // Unify column type across rows
                                col_type = match col_type {
                                    None => Some(type_info.data_type.clone()),
                                    Some(existing_type) => {
                                        // Check type compatibility
                                        match self.unify_values_types(
                                            &existing_type,
                                            &type_info.data_type,
                                        ) {
                                            Ok(unified) => Some(unified),
                                            Err(_) => {
                                                return Err(Error::ExecutionError(format!(
                                                    "Incompatible types in VALUES column {}: {} vs {}",
                                                    col_idx + 1,
                                                    existing_type,
                                                    type_info.data_type
                                                )));
                                            }
                                        }
                                    }
                                };
                            }

                            column_types.push(col_type.unwrap_or(DataType::Null));
                        }
                    }

                    // Type check ORDER BY with VALUES column context
                    for (idx, (expr, _)) in values.order_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![9500 + idx]);

                        // Special handling for columnN references in VALUES ORDER BY
                        let type_info = match expr {
                            Expression::Column(None, name) if name.starts_with("column") => {
                                // Try to parse columnN as a column index
                                if let Ok(col_num) = name[6..].parse::<usize>() {
                                    if col_num > 0 && col_num <= column_types.len() {
                                        TypeInfo {
                                            data_type: column_types[col_num - 1].clone(),
                                            nullable: true, // VALUES columns are nullable
                                            is_aggregate: false,
                                        }
                                    } else {
                                        return Err(Error::ColumnNotFound(name.clone()));
                                    }
                                } else {
                                    // Not a valid columnN reference - try normal resolution
                                    self.infer_expr_type_new(
                                        expr,
                                        &expr_id,
                                        resolution_view.column_map,
                                        param_types,
                                        &mut type_map,
                                    )?
                                }
                            }
                            _ => self.infer_expr_type_new(
                                expr,
                                &expr_id,
                                resolution_view.column_map,
                                param_types,
                                &mut type_map,
                            )?,
                        };

                        type_map.insert(expr_id, type_info);
                    }
                }
                DmlStatement::Update { set, r#where, .. } => {
                    for (idx, (_, expr_opt)) in set.iter().enumerate() {
                        if let Some(expr) = expr_opt {
                            let expr_id = ExpressionId::from_path(vec![6000 + idx]);
                            let type_info = self.infer_expr_type_new(
                                expr,
                                &expr_id,
                                resolution_view.column_map,
                                param_types,
                                &mut type_map,
                            )?;
                            type_map.insert(expr_id, type_info);
                        }
                    }

                    if let Some(where_expr) = r#where {
                        let expr_id = ExpressionId::from_path(vec![7000]);
                        let type_info = self.infer_expr_type_new(
                            where_expr,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }
                }
                DmlStatement::Delete { r#where, .. } => {
                    if let Some(where_expr) = r#where {
                        let expr_id = ExpressionId::from_path(vec![8000]);
                        let type_info = self.infer_expr_type_new(
                            where_expr,
                            &expr_id,
                            resolution_view.column_map,
                            param_types,
                            &mut type_map,
                        )?;
                        type_map.insert(expr_id, type_info);
                    }
                }
            }
        }

        Ok(type_map)
    }

    /// Infer type of an expression (for new phase architecture)
    fn infer_expr_type_new(
        &self,
        expr: &Expression,
        expr_id: &ExpressionId,
        column_map: &ColumnResolutionMap,
        param_types: &[DataType],
        type_map: &mut HashMap<ExpressionId, TypeInfo>,
    ) -> Result<TypeInfo> {
        let type_info = match expr {
            Expression::Literal(lit) => self.infer_literal_type(lit),

            Expression::Column(table, col) => {
                // Use the column map for resolution
                if let Some(resolution) = column_map.resolve(table.as_deref(), col) {
                    TypeInfo {
                        data_type: resolution.data_type.clone(),
                        nullable: resolution.nullable,
                        is_aggregate: false,
                    }
                } else if let Some(qualifier) = table {
                    // Column not found with table qualifier
                    // Check if the qualifier is actually a struct column (DuckDB-style resolution)
                    if let Some(struct_col_resolution) = column_map.resolve(None, qualifier) {
                        // Check if it's a struct type
                        if let DataType::Struct(fields) = &struct_col_resolution.data_type {
                            // Look for the field in the struct
                            if let Some((_, field_type)) =
                                fields.iter().find(|(name, _)| name == col)
                            {
                                return Ok(TypeInfo {
                                    data_type: field_type.clone(),
                                    nullable: true, // Struct field access can return NULL
                                    is_aggregate: false,
                                });
                            } else {
                                return Err(Error::ExecutionError(format!(
                                    "Struct column '{}' does not have field '{}'",
                                    qualifier, col
                                )));
                            }
                        }
                    }
                    return Err(Error::ColumnNotFound(col.clone()));
                } else {
                    // Check if column is ambiguous (exists in multiple tables)
                    if column_map.is_ambiguous(col) {
                        return Err(Error::AmbiguousColumn(col.clone()));
                    }
                    return Err(Error::ColumnNotFound(col.clone()));
                }
            }

            Expression::Parameter(idx) => TypeInfo {
                data_type: param_types.get(*idx).cloned().unwrap_or(DataType::Null),
                nullable: true,
                is_aggregate: false,
            },

            Expression::Operator(op) => {
                self.infer_operator_type_new(op, expr_id, column_map, param_types, type_map)?
            }

            Expression::Function(name, args) => self.infer_function_type_new(
                name,
                args,
                expr_id,
                column_map,
                param_types,
                type_map,
            )?,

            Expression::All => TypeInfo {
                data_type: DataType::Bool,
                nullable: false,
                is_aggregate: false,
            },

            Expression::QualifiedWildcard(_) => TypeInfo {
                // Qualified wildcards are expanded during planning
                data_type: DataType::Bool,
                nullable: false,
                is_aggregate: false,
            },

            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Infer type from operand if present
                let mut is_aggregate = false;
                if let Some(op) = operand {
                    let op_id = expr_id.child(0);
                    let op_type =
                        self.infer_expr_type_new(op, &op_id, column_map, param_types, type_map)?;
                    is_aggregate |= op_type.is_aggregate;
                }

                // Infer types from all WHEN and THEN expressions
                let mut result_type: Option<DataType> = None;
                let mut nullable = false;

                for (idx, (when, then)) in when_clauses.iter().enumerate() {
                    // Type check WHEN expression
                    let when_id = expr_id.child(1000 + idx * 2);
                    let when_type = self.infer_expr_type_new(
                        when,
                        &when_id,
                        column_map,
                        param_types,
                        type_map,
                    )?;
                    is_aggregate |= when_type.is_aggregate;

                    // Type check THEN expression and unify with result type
                    let then_id = expr_id.child(1000 + idx * 2 + 1);
                    let then_type = self.infer_expr_type_new(
                        then,
                        &then_id,
                        column_map,
                        param_types,
                        type_map,
                    )?;
                    is_aggregate |= then_type.is_aggregate;
                    nullable |= then_type.nullable;

                    result_type = match result_type {
                        None => Some(then_type.data_type),
                        Some(existing) => {
                            Some(self.unify_values_types(&existing, &then_type.data_type)?)
                        }
                    };
                }

                // Type check ELSE clause if present
                if let Some(else_expr) = else_clause {
                    let else_id = expr_id.child(9000);
                    let else_type = self.infer_expr_type_new(
                        else_expr,
                        &else_id,
                        column_map,
                        param_types,
                        type_map,
                    )?;
                    is_aggregate |= else_type.is_aggregate;
                    nullable |= else_type.nullable;

                    result_type = match result_type {
                        None => Some(else_type.data_type),
                        Some(existing) => {
                            Some(self.unify_values_types(&existing, &else_type.data_type)?)
                        }
                    };
                } else {
                    // No ELSE clause means NULL is a possible result
                    nullable = true;
                }

                TypeInfo {
                    data_type: result_type.unwrap_or(DataType::Null),
                    nullable,
                    is_aggregate,
                }
            }

            Expression::ArrayAccess { base, .. } => {
                let base_id = expr_id.child(0);
                let base_type =
                    self.infer_expr_type_new(base, &base_id, column_map, param_types, type_map)?;

                match base_type.data_type {
                    DataType::Array(elem_type, _) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                    },
                    DataType::List(elem_type) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                    },
                    DataType::Map(_key_type, value_type) => TypeInfo {
                        data_type: *value_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                    },
                    _ => {
                        return Err(Error::ExecutionError(
                            "Array access on non-array/list/map type".into(),
                        ));
                    }
                }
            }

            Expression::FieldAccess { base, field } => {
                let base_id = expr_id.child(0);
                let base_type =
                    self.infer_expr_type_new(base, &base_id, column_map, param_types, type_map)?;

                match base_type.data_type {
                    DataType::Struct(fields) => {
                        if let Some((_, dtype)) = fields.iter().find(|(name, _)| name == field) {
                            TypeInfo {
                                data_type: dtype.clone(),
                                nullable: true,
                                is_aggregate: base_type.is_aggregate,
                            }
                        } else {
                            return Err(Error::ExecutionError(format!(
                                "Field '{}' not found",
                                field
                            )));
                        }
                    }
                    _ => return Err(Error::ExecutionError("Field access on non-struct".into())),
                }
            }

            Expression::ArrayLiteral(elements) => {
                if elements.is_empty() {
                    TypeInfo {
                        data_type: DataType::Array(Box::new(DataType::I64), None),
                        nullable: false,
                        is_aggregate: false,
                    }
                } else {
                    let elem_id = expr_id.child(0);
                    let elem_type = self.infer_expr_type_new(
                        &elements[0],
                        &elem_id,
                        column_map,
                        param_types,
                        type_map,
                    )?;
                    TypeInfo {
                        data_type: DataType::Array(Box::new(elem_type.data_type), None),
                        nullable: false,
                        is_aggregate: elem_type.is_aggregate,
                    }
                }
            }

            Expression::MapLiteral(entries) => {
                if entries.is_empty() {
                    TypeInfo {
                        data_type: DataType::Map(Box::new(DataType::Str), Box::new(DataType::I64)),
                        nullable: false,
                        is_aggregate: false,
                    }
                } else {
                    let (key, val) = &entries[0];
                    let key_id = expr_id.child(0);
                    let val_id = expr_id.child(1);
                    let key_type =
                        self.infer_expr_type_new(key, &key_id, column_map, param_types, type_map)?;
                    let val_type =
                        self.infer_expr_type_new(val, &val_id, column_map, param_types, type_map)?;
                    TypeInfo {
                        data_type: DataType::Map(
                            Box::new(key_type.data_type),
                            Box::new(val_type.data_type),
                        ),
                        nullable: false,
                        is_aggregate: key_type.is_aggregate || val_type.is_aggregate,
                    }
                }
            }

            Expression::Cast { expr, target_type } => {
                // Infer the type of the expression being cast
                let cast_expr_id = expr_id.child(0);
                let expr_type = self.infer_expr_type_new(
                    expr,
                    &cast_expr_id,
                    column_map,
                    param_types,
                    type_map,
                )?;

                // The result type is the target type
                // Preserve nullability from the source expression
                TypeInfo {
                    data_type: target_type.clone(),
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                }
            }

            Expression::Subquery(_) => {
                // For now, assume subqueries return a nullable value
                // The actual type would need to be inferred from the SELECT statement
                TypeInfo {
                    data_type: DataType::Null,
                    nullable: true,
                    is_aggregate: false,
                }
            }
        };

        Ok(type_info)
    }

    /// Infer operator type (for new architecture)
    fn infer_operator_type_new(
        &self,
        op: &Operator,
        expr_id: &ExpressionId,
        column_map: &ColumnResolutionMap,
        param_types: &[DataType],
        type_map: &mut HashMap<ExpressionId, TypeInfo>,
    ) -> Result<TypeInfo> {
        use Operator::*;

        match op {
            And(l, r) | Or(l, r) | Xor(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(l, &left_id, column_map, param_types, type_map)?;
                let right_type =
                    self.infer_expr_type_new(r, &right_id, column_map, param_types, type_map)?;

                let result_type = match op {
                    And(_, _) => {
                        operators::validate_and(&left_type.data_type, &right_type.data_type)?
                    }
                    Or(_, _) => {
                        operators::validate_or(&left_type.data_type, &right_type.data_type)?
                    }
                    Xor(_, _) => {
                        operators::validate_xor(&left_type.data_type, &right_type.data_type)?
                    }
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                })
            }

            Equal(l, r)
            | NotEqual(l, r)
            | GreaterThan(l, r)
            | GreaterThanOrEqual(l, r)
            | LessThan(l, r)
            | LessThanOrEqual(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(l, &left_id, column_map, param_types, type_map)?;
                let right_type =
                    self.infer_expr_type_new(r, &right_id, column_map, param_types, type_map)?;

                let result_type = match op {
                    Equal(_, _) => {
                        operators::validate_equal(&left_type.data_type, &right_type.data_type)?
                    }
                    NotEqual(_, _) => {
                        operators::validate_not_equal(&left_type.data_type, &right_type.data_type)?
                    }
                    GreaterThan(_, _) => operators::validate_greater_than(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    GreaterThanOrEqual(_, _) => operators::validate_greater_than_equal(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    LessThan(_, _) => {
                        operators::validate_less_than(&left_type.data_type, &right_type.data_type)?
                    }
                    LessThanOrEqual(_, _) => operators::validate_less_than_equal(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                })
            }

            Add(l, r)
            | Concat(l, r)
            | Subtract(l, r)
            | Multiply(l, r)
            | Divide(l, r)
            | Remainder(l, r)
            | Exponentiate(l, r)
            | BitwiseAnd(l, r)
            | BitwiseOr(l, r)
            | BitwiseXor(l, r)
            | BitwiseShiftLeft(l, r)
            | BitwiseShiftRight(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(l, &left_id, column_map, param_types, type_map)?;
                let right_type =
                    self.infer_expr_type_new(r, &right_id, column_map, param_types, type_map)?;

                let result_type = match op {
                    Add(_, _) => {
                        operators::validate_add(&left_type.data_type, &right_type.data_type)?
                    }
                    Concat(_, _) => {
                        operators::validate_concat(&left_type.data_type, &right_type.data_type)?
                    }
                    Subtract(_, _) => {
                        operators::validate_subtract(&left_type.data_type, &right_type.data_type)?
                    }
                    Multiply(_, _) => {
                        operators::validate_multiply(&left_type.data_type, &right_type.data_type)?
                    }
                    Divide(_, _) => {
                        operators::validate_divide(&left_type.data_type, &right_type.data_type)?
                    }
                    Remainder(_, _) => {
                        operators::validate_remainder(&left_type.data_type, &right_type.data_type)?
                    }
                    Exponentiate(_, _) => operators::validate_exponentiate(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    BitwiseAnd(_, _) => operators::validate_bitwise_and(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    BitwiseOr(_, _) => {
                        operators::validate_bitwise_or(&left_type.data_type, &right_type.data_type)?
                    }
                    BitwiseXor(_, _) => operators::validate_bitwise_xor(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    BitwiseShiftLeft(_, _) => operators::validate_bitwise_shift_left(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    BitwiseShiftRight(_, _) => operators::validate_bitwise_shift_right(
                        &left_type.data_type,
                        &right_type.data_type,
                    )?,
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                })
            }

            ILike { expr, pattern, .. } => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(expr, &left_id, column_map, param_types, type_map)?;
                let right_type = self.infer_expr_type_new(
                    pattern,
                    &right_id,
                    column_map,
                    param_types,
                    type_map,
                )?;

                let result_type =
                    operators::validate_ilike(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                })
            }

            Like { expr, pattern, .. } => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(expr, &left_id, column_map, param_types, type_map)?;
                let right_type = self.infer_expr_type_new(
                    pattern,
                    &right_id,
                    column_map,
                    param_types,
                    type_map,
                )?;

                let result_type =
                    operators::validate_like(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                })
            }

            Not(e) | Negate(e) | Identity(e) | Factorial(e) | BitwiseNot(e) => {
                let inner_id = expr_id.child(0);
                let inner_type =
                    self.infer_expr_type_new(e, &inner_id, column_map, param_types, type_map)?;

                let result_type = match op {
                    Not(_) => operators::validate_not(&inner_type.data_type)?,
                    Negate(_) => operators::validate_negate(&inner_type.data_type)?,
                    Identity(_) => operators::validate_identity(&inner_type.data_type)?,
                    Factorial(_) => operators::validate_factorial(&inner_type.data_type)?,
                    BitwiseNot(_) => operators::validate_bitwise_not(&inner_type.data_type)?,
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                })
            }

            Is(e, _) => {
                let inner_id = expr_id.child(0);
                let inner_type =
                    self.infer_expr_type_new(e, &inner_id, column_map, param_types, type_map)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: false,
                    is_aggregate: inner_type.is_aggregate,
                })
            }

            Between {
                expr,
                low,
                high,
                negated: _,
            } => {
                let expr_id_0 = expr_id.child(0);
                let expr_id_1 = expr_id.child(1);
                let expr_id_2 = expr_id.child(2);

                let expr_type =
                    self.infer_expr_type_new(expr, &expr_id_0, column_map, param_types, type_map)?;
                let low_type =
                    self.infer_expr_type_new(low, &expr_id_1, column_map, param_types, type_map)?;
                let high_type =
                    self.infer_expr_type_new(high, &expr_id_2, column_map, param_types, type_map)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable || low_type.nullable || high_type.nullable,
                    is_aggregate: expr_type.is_aggregate
                        || low_type.is_aggregate
                        || high_type.is_aggregate,
                })
            }

            InList {
                expr,
                list,
                negated: _,
            } => {
                let expr_id_0 = expr_id.child(0);
                let expr_type =
                    self.infer_expr_type_new(expr, &expr_id_0, column_map, param_types, type_map)?;

                let mut is_aggregate = expr_type.is_aggregate;
                let mut any_nullable = expr_type.nullable;

                for (i, item) in list.iter().enumerate() {
                    let item_id = expr_id.child(i + 1);
                    let item_type = self.infer_expr_type_new(
                        item,
                        &item_id,
                        column_map,
                        param_types,
                        type_map,
                    )?;
                    is_aggregate = is_aggregate || item_type.is_aggregate;
                    any_nullable = any_nullable || item_type.nullable;
                }

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: any_nullable,
                    is_aggregate,
                })
            }

            InSubquery { expr, .. } => {
                let expr_id_0 = expr_id.child(0);
                let expr_type =
                    self.infer_expr_type_new(expr, &expr_id_0, column_map, param_types, type_map)?;

                // For now, assume subqueries can have NULL results
                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: true,
                    is_aggregate: expr_type.is_aggregate,
                })
            }

            Exists { .. } => {
                // EXISTS always returns a boolean
                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: false,
                    is_aggregate: false,
                })
            }
        }
    }

    /// Infer function type (for new architecture)
    fn infer_function_type_new(
        &self,
        name: &str,
        args: &[Expression],
        expr_id: &ExpressionId,
        column_map: &ColumnResolutionMap,
        param_types: &[DataType],
        type_map: &mut HashMap<ExpressionId, TypeInfo>,
    ) -> Result<TypeInfo> {
        let func_name = name.to_uppercase();
        let is_aggregate = functions::is_aggregate(&func_name);

        let mut arg_types = Vec::new();
        let mut any_nullable = false;
        let mut any_aggregate = false;

        for (i, arg) in args.iter().enumerate() {
            let arg_id = expr_id.child(i);
            let arg_type =
                self.infer_expr_type_new(arg, &arg_id, column_map, param_types, type_map)?;
            arg_types.push(arg_type.data_type.clone());
            any_nullable = any_nullable || arg_type.nullable;
            any_aggregate = any_aggregate || arg_type.is_aggregate;
        }

        let return_type = functions::validate_function(&func_name, &arg_types)?;

        Ok(TypeInfo {
            data_type: return_type,
            nullable: any_nullable,
            is_aggregate: is_aggregate || any_aggregate,
        })
    }

    /// Unify types for VALUES columns - ensures type compatibility
    /// and returns the unified type
    fn unify_values_types(&self, type1: &DataType, type2: &DataType) -> Result<DataType> {
        match (type1, type2) {
            // NULL is compatible with any type
            (DataType::Null, other) | (other, DataType::Null) => Ok(other.clone()),

            // Same type is always compatible
            (t1, t2) if t1 == t2 => Ok(t1.clone()),

            // Numeric type promotion
            (DataType::I32, DataType::I64) | (DataType::I64, DataType::I32) => Ok(DataType::I64),
            (DataType::I32, DataType::I128) | (DataType::I128, DataType::I32) => Ok(DataType::I128),
            (DataType::I64, DataType::I128) | (DataType::I128, DataType::I64) => Ok(DataType::I128),

            // Integer to float promotion
            (DataType::I32, DataType::F32) | (DataType::F32, DataType::I32) => Ok(DataType::F32),
            (DataType::I32, DataType::F64) | (DataType::F64, DataType::I32) => Ok(DataType::F64),
            (DataType::I64, DataType::F64) | (DataType::F64, DataType::I64) => Ok(DataType::F64),
            (DataType::F32, DataType::F64) | (DataType::F64, DataType::F32) => Ok(DataType::F64),

            // All other combinations are incompatible
            _ => Err(Error::ExecutionError(format!(
                "Incompatible types in VALUES: {} and {}",
                type1, type2
            ))),
        }
    }
}
