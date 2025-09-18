//! Type inference and checking phase of semantic analysis
//!
//! This module handles:
//! - Bottom-up type inference for expressions
//! - Type compatibility checking for operations
//! - Implicit type coercions
//! - Type annotation of all expressions

use super::statement::{
    AnalyzedStatement, ColumnResolutionMap, ExpressionId, ResolvedColumn, TypeInfo,
};
use crate::coercion;
use crate::error::{Error, Result};
use crate::functions;
use crate::operators;
use crate::parsing::ast::{
    DmlStatement, Expression, Literal, Operator, SelectStatement, Statement,
};
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

    /// Main entry point: infer types for all expressions in the statement
    pub fn infer_types(
        &self,
        statement: &Statement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        match statement {
            Statement::Dml(dml) => self.infer_dml_types(dml, analyzed),
            Statement::Ddl(_) => {
                // DDL has minimal type checking
                Ok(())
            }
            Statement::Explain(_) => Ok(()),
        }
    }

    /// Check a specific expression and return its type
    pub fn check_expression(&self, expr: &Expression) -> Result<TypeInfo> {
        // Create a dummy analyzed statement for expression checking
        let dummy_select = SelectStatement {
            select: vec![],
            from: vec![],
            r#where: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        };
        let dummy_statement = Statement::Dml(DmlStatement::Select(Box::new(dummy_select)));
        let mut analyzed = AnalyzedStatement::new(std::sync::Arc::new(dummy_statement));
        let expr_id = ExpressionId::from_path(vec![]);
        self.infer_expression_type(expr, expr_id, &mut analyzed)
    }

    /// Infer types for DML statements
    fn infer_dml_types(
        &self,
        statement: &DmlStatement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        match statement {
            DmlStatement::Select(select) => self.infer_select_types(select, analyzed),
            DmlStatement::Insert { source, .. } => {
                // Infer types for INSERT source
                use crate::parsing::ast::InsertSource;
                match source {
                    InsertSource::Values(rows) => {
                        for (row_idx, row) in rows.iter().enumerate() {
                            for (col_idx, expr) in row.iter().enumerate() {
                                let expr_id = ExpressionId::from_path(vec![row_idx, col_idx]);
                                let type_info =
                                    self.infer_expression_type(expr, expr_id.clone(), analyzed)?;
                                analyzed.type_annotations.annotate(expr_id, type_info);
                            }
                        }
                    }
                    InsertSource::Select(select) => {
                        self.infer_select_types(select, analyzed)?;
                    }
                    InsertSource::DefaultValues => {
                        // No expressions to type
                    }
                }
                Ok(())
            }
            DmlStatement::Update { set, r#where, .. } => {
                // Infer types for SET expressions
                for (col_idx, (_column, expr_opt)) in set.iter().enumerate() {
                    if let Some(expr) = expr_opt {
                        let expr_id = ExpressionId::from_path(vec![col_idx]);
                        let type_info =
                            self.infer_expression_type(expr, expr_id.clone(), analyzed)?;
                        analyzed.type_annotations.annotate(expr_id, type_info);
                    }
                }

                // Infer types for WHERE clause
                if let Some(where_expr) = r#where {
                    let expr_id = ExpressionId::from_path(vec![]);
                    let type_info =
                        self.infer_expression_type(where_expr, expr_id.clone(), analyzed)?;
                    analyzed.type_annotations.annotate(expr_id, type_info);
                }

                Ok(())
            }
            DmlStatement::Delete { r#where, .. } => {
                // Infer types for WHERE clause
                if let Some(where_expr) = r#where {
                    let expr_id = ExpressionId::from_path(vec![]);
                    let type_info =
                        self.infer_expression_type(where_expr, expr_id.clone(), analyzed)?;
                    analyzed.type_annotations.annotate(expr_id, type_info);
                }
                Ok(())
            }
        }
    }

    /// Infer types for SELECT statement
    fn infer_select_types(
        &self,
        select: &SelectStatement,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<()> {
        // Infer types for SELECT expressions
        for (idx, (expr, _alias)) in select.select.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![idx]);
            let type_info = self.infer_expression_type(expr, expr_id.clone(), analyzed)?;
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        // Infer types for WHERE clause
        if let Some(where_expr) = &select.r#where {
            let expr_id = ExpressionId::from_path(vec![]);
            let type_info = self.infer_expression_type(where_expr, expr_id.clone(), analyzed)?;
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        // Infer types for GROUP BY expressions
        for (idx, expr) in select.group_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![1000 + idx]); // Offset to avoid collision
            let type_info = self.infer_expression_type(expr, expr_id.clone(), analyzed)?;
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        // Infer types for HAVING clause
        if let Some(having_expr) = &select.having {
            let expr_id = ExpressionId::from_path(vec![2000]); // Unique ID for HAVING
            let type_info = self.infer_expression_type(having_expr, expr_id.clone(), analyzed)?;
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        // Infer types for ORDER BY expressions
        for (idx, (expr, _)) in select.order_by.iter().enumerate() {
            let expr_id = ExpressionId::from_path(vec![3000 + idx]); // Offset for ORDER BY
            let type_info = self.infer_expression_type(expr, expr_id.clone(), analyzed)?;
            analyzed.type_annotations.annotate(expr_id, type_info);
        }

        Ok(())
    }

    /// Infer and check types for an expression, annotating the analyzed statement
    pub fn infer_expression_type(
        &self,
        expr: &Expression,
        expr_id: ExpressionId,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<TypeInfo> {
        let type_info = match expr {
            Expression::Literal(lit) => self.infer_literal_type(lit),
            Expression::Column(_table, _col) => {
                // Without context, we can't determine the exact column type
                // This would normally be resolved by looking up in schemas
                TypeInfo {
                    data_type: DataType::Null, // Will be resolved properly in the new infer_expr_type_new method
                    nullable: true,
                    is_aggregate: false,
                    resolution: None,
                }
            }
            Expression::Operator(op) => self.infer_operator_type(op, &expr_id, analyzed)?,
            Expression::Function(name, args) => {
                self.infer_function_type(name, args, &expr_id, analyzed)?
            }
            Expression::Parameter(_idx) => {
                // Parameter types would normally come from param_types
                // This old method doesn't have access to them
                TypeInfo {
                    data_type: DataType::Null,
                    nullable: true,
                    is_aggregate: false,
                    resolution: None,
                }
            }
            Expression::All => TypeInfo {
                data_type: DataType::Bool,
                nullable: false,
                is_aggregate: false,
                resolution: None,
            },
            Expression::Case { .. } => {
                // Case expressions need special handling
                // For now, return a generic type
                TypeInfo {
                    data_type: DataType::Null,
                    nullable: true,
                    is_aggregate: false,
                    resolution: None,
                }
            }
            Expression::ArrayAccess { base, index: _ } => {
                let array_id = expr_id.child(0);
                let array_type = self.infer_expression_type(base, array_id, analyzed)?;

                // Extract element type from array, list, or map
                match array_type.data_type {
                    DataType::Array(elem_type, _size) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true, // Array access can return NULL
                        is_aggregate: array_type.is_aggregate,
                        resolution: None,
                    },
                    DataType::List(elem_type) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true, // List access can return NULL
                        is_aggregate: array_type.is_aggregate,
                        resolution: None,
                    },
                    DataType::Map(_key_type, value_type) => TypeInfo {
                        data_type: *value_type,
                        nullable: true, // Map access can return NULL if key not found
                        is_aggregate: array_type.is_aggregate,
                        resolution: None,
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
                let base_type = self.infer_expression_type(base, base_id, analyzed)?;

                // Extract field type from struct
                match base_type.data_type {
                    DataType::Struct(fields) => {
                        if let Some(field_type) = fields
                            .iter()
                            .find(|(name, _)| name == field)
                            .map(|(_, dtype)| dtype.clone())
                        {
                            TypeInfo {
                                data_type: field_type,
                                nullable: true, // Field access can return NULL
                                is_aggregate: base_type.is_aggregate,
                                resolution: None,
                            }
                        } else {
                            return Err(Error::ExecutionError(format!(
                                "Field '{}' not found in struct",
                                field
                            )));
                        }
                    }
                    _ => {
                        return Err(Error::ExecutionError(
                            "Field access on non-struct type".into(),
                        ));
                    }
                }
            }
            Expression::ArrayLiteral(elements) => {
                if elements.is_empty() {
                    // Empty array defaults to Array<I64>
                    TypeInfo {
                        data_type: DataType::Array(Box::new(DataType::I64), None),
                        nullable: false,
                        is_aggregate: false,
                        resolution: None,
                    }
                } else {
                    // Infer element type from first element
                    let elem_id = expr_id.child(0);
                    let elem_type = self.infer_expression_type(&elements[0], elem_id, analyzed)?;

                    TypeInfo {
                        data_type: DataType::Array(Box::new(elem_type.data_type), None),
                        nullable: false,
                        is_aggregate: elem_type.is_aggregate,
                        resolution: None,
                    }
                }
            }
            Expression::MapLiteral(entries) => {
                if entries.is_empty() {
                    // Empty map defaults to Map<Str, I64>
                    TypeInfo {
                        data_type: DataType::Map(Box::new(DataType::Str), Box::new(DataType::I64)),
                        nullable: false,
                        is_aggregate: false,
                        resolution: None,
                    }
                } else {
                    // Infer types from first entry
                    let (first_key, first_val) = &entries[0];
                    let key_id = expr_id.child(0);
                    let val_id = expr_id.child(1);

                    let key_type = self.infer_expression_type(first_key, key_id, analyzed)?;
                    let val_type = self.infer_expression_type(first_val, val_id, analyzed)?;

                    TypeInfo {
                        data_type: DataType::Map(
                            Box::new(key_type.data_type),
                            Box::new(val_type.data_type),
                        ),
                        nullable: false,
                        is_aggregate: key_type.is_aggregate || val_type.is_aggregate,
                        resolution: None,
                    }
                }
            }
        };

        // Store the type annotation
        analyzed
            .type_annotations
            .annotate(expr_id, type_info.clone());
        Ok(type_info)
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
            resolution: None,
        }
    }

    /// Infer type of an operator expression
    fn infer_operator_type(
        &self,
        op: &Operator,
        expr_id: &ExpressionId,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<TypeInfo> {
        use Operator::*;

        match op {
            // Binary operators
            And(l, r) | Or(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type = self.infer_expression_type(l, left_id, analyzed)?;
                let right_type = self.infer_expression_type(r, right_id, analyzed)?;

                let result_type = match op {
                    And(_, _) => {
                        operators::validate_and(&left_type.data_type, &right_type.data_type)?
                    }
                    Or(_, _) => {
                        operators::validate_or(&left_type.data_type, &right_type.data_type)?
                    }
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Comparison operators
            Equal(l, r)
            | NotEqual(l, r)
            | GreaterThan(l, r)
            | GreaterThanOrEqual(l, r)
            | LessThan(l, r)
            | LessThanOrEqual(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type = self.infer_expression_type(l, left_id, analyzed)?;
                let right_type = self.infer_expression_type(r, right_id, analyzed)?;

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
                    resolution: None,
                })
            }

            // Arithmetic operators
            Add(l, r)
            | Subtract(l, r)
            | Multiply(l, r)
            | Divide(l, r)
            | Remainder(l, r)
            | Exponentiate(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type = self.infer_expression_type(l, left_id, analyzed)?;
                let right_type = self.infer_expression_type(r, right_id, analyzed)?;

                let result_type = match op {
                    Add(_, _) => {
                        operators::validate_add(&left_type.data_type, &right_type.data_type)?
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
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Pattern matching
            Like(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type = self.infer_expression_type(l, left_id, analyzed)?;
                let right_type = self.infer_expression_type(r, right_id, analyzed)?;

                let result_type =
                    operators::validate_like(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Unary operators
            Not(e) => {
                let inner_id = expr_id.child(0);
                let inner_type = self.infer_expression_type(e, inner_id, analyzed)?;
                let result_type = operators::validate_not(&inner_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
                })
            }

            Negate(e) => {
                let inner_id = expr_id.child(0);
                let inner_type = self.infer_expression_type(e, inner_id, analyzed)?;
                let result_type = operators::validate_negate(&inner_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
                })
            }

            Identity(e) => {
                let inner_id = expr_id.child(0);
                let inner_type = self.infer_expression_type(e, inner_id, analyzed)?;
                let result_type = operators::validate_identity(&inner_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
                })
            }

            Factorial(e) => {
                let inner_id = expr_id.child(0);
                let inner_type = self.infer_expression_type(e, inner_id, analyzed)?;
                let result_type = operators::validate_factorial(&inner_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
                })
            }

            // Special operators
            Is(e, _) => {
                let inner_id = expr_id.child(0);
                let inner_type = self.infer_expression_type(e, inner_id, analyzed)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: false, // IS NULL/IS NOT NULL always returns non-null boolean
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
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

                let expr_type = self.infer_expression_type(expr, expr_id_0, analyzed)?;
                let low_type = self.infer_expression_type(low, expr_id_1, analyzed)?;
                let high_type = self.infer_expression_type(high, expr_id_2, analyzed)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable || low_type.nullable || high_type.nullable,
                    is_aggregate: expr_type.is_aggregate
                        || low_type.is_aggregate
                        || high_type.is_aggregate,
                    resolution: None,
                })
            }

            InList {
                expr,
                list,
                negated: _,
            } => {
                let expr_id_0 = expr_id.child(0);
                let expr_type = self.infer_expression_type(expr, expr_id_0, analyzed)?;

                let mut is_aggregate = expr_type.is_aggregate;
                let mut any_nullable = expr_type.nullable;

                for (i, item) in list.iter().enumerate() {
                    let item_id = expr_id.child(i + 1);
                    let item_type = self.infer_expression_type(item, item_id, analyzed)?;
                    is_aggregate = is_aggregate || item_type.is_aggregate;
                    any_nullable = any_nullable || item_type.nullable;
                }

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: any_nullable,
                    is_aggregate,
                    resolution: None,
                })
            }
        }
    }

    /// Infer type of a function call
    fn infer_function_type(
        &self,
        name: &str,
        args: &[Expression],
        expr_id: &ExpressionId,
        analyzed: &mut AnalyzedStatement,
    ) -> Result<TypeInfo> {
        let func_name = name.to_uppercase();

        // Special handling for CAST function
        if func_name == "CAST" {
            // CAST has special syntax: CAST(expr AS type)
            // The type is passed as a second argument that's a type literal
            if args.len() == 2 {
                // First arg is the expression to cast
                let arg_id = expr_id.child(0);
                let arg_type = self.infer_expression_type(&args[0], arg_id, analyzed)?;

                // Second arg should be a literal representing the target type
                // For now, use the functions module validation
                let mut arg_types = Vec::new();
                for (i, arg) in args.iter().enumerate() {
                    let arg_id = expr_id.child(i);
                    let arg_type = self.infer_expression_type(arg, arg_id, analyzed)?;
                    arg_types.push(arg_type.data_type.clone());
                }

                let return_type = functions::validate_function(&func_name, &arg_types)?;

                return Ok(TypeInfo {
                    data_type: return_type,
                    nullable: arg_type.nullable,
                    is_aggregate: arg_type.is_aggregate,
                    resolution: None,
                });
            }
        }

        // Check if it's an aggregate function
        let is_aggregate = functions::is_aggregate(&func_name);

        // Infer argument types
        let mut arg_types = Vec::new();
        let mut any_nullable = false;
        let mut any_aggregate = false;

        for (i, arg) in args.iter().enumerate() {
            let arg_id = expr_id.child(i);
            let arg_type = self.infer_expression_type(arg, arg_id, analyzed)?;
            arg_types.push(arg_type.data_type.clone());
            any_nullable = any_nullable || arg_type.nullable;
            any_aggregate = any_aggregate || arg_type.is_aggregate;
        }

        // Get function return type using validate_function
        let return_type = functions::validate_function(&func_name, &arg_types)?;

        Ok(TypeInfo {
            data_type: return_type,
            nullable: any_nullable,
            is_aggregate: is_aggregate || any_aggregate,
            resolution: None,
        })
    }

    /// Check if a type can be coerced to another
    pub fn can_coerce(&self, from: &DataType, to: &DataType) -> bool {
        coercion::can_coerce(from, to)
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
                    for (idx, (expr, _)) in select.order_by.iter().enumerate() {
                        let expr_id = ExpressionId::from_path(vec![4000 + idx]);
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
                        resolution: Some(ResolvedColumn {
                            table_name: resolution.table_name.clone(),
                            column_name: col.clone(),
                            column_index: resolution.column_index,
                        }),
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
                                    resolution: Some(ResolvedColumn {
                                        table_name: struct_col_resolution.table_name.clone(),
                                        column_name: format!("{}.{}", qualifier, col),
                                        column_index: struct_col_resolution.column_index,
                                    }),
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
                    return Err(Error::ColumnNotFound(col.clone()));
                }
            }

            Expression::Parameter(idx) => TypeInfo {
                data_type: param_types.get(*idx).cloned().unwrap_or(DataType::Null),
                nullable: true,
                is_aggregate: false,
                resolution: None,
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
                resolution: None,
            },

            Expression::Case { .. } => TypeInfo {
                data_type: DataType::Null,
                nullable: true,
                is_aggregate: false,
                resolution: None,
            },

            Expression::ArrayAccess { base, .. } => {
                let base_id = expr_id.child(0);
                let base_type =
                    self.infer_expr_type_new(base, &base_id, column_map, param_types, type_map)?;

                match base_type.data_type {
                    DataType::Array(elem_type, _) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                        resolution: None,
                    },
                    DataType::List(elem_type) => TypeInfo {
                        data_type: *elem_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                        resolution: None,
                    },
                    DataType::Map(_key_type, value_type) => TypeInfo {
                        data_type: *value_type,
                        nullable: true,
                        is_aggregate: base_type.is_aggregate,
                        resolution: None,
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
                                resolution: None,
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
                        resolution: None,
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
                        resolution: None,
                    }
                }
            }

            Expression::MapLiteral(entries) => {
                if entries.is_empty() {
                    TypeInfo {
                        data_type: DataType::Map(Box::new(DataType::Str), Box::new(DataType::I64)),
                        nullable: false,
                        is_aggregate: false,
                        resolution: None,
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
                        resolution: None,
                    }
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
            And(l, r) | Or(l, r) => {
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
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
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
                    resolution: None,
                })
            }

            Add(l, r)
            | Subtract(l, r)
            | Multiply(l, r)
            | Divide(l, r)
            | Remainder(l, r)
            | Exponentiate(l, r) => {
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
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Like(l, r) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);
                let left_type =
                    self.infer_expr_type_new(l, &left_id, column_map, param_types, type_map)?;
                let right_type =
                    self.infer_expr_type_new(r, &right_id, column_map, param_types, type_map)?;

                let result_type =
                    operators::validate_like(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Not(e) | Negate(e) | Identity(e) | Factorial(e) => {
                let inner_id = expr_id.child(0);
                let inner_type =
                    self.infer_expr_type_new(e, &inner_id, column_map, param_types, type_map)?;

                let result_type = match op {
                    Not(_) => operators::validate_not(&inner_type.data_type)?,
                    Negate(_) => operators::validate_negate(&inner_type.data_type)?,
                    Identity(_) => operators::validate_identity(&inner_type.data_type)?,
                    Factorial(_) => operators::validate_factorial(&inner_type.data_type)?,
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: inner_type.nullable,
                    is_aggregate: inner_type.is_aggregate,
                    resolution: None,
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
                    resolution: None,
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
                    resolution: None,
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
                    resolution: None,
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
            resolution: None,
        })
    }
}
