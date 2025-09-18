//! Type checker for the new ExpressionId-based architecture
//!
//! This type checker analyzes expressions and statements, creating
//! TypeInfo entries indexed by ExpressionId paths without modifying
//! the AST itself.

use crate::error::{Error, Result};
use crate::operators;
use crate::parsing::ast::{Expression, Literal, Operator};
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::{ExpressionId, TypeAnnotations, TypeInfo};
use crate::types::data_type::DataType;

/// Type checker that produces TypeInfo annotations
///
/// This type checker delegates function validation to the functions module
/// to avoid redundancy and maintain a single source of truth.
pub struct TypeValidator;

impl TypeValidator {
    /// Create a new type checker
    pub fn new() -> Self {
        Self
    }

    /// Check an expression and return its type info
    pub fn check_expression(
        &self,
        expr: &Expression,
        expr_id: &ExpressionId,
        context: &AnalysisContext,
        annotations: &mut TypeAnnotations,
    ) -> Result<TypeInfo> {
        let type_info = match expr {
            Expression::Literal(lit) => self.check_literal(lit, context)?,

            Expression::Column(table, col) => self.check_column(table.as_deref(), col, context)?,

            Expression::Operator(op) => self.check_operator(op, expr_id, context, annotations)?,

            Expression::Function(name, args) => {
                self.check_function(name, args, expr_id, context, annotations)?
            }

            Expression::Parameter(idx) => {
                // Parameters should have their type from context
                // If we don't have parameter types, this is an error
                if let Some(param_type) = context.get_parameter_type(*idx) {
                    TypeInfo {
                        data_type: param_type,
                        nullable: true,
                        is_aggregate: false,
                        resolution: None,
                    }
                } else {
                    return Err(Error::ExecutionError(format!(
                        "Parameter {} type not provided",
                        idx
                    )));
                }
            }

            Expression::All => {
                // All columns wildcard
                TypeInfo {
                    data_type: DataType::Struct(vec![]), // Placeholder
                    nullable: true,
                    is_aggregate: false,
                    resolution: None,
                }
            }

            Expression::Case { .. } => {
                // CASE expressions need special handling
                TypeInfo {
                    data_type: DataType::I64, // Placeholder
                    nullable: true,
                    is_aggregate: false,
                    resolution: None,
                }
            }

            Expression::ArrayAccess { base, index: _ } => {
                // Check the base expression type
                let base_id = expr_id.child(0);
                let base_type = self.check_expression(base, &base_id, context, annotations)?;

                // Determine the result type based on the base type
                let result_type = match &base_type.data_type {
                    DataType::Array(elem_type, _) | DataType::List(elem_type) => {
                        // Array/List access returns the element type
                        (**elem_type).clone()
                    }
                    DataType::Map(_, value_type) => {
                        // Map access returns the value type
                        (**value_type).clone()
                    }
                    DataType::Nullable(inner) => {
                        // Handle nullable collections
                        match &**inner {
                            DataType::Array(elem_type, _) | DataType::List(elem_type) => {
                                DataType::Nullable(elem_type.clone())
                            }
                            DataType::Map(_, value_type) => DataType::Nullable(value_type.clone()),
                            _ => DataType::I64, // Default for unsupported nested types
                        }
                    }
                    _ => DataType::I64, // Default for unsupported base types
                };

                TypeInfo {
                    data_type: result_type,
                    nullable: true, // Array/Map access can return null if index/key not found
                    is_aggregate: base_type.is_aggregate,
                    resolution: None,
                }
            }

            Expression::FieldAccess { base, field: _ } => {
                // Check the base expression type
                let base_id = expr_id.child(0);
                let base_type = self.check_expression(base, &base_id, context, annotations)?;

                // For now, field access returns I64 as placeholder
                // TODO: Implement struct field type resolution
                TypeInfo {
                    data_type: DataType::I64,
                    nullable: true,
                    is_aggregate: base_type.is_aggregate,
                    resolution: None,
                }
            }

            Expression::ArrayLiteral(elements) => {
                // Type check all elements
                let mut element_types = Vec::new();
                let mut any_nullable = false;
                let mut any_aggregate = false;

                for (i, elem) in elements.iter().enumerate() {
                    let elem_id = expr_id.child(i);
                    let elem_type = self.check_expression(elem, &elem_id, context, annotations)?;
                    element_types.push(elem_type.data_type);
                    any_nullable = any_nullable || elem_type.nullable;
                    any_aggregate = any_aggregate || elem_type.is_aggregate;
                }

                // Determine common element type
                let element_type = if element_types.is_empty() {
                    DataType::I64 // Default element type for empty arrays
                } else {
                    // For now, use the first element's type
                    // TODO: Find common type among all elements
                    element_types[0].clone()
                };

                TypeInfo {
                    data_type: DataType::Array(Box::new(element_type), None),
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    resolution: None,
                }
            }

            Expression::MapLiteral(entries) => {
                // Type check all entries
                let mut key_types = Vec::new();
                let mut value_types = Vec::new();
                let mut any_nullable = false;
                let mut any_aggregate = false;

                for (i, (key, value)) in entries.iter().enumerate() {
                    let key_id = expr_id.child(i * 2);
                    let value_id = expr_id.child(i * 2 + 1);

                    let key_type = self.check_expression(key, &key_id, context, annotations)?;
                    let value_type =
                        self.check_expression(value, &value_id, context, annotations)?;

                    key_types.push(key_type.data_type);
                    value_types.push(value_type.data_type);
                    any_nullable = any_nullable || key_type.nullable || value_type.nullable;
                    any_aggregate =
                        any_aggregate || key_type.is_aggregate || value_type.is_aggregate;
                }

                // Determine key and value types
                let key_type = if key_types.is_empty() {
                    DataType::Str // Default key type for empty maps
                } else {
                    key_types[0].clone()
                };
                let value_type = if value_types.is_empty() {
                    DataType::I64 // Default value type for empty maps
                } else {
                    value_types[0].clone()
                };

                TypeInfo {
                    data_type: DataType::Map(Box::new(key_type), Box::new(value_type)),
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    resolution: None,
                }
            }
        };

        // Store the annotation
        annotations.annotate(expr_id.clone(), type_info.clone());

        Ok(type_info)
    }

    /// Check a literal value
    fn check_literal(&self, lit: &Literal, _context: &AnalysisContext) -> Result<TypeInfo> {
        let data_type = match lit {
            Literal::Null => DataType::Null, // NULL is explicitly Null type
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

        Ok(TypeInfo {
            data_type,
            nullable: matches!(lit, Literal::Null),
            is_aggregate: false,
            resolution: None,
        })
    }

    /// Check a column reference
    fn check_column(
        &self,
        table: Option<&str>,
        col: &str,
        context: &AnalysisContext,
    ) -> Result<TypeInfo> {
        // Look up full column info from context
        if let Some((data_type, nullable, table_name, column_index)) =
            context.get_column_info(table, col)
        {
            // Wrap in Nullable if the column is nullable
            let final_type = if nullable {
                DataType::Nullable(Box::new(data_type.clone()))
            } else {
                data_type.clone()
            };

            // Create resolution information
            let resolution = Some(super::super::statement::ResolvedColumn {
                table_name,
                column_name: col.to_string(),
                column_index,
            });

            Ok(TypeInfo {
                data_type: final_type,
                nullable,
                is_aggregate: false,
                resolution,
            })
        } else if let Some(table_ref) = table {
            // If we couldn't find a table with this name, check if it's actually a struct column
            // This handles cases like "details.name" where "details" is a struct column, not a table
            if let Some((struct_type, struct_nullable, _actual_table, _struct_col_idx)) =
                context.get_column_info(None, table_ref)
            {
                // Check if this is a struct type
                match &struct_type {
                    DataType::Struct(fields) => {
                        // Find the field in the struct
                        if let Some(field) = fields.iter().find(|(name, _)| name == col) {
                            let field_type = field.1.clone();

                            // The field is nullable if either the struct is nullable or field allows null
                            let field_nullable = struct_nullable;

                            let final_type = if field_nullable {
                                DataType::Nullable(Box::new(field_type.clone()))
                            } else {
                                field_type.clone()
                            };

                            // Note: For struct field access, we don't have a direct resolution
                            // This would need special handling in the planner
                            Ok(TypeInfo {
                                data_type: final_type,
                                nullable: field_nullable,
                                is_aggregate: false,
                                resolution: None, // Struct field access doesn't map to a direct column
                            })
                        } else {
                            Err(Error::ExecutionError(format!(
                                "Field '{}' not found in struct '{}'",
                                col, table_ref
                            )))
                        }
                    }
                    DataType::Nullable(inner) => {
                        // Unwrap nullable and check if it's a struct
                        if let DataType::Struct(fields) = &**inner {
                            if let Some(field) = fields.iter().find(|(name, _)| name == col) {
                                let field_type = field.1.clone();

                                // Nullable struct means all fields are nullable
                                let final_type = DataType::Nullable(Box::new(field_type));

                                Ok(TypeInfo {
                                    data_type: final_type,
                                    nullable: true,
                                    is_aggregate: false,
                                    resolution: None,
                                })
                            } else {
                                Err(Error::ExecutionError(format!(
                                    "Field '{}' not found in struct '{}'",
                                    col, table_ref
                                )))
                            }
                        } else {
                            Err(Error::ColumnNotFound(col.to_string()))
                        }
                    }
                    _ => {
                        // Not a struct type, so this is genuinely a missing column
                        Err(Error::ColumnNotFound(col.to_string()))
                    }
                }
            } else {
                // Neither a table nor a column was found
                Err(Error::ColumnNotFound(col.to_string()))
            }
        } else {
            // Column not found - return error
            Err(Error::ColumnNotFound(col.to_string()))
        }
    }

    /// Check an operator expression
    fn check_operator(
        &self,
        op: &Operator,
        expr_id: &ExpressionId,
        context: &AnalysisContext,
        annotations: &mut TypeAnnotations,
    ) -> Result<TypeInfo> {
        match op {
            // Logical operators return boolean
            Operator::And(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Validate using operators module
                let result_type =
                    operators::validate_and(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Or(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Validate using operators module
                let result_type =
                    operators::validate_or(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Comparison operators return boolean
            Operator::Equal(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_equal(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::NotEqual(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_not_equal(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::LessThan(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_less_than(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::LessThanOrEqual(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type = operators::validate_less_than_equal(
                    &left_type.data_type,
                    &right_type.data_type,
                )?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::GreaterThan(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_greater_than(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::GreaterThanOrEqual(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type = operators::validate_greater_than_equal(
                    &left_type.data_type,
                    &right_type.data_type,
                )?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Arithmetic operators
            Operator::Add(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Validate using operators module
                let result_type =
                    operators::validate_add(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Subtract(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Validate using operators module
                let result_type =
                    operators::validate_subtract(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Multiply(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_multiply(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Divide(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_divide(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Remainder(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_remainder(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Exponentiate(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_exponentiate(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // Unary operators
            Operator::Not(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                let result_type = operators::validate_not(&expr_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Negate(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                let result_type = operators::validate_negate(&expr_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Identity(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                let result_type = operators::validate_identity(&expr_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    resolution: None,
                })
            }

            Operator::Factorial(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                let result_type = operators::validate_factorial(&expr_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    resolution: None,
                })
            }

            // IS NULL/IS NOT NULL
            Operator::Is(expr, _) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: false, // IS NULL/IS NOT NULL always returns non-null boolean
                    is_aggregate: expr_type.is_aggregate,
                    resolution: None,
                })
            }

            // LIKE operator
            Operator::Like(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    operators::validate_like(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    resolution: None,
                })
            }

            // IN list
            Operator::InList { expr, list, .. } => {
                let expr_id_child = expr_id.child(0);
                let expr_type =
                    self.check_expression(expr, &expr_id_child, context, annotations)?;

                let mut any_nullable = expr_type.nullable;
                let mut any_aggregate = expr_type.is_aggregate;
                for (i, item) in list.iter().enumerate() {
                    let item_id = expr_id.child(i + 1);
                    let item_type = self.check_expression(item, &item_id, context, annotations)?;
                    any_nullable = any_nullable || item_type.nullable;
                    any_aggregate = any_aggregate || item_type.is_aggregate;
                }

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    resolution: None,
                })
            }

            // BETWEEN
            Operator::Between {
                expr, low, high, ..
            } => {
                let expr_id_child = expr_id.child(0);
                let low_id = expr_id.child(1);
                let high_id = expr_id.child(2);

                let expr_type =
                    self.check_expression(expr, &expr_id_child, context, annotations)?;
                let low_type = self.check_expression(low, &low_id, context, annotations)?;
                let high_type = self.check_expression(high, &high_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable || low_type.nullable || high_type.nullable,
                    is_aggregate: expr_type.is_aggregate
                        || low_type.is_aggregate
                        || high_type.is_aggregate,
                    resolution: None,
                })
            }
        }
    }

    /// Check a function call
    ///
    /// This delegates to the functions module for validation to maintain
    /// a single source of truth for function signatures and type checking.
    fn check_function(
        &self,
        name: &str,
        args: &[Expression],
        expr_id: &ExpressionId,
        context: &AnalysisContext,
        annotations: &mut TypeAnnotations,
    ) -> Result<TypeInfo> {
        // Get function from the registry
        let func = crate::functions::get_function(name)
            .ok_or_else(|| Error::ExecutionError(format!("Unknown function: {}", name)))?;

        let signature = func.signature();
        let is_aggregate = signature.is_aggregate;

        // Type check arguments
        let mut arg_types = Vec::new();
        let mut any_nullable = false;
        let mut any_aggregate = false;

        for (i, arg) in args.iter().enumerate() {
            // Check if this argument is a parameter
            if let Expression::Parameter(idx) = arg {
                // For parameters, get type from context
                if let Some(param_type) = context.get_parameter_type(*idx) {
                    arg_types.push(param_type);
                } else {
                    return Err(Error::ExecutionError(format!(
                        "Parameter {} type not provided for function {}",
                        idx, name
                    )));
                }
            } else {
                let arg_id = expr_id.child(i);
                let arg_type = self.check_expression(arg, &arg_id, context, annotations)?;

                arg_types.push(arg_type.data_type.clone());
                any_nullable = any_nullable || arg_type.nullable;
                any_aggregate = any_aggregate || arg_type.is_aggregate;
            }
        }

        // Check for nested aggregate functions - not allowed in SQL
        if is_aggregate && any_aggregate {
            return Err(Error::ExecutionError(format!(
                "Cannot nest aggregate function '{}' inside another aggregate function",
                name.to_uppercase()
            )));
        }

        // Get the return type from the function
        // Now we always have concrete types, so we can validate
        let return_type = func.validate(&arg_types)?;

        Ok(TypeInfo {
            data_type: return_type,
            nullable: any_nullable,
            is_aggregate: is_aggregate || any_aggregate,
            resolution: None,
        })
    }
}
