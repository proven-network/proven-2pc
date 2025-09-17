//! Type checker for the new ExpressionId-based architecture
//!
//! This type checker analyzes expressions and statements, creating
//! TypeInfo entries indexed by ExpressionId paths without modifying
//! the AST itself.

use super::context::AnalysisContext;
use super::statement::{ExpressionId, TypeAnnotations, TypeInfo};
use crate::error::{Error, Result};
use crate::parsing::ast::{Expression, Literal, Operator};
use crate::types::data_type::DataType;

/// Type checker that produces TypeInfo annotations
///
/// This type checker delegates function validation to the functions module
/// to avoid redundancy and maintain a single source of truth.
pub struct TypeChecker;

impl TypeChecker {
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
            Expression::Literal(lit) => self.check_literal(lit),

            Expression::Column(table, col) => self.check_column(table.as_deref(), col, context)?,

            Expression::Operator(op) => self.check_operator(op, expr_id, context, annotations)?,

            Expression::Function(name, args) => {
                self.check_function(name, args, expr_id, context, annotations)?
            }

            Expression::Parameter(_idx) => {
                // Parameters get their type from context later
                TypeInfo {
                    data_type: DataType::Unknown,
                    nullable: true,
                    is_aggregate: false,
                    is_deterministic: false,
                }
            }

            Expression::All => {
                // All columns wildcard
                TypeInfo {
                    data_type: DataType::Struct(vec![]), // Placeholder
                    nullable: true,
                    is_aggregate: false,
                    is_deterministic: true,
                }
            }

            Expression::Case { .. } => {
                // CASE expressions need special handling
                TypeInfo {
                    data_type: DataType::I64, // Placeholder
                    nullable: true,
                    is_aggregate: false,
                    is_deterministic: true,
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
                            _ => DataType::Unknown,
                        }
                    }
                    _ => DataType::Unknown,
                };

                TypeInfo {
                    data_type: result_type,
                    nullable: true, // Array/Map access can return null if index/key not found
                    is_aggregate: base_type.is_aggregate,
                    is_deterministic: base_type.is_deterministic,
                }
            }

            Expression::FieldAccess { base, field: _ } => {
                // Check the base expression type
                let base_id = expr_id.child(0);
                let base_type = self.check_expression(base, &base_id, context, annotations)?;

                // For now, field access returns Unknown type
                // TODO: Implement struct field type resolution
                TypeInfo {
                    data_type: DataType::Unknown,
                    nullable: true,
                    is_aggregate: base_type.is_aggregate,
                    is_deterministic: base_type.is_deterministic,
                }
            }

            Expression::ArrayLiteral(elements) => {
                // Type check all elements
                let mut element_types = Vec::new();
                let mut any_nullable = false;
                let mut any_aggregate = false;
                let mut all_deterministic = true;

                for (i, elem) in elements.iter().enumerate() {
                    let elem_id = expr_id.child(i);
                    let elem_type = self.check_expression(elem, &elem_id, context, annotations)?;
                    element_types.push(elem_type.data_type);
                    any_nullable = any_nullable || elem_type.nullable;
                    any_aggregate = any_aggregate || elem_type.is_aggregate;
                    all_deterministic = all_deterministic && elem_type.is_deterministic;
                }

                // Determine common element type
                let element_type = if element_types.is_empty() {
                    DataType::Unknown
                } else {
                    // For now, use the first element's type
                    // TODO: Find common type among all elements
                    element_types[0].clone()
                };

                TypeInfo {
                    data_type: DataType::Array(Box::new(element_type), None),
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    is_deterministic: all_deterministic,
                }
            }

            Expression::MapLiteral(entries) => {
                // Type check all entries
                let mut key_types = Vec::new();
                let mut value_types = Vec::new();
                let mut any_nullable = false;
                let mut any_aggregate = false;
                let mut all_deterministic = true;

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
                    all_deterministic = all_deterministic
                        && key_type.is_deterministic
                        && value_type.is_deterministic;
                }

                // Determine key and value types
                let key_type = if key_types.is_empty() {
                    DataType::Unknown
                } else {
                    key_types[0].clone()
                };
                let value_type = if value_types.is_empty() {
                    DataType::Unknown
                } else {
                    value_types[0].clone()
                };

                TypeInfo {
                    data_type: DataType::Map(Box::new(key_type), Box::new(value_type)),
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    is_deterministic: all_deterministic,
                }
            }
        };

        // Store the annotation
        annotations.annotate(expr_id.clone(), type_info.clone());

        Ok(type_info)
    }

    /// Check a literal value
    fn check_literal(&self, lit: &Literal) -> TypeInfo {
        let data_type = match lit {
            Literal::Null => DataType::Nullable(Box::new(DataType::I64)),
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
            is_deterministic: true,
        }
    }

    /// Check a column reference
    fn check_column(
        &self,
        table: Option<&str>,
        col: &str,
        context: &AnalysisContext,
    ) -> Result<TypeInfo> {
        // Look up column type from context
        if let Some((data_type, nullable)) = context.get_column_type(table, col) {
            // Wrap in Nullable if the column is nullable
            let final_type = if nullable {
                DataType::Nullable(Box::new(data_type))
            } else {
                data_type
            };

            Ok(TypeInfo {
                data_type: final_type,
                nullable,
                is_aggregate: false,
                is_deterministic: true,
            })
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
            Operator::And(left, right) | Operator::Or(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            // Comparison operators return boolean
            Operator::Equal(left, right)
            | Operator::NotEqual(left, right)
            | Operator::LessThan(left, right)
            | Operator::LessThanOrEqual(left, right)
            | Operator::GreaterThan(left, right)
            | Operator::GreaterThanOrEqual(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            // Arithmetic operators
            Operator::Add(left, right) | Operator::Subtract(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Handle date/time arithmetic specially
                let result_type = if matches!(op, Operator::Add(_, _) | Operator::Subtract(_, _)) {
                    self.check_datetime_arithmetic(
                        &left_type.data_type,
                        &right_type.data_type,
                        matches!(op, Operator::Subtract(_, _)),
                    )
                    .or_else(|_| {
                        // Fall back to numeric promotion if not date/time types
                        self.promote_numeric_types(&left_type.data_type, &right_type.data_type)
                    })?
                } else {
                    self.promote_numeric_types(&left_type.data_type, &right_type.data_type)?
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            Operator::Multiply(left, right)
            | Operator::Divide(left, right)
            | Operator::Remainder(left, right)
            | Operator::Exponentiate(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // These operators don't apply to date/time types
                let result_type =
                    self.promote_numeric_types(&left_type.data_type, &right_type.data_type)?;

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            // Unary operators
            Operator::Not(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    is_deterministic: expr_type.is_deterministic,
                })
            }

            Operator::Negate(expr) | Operator::Identity(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                Ok(expr_type)
            }

            Operator::Factorial(expr) => {
                let expr_id = expr_id.child(0);
                let expr_type = self.check_expression(expr, &expr_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::I64, // Factorial always returns integer
                    nullable: expr_type.nullable,
                    is_aggregate: expr_type.is_aggregate,
                    is_deterministic: expr_type.is_deterministic,
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
                    is_deterministic: expr_type.is_deterministic,
                })
            }

            // LIKE operator
            Operator::Like(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            // IN list
            Operator::InList { expr, list, .. } => {
                let expr_id_child = expr_id.child(0);
                let expr_type =
                    self.check_expression(expr, &expr_id_child, context, annotations)?;

                let mut any_nullable = expr_type.nullable;
                let mut any_aggregate = expr_type.is_aggregate;
                let mut all_deterministic = expr_type.is_deterministic;

                for (i, item) in list.iter().enumerate() {
                    let item_id = expr_id.child(i + 1);
                    let item_type = self.check_expression(item, &item_id, context, annotations)?;
                    any_nullable = any_nullable || item_type.nullable;
                    any_aggregate = any_aggregate || item_type.is_aggregate;
                    all_deterministic = all_deterministic && item_type.is_deterministic;
                }

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: any_nullable,
                    is_aggregate: any_aggregate,
                    is_deterministic: all_deterministic,
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
                    is_deterministic: expr_type.is_deterministic
                        && low_type.is_deterministic
                        && high_type.is_deterministic,
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
        let mut all_deterministic = true;
        let mut has_parameters = false;

        for (i, arg) in args.iter().enumerate() {
            // Check if this argument is a parameter
            if matches!(arg, Expression::Parameter(_)) {
                has_parameters = true;
                // For parameters, we use Unknown type and validate later
                arg_types.push(DataType::Unknown);
            } else {
                let arg_id = expr_id.child(i);
                let arg_type = self.check_expression(arg, &arg_id, context, annotations)?;

                arg_types.push(arg_type.data_type.clone());
                any_nullable = any_nullable || arg_type.nullable;
                any_aggregate = any_aggregate || arg_type.is_aggregate;
                all_deterministic = all_deterministic && arg_type.is_deterministic;
            }
        }

        // Get the return type from the function
        // If there are parameters, we can't validate yet - use Unknown
        let return_type = if has_parameters {
            DataType::Unknown
        } else {
            // Validate and get return type from the function itself
            func.validate(&arg_types)?
        };

        // Check if this is a non-deterministic function
        let is_deterministic = all_deterministic
            && !matches!(
                name.to_lowercase().as_str(),
                "random" | "rand" | "now" | "current_timestamp" | "current_date" | "current_time"
            );

        Ok(TypeInfo {
            data_type: return_type,
            nullable: any_nullable,
            is_aggregate: is_aggregate || any_aggregate,
            is_deterministic,
        })
    }

    /// Check date/time arithmetic operations
    fn check_datetime_arithmetic(
        &self,
        left: &DataType,
        right: &DataType,
        is_subtract: bool,
    ) -> Result<DataType> {
        use DataType::*;

        // Handle nullable types
        let (left_inner, left_nullable) = match left {
            Nullable(inner) => (&**inner, true),
            other => (other, false),
        };

        let (right_inner, right_nullable) = match right {
            Nullable(inner) => (&**inner, true),
            other => (other, false),
        };

        let result = match (left_inner, right_inner, is_subtract) {
            // Date arithmetic
            (Date, Date, true) => I32, // Date - Date = days
            (Date, I32 | I64, _) | (I32 | I64, Date, false) => Date, // Date ± integer days
            (Date, Interval, _) => Date, // Date ± Interval
            (Interval, Date, false) => Date, // Interval + Date

            // Timestamp arithmetic
            (Timestamp, Timestamp, true) => Interval, // Timestamp - Timestamp
            (Timestamp, Interval, _) => Timestamp,    // Timestamp ± Interval
            (Interval, Timestamp, false) => Timestamp, // Interval + Timestamp

            // Time arithmetic
            (Time, Time, true) => Interval,  // Time - Time
            (Time, Interval, _) => Time,     // Time ± Interval
            (Interval, Time, false) => Time, // Interval + Time

            // Interval arithmetic
            (Interval, Interval, false) => Interval, // Interval + Interval
            (Interval, Interval, true) => Interval,  // Interval - Interval

            // No match - not valid date/time arithmetic
            _ => {
                return Err(Error::TypeMismatch {
                    expected: format!("date/time arithmetic operands"),
                    found: format!("{:?} and {:?}", left, right),
                });
            }
        };

        // Wrap in nullable if either operand was nullable
        Ok(if left_nullable || right_nullable {
            Nullable(Box::new(result))
        } else {
            result
        })
    }

    /// Promote numeric types to a common type
    fn promote_numeric_types(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        // Handle nullable types
        let (left_inner, left_nullable) = match left {
            Nullable(inner) => (&**inner, true),
            other => (other, false),
        };

        let (right_inner, right_nullable) = match right {
            Nullable(inner) => (&**inner, true),
            other => (other, false),
        };

        let promoted = match (left_inner, right_inner) {
            // Same types
            (I8, I8) => I8,
            (I16, I16) => I16,
            (I32, I32) => I32,
            (I64, I64) => I64,
            (I128, I128) => I128,
            (U8, U8) => U8,
            (U16, U16) => U16,
            (U32, U32) => U32,
            (U64, U64) => U64,
            (U128, U128) => U128,
            (F32, F32) => F32,
            (F64, F64) => F64,
            (Decimal(p1, s1), Decimal(p2, s2)) => {
                // For decimal arithmetic, use the larger precision/scale
                let precision = match (p1, p2) {
                    (Some(p1), Some(p2)) => Some((*p1).max(*p2)),
                    (Some(p), None) | (None, Some(p)) => Some(*p),
                    (None, None) => None,
                };
                let scale = match (s1, s2) {
                    (Some(s1), Some(s2)) => Some((*s1).max(*s2)),
                    (Some(s), None) | (None, Some(s)) => Some(*s),
                    (None, None) => None,
                };
                Decimal(precision, scale)
            }

            // Signed integer promotions
            (I8, I16) | (I16, I8) => I16,
            (I8, I32) | (I32, I8) | (I16, I32) | (I32, I16) => I32,
            (I8, I64) | (I64, I8) | (I16, I64) | (I64, I16) | (I32, I64) | (I64, I32) => I64,
            (_, I128) | (I128, _) => I128,

            // Unsigned integer promotions
            (U8, U16) | (U16, U8) => U16,
            (U8, U32) | (U32, U8) | (U16, U32) | (U32, U16) => U32,
            (U8, U64) | (U64, U8) | (U16, U64) | (U64, U16) | (U32, U64) | (U64, U32) => U64,
            (U8, U128)
            | (U128, U8)
            | (U16, U128)
            | (U128, U16)
            | (U32, U128)
            | (U128, U32)
            | (U64, U128)
            | (U128, U64) => U128,

            // Mixed signed/unsigned promotions (promote to larger signed type to avoid overflow)
            (U8, I8) | (I8, U8) => I16, // U8 max (255) > I8 max (127), need I16
            (U8, I16) | (I16, U8) => I16,
            (U8, I32) | (I32, U8) | (U16, I32) | (I32, U16) => I32,
            (U32, I32) | (I32, U32) => I64, // U32 max > I32 max, need I64
            (U8, I64) | (I64, U8) | (U16, I64) | (I64, U16) | (U32, I64) | (I64, U32) => I64,
            (U64, I64) | (I64, U64) => I128, // U64 max > I64 max, need I128
            (U8, I128)
            | (I128, U8)
            | (U16, I128)
            | (I128, U16)
            | (U32, I128)
            | (I128, U32)
            | (U64, I128)
            | (I128, U64) => I128,

            // Float promotions
            (F32, F64) | (F64, F32) => F64,

            // Integer to float
            (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64, F32)
            | (F32, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64) => F32,
            (I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128, F64)
            | (F64, I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128) => F64,

            // Decimal with integer/float
            (Decimal(p, s), I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128)
            | (I8 | I16 | I32 | I64 | I128 | U8 | U16 | U32 | U64 | U128, Decimal(p, s)) => {
                // Integer promotes to decimal
                Decimal(*p, *s)
            }
            (Decimal(_, _), F32 | F64) | (F32 | F64, Decimal(_, _)) => {
                // Decimal with float promotes to float (loss of precision warning would be good)
                F64
            }

            // Unknown type propagates
            (Unknown, _) | (_, Unknown) => Unknown,

            _ => {
                return Err(Error::TypeMismatch {
                    expected: format!("{:?}", left),
                    found: format!("{:?}", right),
                });
            }
        };

        // Wrap in nullable if either operand was nullable
        Ok(if left_nullable || right_nullable {
            Nullable(Box::new(promoted))
        } else {
            promoted
        })
    }
}
