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

use std::collections::HashMap;

/// Type checker that produces TypeInfo annotations
pub struct TypeChecker {
    /// Function signatures for built-in functions
    function_signatures: HashMap<String, FunctionSignature>,
}

/// Function signature definition
#[derive(Clone, Debug)]
struct FunctionSignature {
    /// Parameter types (None means any type)
    params: Vec<Option<DataType>>,
    /// Whether function accepts variable args
    variadic: bool,
    /// Return type calculator
    return_type: ReturnTypeCalculator,
}

/// How to calculate return type
#[derive(Clone, Debug)]
enum ReturnTypeCalculator {
    /// Fixed return type
    Fixed(DataType),
    /// Same as first argument
    FirstArg,
    /// Promote numeric arguments to common type
    NumericPromotion,
    /// Custom logic (placeholder)
    Custom,
}

impl TypeChecker {
    /// Create a new type checker
    pub fn new() -> Self {
        let mut function_signatures = HashMap::new();

        // Register common SQL functions
        function_signatures.insert(
            "count".to_string(),
            FunctionSignature {
                params: vec![None], // COUNT(*) or COUNT(expr)
                variadic: false,
                return_type: ReturnTypeCalculator::Fixed(DataType::I64),
            },
        );

        function_signatures.insert(
            "sum".to_string(),
            FunctionSignature {
                params: vec![None], // SUM(numeric_expr)
                variadic: false,
                return_type: ReturnTypeCalculator::FirstArg,
            },
        );

        function_signatures.insert(
            "avg".to_string(),
            FunctionSignature {
                params: vec![None],
                variadic: false,
                return_type: ReturnTypeCalculator::Fixed(DataType::F64),
            },
        );

        function_signatures.insert(
            "max".to_string(),
            FunctionSignature {
                params: vec![None],
                variadic: false,
                return_type: ReturnTypeCalculator::FirstArg,
            },
        );

        function_signatures.insert(
            "min".to_string(),
            FunctionSignature {
                params: vec![None],
                variadic: false,
                return_type: ReturnTypeCalculator::FirstArg,
            },
        );

        function_signatures.insert(
            "coalesce".to_string(),
            FunctionSignature {
                params: vec![],
                variadic: true,
                return_type: ReturnTypeCalculator::FirstArg,
            },
        );

        Self {
            function_signatures,
        }
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

            Expression::FieldAccess { base, field } => {
                // Check the base expression type
                let base_id = expr_id.child(0);
                let base_type = self.check_expression(base, &base_id, context, annotations)?;

                // Determine the result type based on the struct field
                let result_type = match &base_type.data_type {
                    DataType::Struct(fields) => {
                        // Find the field type
                        fields
                            .iter()
                            .find(|(name, _)| name == field)
                            .map(|(_, dtype)| dtype.clone())
                            .unwrap_or(DataType::Unknown)
                    }
                    DataType::Nullable(inner) => match &**inner {
                        DataType::Struct(fields) => fields
                            .iter()
                            .find(|(name, _)| name == field)
                            .map(|(_, dtype)| DataType::Nullable(Box::new(dtype.clone())))
                            .unwrap_or(DataType::Unknown),
                        _ => DataType::Unknown,
                    },
                    _ => DataType::Unknown,
                };

                TypeInfo {
                    data_type: result_type,
                    nullable: base_type.nullable,
                    is_aggregate: base_type.is_aggregate,
                    is_deterministic: base_type.is_deterministic,
                }
            }

            Expression::ArrayLiteral(elements) => {
                // Infer array type from elements
                if elements.is_empty() {
                    // Empty array - type unknown
                    TypeInfo {
                        data_type: DataType::Array(Box::new(DataType::Unknown), None),
                        nullable: false,
                        is_aggregate: false,
                        is_deterministic: true,
                    }
                } else {
                    // Check all element types
                    let mut elem_types = Vec::new();
                    let mut any_aggregate = false;
                    let mut all_deterministic = true;

                    for (i, elem) in elements.iter().enumerate() {
                        let elem_id = expr_id.child(i);
                        let elem_type =
                            self.check_expression(elem, &elem_id, context, annotations)?;
                        elem_types.push(elem_type.data_type.clone());
                        any_aggregate = any_aggregate || elem_type.is_aggregate;
                        all_deterministic = all_deterministic && elem_type.is_deterministic;
                    }

                    // Find common type (simplified - just use first non-Unknown type)
                    let elem_type = elem_types
                        .iter()
                        .find(|t| !matches!(t, DataType::Unknown))
                        .cloned()
                        .unwrap_or(DataType::Unknown);

                    TypeInfo {
                        data_type: DataType::Array(Box::new(elem_type), None),
                        nullable: false,
                        is_aggregate: any_aggregate,
                        is_deterministic: all_deterministic,
                    }
                }
            }

            Expression::MapLiteral(entries) => {
                // Infer map type from entries
                if entries.is_empty() {
                    // Empty map - types unknown
                    TypeInfo {
                        data_type: DataType::Map(
                            Box::new(DataType::Unknown),
                            Box::new(DataType::Unknown),
                        ),
                        nullable: false,
                        is_aggregate: false,
                        is_deterministic: true,
                    }
                } else {
                    // Check all key and value types
                    let mut key_types = Vec::new();
                    let mut value_types = Vec::new();
                    let mut any_aggregate = false;
                    let mut all_deterministic = true;

                    for (i, (key_expr, value_expr)) in entries.iter().enumerate() {
                        // Check key
                        let key_id = expr_id.child(i * 2);
                        let key_type =
                            self.check_expression(key_expr, &key_id, context, annotations)?;
                        key_types.push(key_type.data_type.clone());
                        any_aggregate = any_aggregate || key_type.is_aggregate;
                        all_deterministic = all_deterministic && key_type.is_deterministic;

                        // Check value
                        let value_id = expr_id.child(i * 2 + 1);
                        let value_type =
                            self.check_expression(value_expr, &value_id, context, annotations)?;
                        value_types.push(value_type.data_type.clone());
                        any_aggregate = any_aggregate || value_type.is_aggregate;
                        all_deterministic = all_deterministic && value_type.is_deterministic;
                    }

                    // Find common types (simplified - just use first non-Unknown types)
                    let key_type = key_types
                        .iter()
                        .find(|t| !matches!(t, DataType::Unknown))
                        .cloned()
                        .unwrap_or(DataType::Unknown);

                    let value_type = value_types
                        .iter()
                        .find(|t| !matches!(t, DataType::Unknown))
                        .cloned()
                        .unwrap_or(DataType::Unknown);

                    TypeInfo {
                        data_type: DataType::Map(Box::new(key_type), Box::new(value_type)),
                        nullable: false,
                        is_aggregate: any_aggregate,
                        is_deterministic: all_deterministic,
                    }
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
            Operator::Add(left, right)
            | Operator::Subtract(left, right)
            | Operator::Multiply(left, right)
            | Operator::Divide(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                // Promote to common numeric type
                let result_type =
                    self.promote_numeric_types(&left_type.data_type, &right_type.data_type);

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }

            // Unary operators
            Operator::Not(expr) | Operator::Negate(expr) => {
                let child_id = expr_id.child(0);
                let child_type = self.check_expression(expr, &child_id, context, annotations)?;

                let result_type = match op {
                    Operator::Not(_) => DataType::Bool,
                    Operator::Negate(_) => child_type.data_type.clone(),
                    _ => unreachable!(),
                };

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: child_type.nullable,
                    is_aggregate: child_type.is_aggregate,
                    is_deterministic: child_type.is_deterministic,
                })
            }

            // IN/NOT IN operators
            Operator::InList {
                expr,
                list,
                negated: _,
            } => {
                let expr_id_child = expr_id.child(0);
                let expr_type =
                    self.check_expression(expr, &expr_id_child, context, annotations)?;

                // Check all list items
                let mut is_aggregate = expr_type.is_aggregate;
                let mut is_deterministic = expr_type.is_deterministic;

                for (i, item) in list.iter().enumerate() {
                    let item_id = expr_id.child(i + 1);
                    let item_type = self.check_expression(item, &item_id, context, annotations)?;
                    is_aggregate = is_aggregate || item_type.is_aggregate;
                    is_deterministic = is_deterministic && item_type.is_deterministic;
                }

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable,
                    is_aggregate,
                    is_deterministic,
                })
            }

            // BETWEEN operator
            Operator::Between {
                expr,
                low,
                high,
                negated: _,
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

            // IS operator (for IS NULL, IS NOT NULL)
            Operator::Is(expr, _lit) => {
                let child_id = expr_id.child(0);
                let child_type = self.check_expression(expr, &child_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: false, // IS NULL/IS NOT NULL always returns non-null boolean
                    is_aggregate: child_type.is_aggregate,
                    is_deterministic: child_type.is_deterministic,
                })
            }

            // LIKE operator
            Operator::Like(expr, pattern) => {
                let expr_id_child = expr_id.child(0);
                let pattern_id = expr_id.child(1);

                let expr_type =
                    self.check_expression(expr, &expr_id_child, context, annotations)?;
                let pattern_type =
                    self.check_expression(pattern, &pattern_id, context, annotations)?;

                Ok(TypeInfo {
                    data_type: DataType::Bool,
                    nullable: expr_type.nullable || pattern_type.nullable,
                    is_aggregate: expr_type.is_aggregate || pattern_type.is_aggregate,
                    is_deterministic: expr_type.is_deterministic && pattern_type.is_deterministic,
                })
            }

            // Identity and Factorial operators
            Operator::Identity(expr) => {
                let child_id = expr_id.child(0);
                self.check_expression(expr, &child_id, context, annotations)
            }
            Operator::Factorial(expr) => {
                let child_id = expr_id.child(0);
                let child_type = self.check_expression(expr, &child_id, context, annotations)?;
                Ok(TypeInfo {
                    data_type: DataType::I64, // Factorial returns integer
                    nullable: child_type.nullable,
                    is_aggregate: child_type.is_aggregate,
                    is_deterministic: child_type.is_deterministic,
                })
            }
            Operator::Remainder(left, right) | Operator::Exponentiate(left, right) => {
                let left_id = expr_id.child(0);
                let right_id = expr_id.child(1);

                let left_type = self.check_expression(left, &left_id, context, annotations)?;
                let right_type = self.check_expression(right, &right_id, context, annotations)?;

                let result_type =
                    self.promote_numeric_types(&left_type.data_type, &right_type.data_type);

                Ok(TypeInfo {
                    data_type: result_type,
                    nullable: left_type.nullable || right_type.nullable,
                    is_aggregate: left_type.is_aggregate || right_type.is_aggregate,
                    is_deterministic: left_type.is_deterministic && right_type.is_deterministic,
                })
            }
        }
    }

    /// Check a function call
    fn check_function(
        &self,
        name: &str,
        args: &[Expression],
        expr_id: &ExpressionId,
        context: &AnalysisContext,
        annotations: &mut TypeAnnotations,
    ) -> Result<TypeInfo> {
        // Check if it's an aggregate function from the function registry
        let is_aggregate = if let Some(func) = crate::functions::get_function(name) {
            func.signature().is_aggregate
        } else {
            false
        };

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
            }

            let arg_id = expr_id.child(i);
            let arg_type = self.check_expression(arg, &arg_id, context, annotations)?;

            arg_types.push(arg_type.data_type.clone());
            any_nullable = any_nullable || arg_type.nullable;
            any_aggregate = any_aggregate || arg_type.is_aggregate;
            all_deterministic = all_deterministic && arg_type.is_deterministic;
        }

        // Get the return type
        let return_type = if let Some(func) = crate::functions::get_function(name) {
            if has_parameters {
                // Skip validation for now - will validate at bind time
                // Use Unknown type as placeholder
                // The actual type will be determined during parameter binding
                DataType::Unknown
            } else {
                // No parameters - validate normally
                func.validate(&arg_types)?
            }
        } else {
            // Unknown function - this should have been caught earlier
            // but let's return an error here too
            return Err(Error::ExecutionError(format!(
                "Unknown function: {}",
                name.to_uppercase()
            )));
        };

        // Some functions are non-deterministic
        let is_deterministic = all_deterministic
            && !matches!(
                name.to_lowercase().as_str(),
                "random" | "uuid" | "now" | "current_timestamp" | "current_date"
            );

        Ok(TypeInfo {
            data_type: return_type,
            nullable: any_nullable,
            is_aggregate: is_aggregate || any_aggregate,
            is_deterministic,
        })
    }

    /// Promote two numeric types to their common type
    fn promote_numeric_types(&self, left: &DataType, right: &DataType) -> DataType {
        use DataType::*;

        match (left, right) {
            // If either is float/double, promote to floating point
            (F64, _) | (_, F64) => F64,
            (F32, _) | (_, F32) => F32,

            // For integers, promote to larger size
            (I128, _) | (_, I128) => I128,
            (I64, _) | (_, I64) => I64,
            (I32, _) | (_, I32) => I32,
            (I16, _) | (_, I16) => I16,
            (I8, _) | (_, I8) => I8,

            // Default to I64 for unknown types
            _ => I64,
        }
    }
}
