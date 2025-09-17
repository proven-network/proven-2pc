//! Parameter binding and validation
//!
//! This module provides parameter binding without AST mutation,
//! passing parameter values separately through the execution pipeline.

use crate::error::{Error, Result};
use crate::semantic::statement::{AnalyzedStatement, ParameterSlot};
use crate::types::value::Value;

/// Bound parameters ready for execution
#[derive(Debug, Clone)]
pub struct BoundParameters {
    /// Parameter values in order
    pub values: Vec<Value>,

    /// Parameter slots from semantic analysis
    pub slots: Vec<ParameterSlot>,

    /// Whether parameters have been validated
    pub validated: bool,
}

impl BoundParameters {
    /// Create new bound parameters from values and slots
    pub fn new(values: Vec<Value>, slots: Vec<ParameterSlot>) -> Self {
        Self {
            values,
            slots,
            validated: false,
        }
    }

    /// Validate that values match their expected types
    pub fn validate(&mut self) -> Result<()> {
        if self.values.len() != self.slots.len() {
            return Err(Error::ExecutionError(format!(
                "Parameter count mismatch: expected {}, got {}",
                self.slots.len(),
                self.values.len()
            )));
        }

        for (value, slot) in self.values.iter().zip(self.slots.iter()) {
            // Check NULL values
            if matches!(value, Value::Null) && !slot.coercion_context.nullable {
                return Err(Error::TypeMismatch {
                    expected: format!("non-null value for {}", slot.description),
                    found: "NULL".to_string(),
                });
            }

            // Skip further validation for NULL
            if matches!(value, Value::Null) {
                continue;
            }

            // Get the value's type
            let value_type = value.data_type();

            // Check if value can coerce to at least one acceptable type
            if !slot.acceptable_types.is_empty() {
                let can_coerce = slot
                    .acceptable_types
                    .iter()
                    .any(|expected| crate::coercion::can_coerce(&value_type, expected));

                if !can_coerce {
                    return Err(Error::TypeMismatch {
                        expected: format!("{:?} for {}", slot.acceptable_types, slot.description),
                        found: format!("{:?}", value_type),
                    });
                }
            }
        }

        self.validated = true;
        Ok(())
    }

    /// Get a parameter value by index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }
}

/// Create bound parameters from statement metadata and values
pub fn bind_parameters(
    statement: &AnalyzedStatement,
    values: Vec<Value>,
) -> Result<BoundParameters> {
    let slots = statement.parameter_slots.clone();

    let mut bound = BoundParameters::new(values, slots);
    bound.validate()?;

    // Phase 2: Validate functions that have parameters
    validate_functions_with_parameters(statement, &bound)?;

    Ok(bound)
}

/// Validate functions that contain parameters now that we have actual types
///
/// This function reconstructs the complete argument list for each function call
/// that contains parameters, combining both parameter and non-parameter argument types.
fn validate_functions_with_parameters(
    statement: &AnalyzedStatement,
    bound: &BoundParameters,
) -> Result<()> {
    use super::statement::{ExpressionId, SqlContext};
    use crate::types::data_type::DataType;
    use std::collections::{BTreeMap, HashMap};

    // Track all function calls that have parameters
    // Map: (function_name, function_expr_id) -> Map<arg_index, DataType>
    let mut function_calls: HashMap<(String, ExpressionId), BTreeMap<usize, DataType>> =
        HashMap::new();

    // First pass: collect parameter types for functions
    for slot in &statement.parameter_slots {
        if let SqlContext::FunctionArgument {
            ref function_name,
            arg_index,
        } = slot.coercion_context.sql_context
        {
            // Get the actual parameter value type
            let value = bound.values.get(slot.index).ok_or_else(|| {
                Error::ExecutionError(format!("Missing parameter {}", slot.index))
            })?;
            let value_type = value.data_type();

            // Find the function's expression ID (parent of this parameter)
            let func_expr_id =
                if let Some((_, parent_path)) = slot.expression_id.path().split_last() {
                    ExpressionId::from_path(parent_path.to_vec())
                } else {
                    continue; // Skip if we can't find parent
                };

            // Store the parameter type at its argument position
            function_calls
                .entry((function_name.clone(), func_expr_id.clone()))
                .or_default()
                .insert(arg_index, value_type);
        }
    }

    // Second pass: for each function with parameters, get non-parameter arg types from annotations
    for ((function_name, func_expr_id), param_arg_types) in &mut function_calls {
        // Get the function to know how many args it expects
        let func = crate::functions::get_function(function_name)
            .ok_or_else(|| Error::ExecutionError(format!("Unknown function: {}", function_name)))?;

        let sig = func.signature();

        // For each possible argument index, check if we already have it from parameters
        // If not, try to get it from type annotations
        for arg_idx in 0..sig.min_args {
            if !param_arg_types.contains_key(&arg_idx) {
                // This argument is not a parameter, get its type from annotations
                let arg_expr_id = func_expr_id.child(arg_idx);

                if let Some(type_info) = statement.get_type(&arg_expr_id) {
                    // Skip Unknown types - these are likely unresolved references
                    if !matches!(type_info.data_type, DataType::Unknown) {
                        param_arg_types.insert(arg_idx, type_info.data_type.clone());
                    }
                }
            }
        }
    }

    // Final pass: validate each function with complete arg list
    for ((function_name, _), arg_types_map) in function_calls {
        // Get the function
        let func = crate::functions::get_function(&function_name)
            .ok_or_else(|| Error::ExecutionError(format!("Unknown function: {}", function_name)))?;

        // Build complete arg list in order
        let mut arg_types = Vec::new();
        let max_idx = arg_types_map.keys().max().copied().unwrap_or(0);

        for i in 0..=max_idx {
            if let Some(dt) = arg_types_map.get(&i) {
                arg_types.push(dt.clone());
            } else {
                // We're missing type information for this argument
                // This can happen if it's a complex expression we couldn't resolve
                arg_types.push(DataType::Unknown);
            }
        }

        // Only validate if we have enough type information
        // Skip validation if too many Unknown types
        let unknown_count = arg_types
            .iter()
            .filter(|dt| matches!(dt, DataType::Unknown))
            .count();
        if unknown_count == 0 || (arg_types.len() > 1 && unknown_count < arg_types.len() / 2) {
            // Validate with the function's validate method
            func.validate(&arg_types)?;
        }
    }

    Ok(())
}
