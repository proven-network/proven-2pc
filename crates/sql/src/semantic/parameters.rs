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
fn validate_functions_with_parameters(
    statement: &AnalyzedStatement,
    bound: &BoundParameters,
) -> Result<()> {
    use super::statement::{ExpressionId, SqlContext};
    use crate::types::data_type::DataType;
    use std::collections::HashMap;

    // Group parameters by their function context
    let mut function_params: HashMap<(String, ExpressionId), Vec<(usize, DataType)>> =
        HashMap::new();

    for slot in &statement.parameter_slots {
        if let SqlContext::FunctionArgument {
            ref function_name,
            arg_index,
        } = slot.coercion_context.sql_context
        {
            // Get the actual value type for this parameter
            let value = bound.values.get(slot.index).ok_or_else(|| {
                Error::ExecutionError(format!("Missing parameter {}", slot.index))
            })?;
            let value_type = value.data_type();

            // Find the function's expression ID (parent of the parameter)
            // The parameter's expression_id has the arg_index as the last element
            let func_expr_id = slot.expression_id.clone();
            if let Some(path) = func_expr_id.path().split_last() {
                let parent_id = ExpressionId::from_path(path.1.to_vec());

                function_params
                    .entry((function_name.clone(), parent_id))
                    .or_default()
                    .push((arg_index, value_type));
            }
        }
    }

    // Now validate each function that had parameters
    for ((function_name, _expr_id), param_args) in function_params {
        // Get the function
        let func = crate::functions::get_function(&function_name)
            .ok_or_else(|| Error::ExecutionError(format!("Unknown function: {}", function_name)))?;

        // We need to reconstruct the complete argument list
        // The param_args only contains parameters, but the function might have non-parameter args too
        // For now, we'll validate with just the parameter types we have
        // TODO: We should properly track all args (both params and non-params) for complete validation

        // Sort args by index to ensure correct order
        let mut param_args = param_args;
        param_args.sort_by_key(|(idx, _)| *idx);

        // For validation, we need the complete arg list
        // This is a limitation - we're only validating with parameter types
        // Ideally we'd track the full function signature during analysis
        if param_args.len() == func.signature().min_args {
            // If we have all args as parameters, we can validate
            let arg_types: Vec<DataType> = param_args.into_iter().map(|(_, dt)| dt).collect();
            func.validate(&arg_types)?;
        }
        // Otherwise skip validation - we don't have complete type info
    }

    Ok(())
}
