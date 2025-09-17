//! Function validation
//!
//! Updates parameter descriptions and delegates actual validation to functions module

use crate::error::Result;
use crate::semantic::context::AnalysisContext;
use crate::semantic::statement::{AnalyzedStatement, SqlContext};

/// Validates functions with parameters
///
/// This validator simply updates parameter descriptions for better error messages.
/// Actual function validation is delegated to the functions module.
pub struct FunctionValidator;

impl FunctionValidator {
    /// Create a new function validator
    pub fn new() -> Self {
        Self
    }

    /// Update parameter descriptions for function arguments
    ///
    /// The actual validation happens in the functions module when we have
    /// concrete parameter values. This just improves error messages.
    pub fn validate(
        &self,
        analyzed: &mut AnalyzedStatement,
        _context: &AnalysisContext,
    ) -> Result<()> {
        // Just update descriptions for better error messages
        for slot in &mut analyzed.parameter_slots {
            if let SqlContext::FunctionArgument {
                ref function_name,
                arg_index,
            } = slot.coercion_context.sql_context
            {
                // Get function to check if it's aggregate
                if let Some(func) = crate::functions::get_function(function_name) {
                    let sig = func.signature();

                    slot.description = if sig.is_aggregate {
                        format!(
                            "Parameter {} for aggregate function '{}'",
                            arg_index + 1,
                            function_name
                        )
                    } else {
                        format!(
                            "Parameter {} for function '{}'",
                            arg_index + 1,
                            function_name
                        )
                    };
                }
            }
        }
        Ok(())
    }
}
