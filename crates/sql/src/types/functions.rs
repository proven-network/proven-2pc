//! Compatibility layer for old function calls
//!
//! This module provides backward compatibility by delegating to the new
//! function registry system.

use crate::error::Result;
use crate::stream::transaction::TransactionContext;
use crate::types::value::Value;

/// Evaluate a SQL function call (compatibility layer)
pub fn evaluate_function(
    name: &str,
    args: &[Value],
    context: &TransactionContext,
) -> Result<Value> {
    // Delegate to the new function system
    crate::functions::execute_function(name, args, context)
}

/// Cast a value to a target type (for backward compatibility)
pub fn cast_value(value: &Value, target_type: &str) -> Result<Value> {
    // Use the CAST function from the registry
    let args = vec![value.clone(), Value::string(target_type)];
    use proven_hlc::{HlcTimestamp, NodeId};
    let context = TransactionContext::new(HlcTimestamp::new(1, 0, NodeId::new(0)));
    crate::functions::execute_function("CAST", &args, &context)
}

// This module now only provides backward compatibility.
// All actual function implementations have been moved to crate::functions