//! Type definitions for semantic analysis

use crate::parsing::ast::Expression;
use crate::types::data_type::DataType;
use std::collections::HashSet;

/// Metadata about a statement collected during analysis
#[derive(Debug, Default, Clone)]
pub struct StatementMetadata {
    /// Tables referenced in the statement
    pub referenced_tables: HashSet<String>,
    /// Columns referenced (table_name, column_name)
    pub referenced_columns: HashSet<(String, String)>,
    /// Whether the statement is deterministic
    pub is_deterministic: bool,
    /// Whether the statement modifies data
    pub is_mutation: bool,
    /// Parameter type expectations
    pub parameter_expectations: Vec<ParameterExpectation>,
}

impl StatementMetadata {
    pub fn new() -> Self {
        Self {
            referenced_tables: HashSet::new(),
            referenced_columns: HashSet::new(),
            is_deterministic: true,
            is_mutation: false,
            parameter_expectations: Vec::new(),
        }
    }
}

/// Expected type for a parameter placeholder
#[derive(Debug, Clone)]
pub struct ParameterExpectation {
    /// Parameter index (0-based)
    pub index: usize,
    /// Expected data types (multiple types may be valid due to coercion)
    pub expected_types: Vec<DataType>,
    /// Context where the parameter is used (for error messages)
    pub context: String,
}

impl ParameterExpectation {
    pub fn new(index: usize, expected_type: DataType, context: impl Into<String>) -> Self {
        Self {
            index,
            expected_types: vec![expected_type],
            context: context.into(),
        }
    }

    pub fn with_types(index: usize, types: Vec<DataType>, context: impl Into<String>) -> Self {
        Self {
            index,
            expected_types: types,
            context: context.into(),
        }
    }
}
