//! Type definitions for semantic analysis

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
}

impl StatementMetadata {
    pub fn new() -> Self {
        Self {
            referenced_tables: HashSet::new(),
            referenced_columns: HashSet::new(),
            is_deterministic: true,
        }
    }
}
