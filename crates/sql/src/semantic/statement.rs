//! Analyzed statement with zero-copy AST sharing
//!
//! This module defines the output of semantic analysis - a lightweight
//! structure that references the original AST via Arc and maintains
//! all type annotations and metadata separately.

use crate::parsing::ast::Statement;
use crate::types::data_type::DataType;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Complete output of semantic analysis
#[derive(Debug, Clone)]
pub struct AnalyzedStatement {
    /// Shared, immutable reference to the original AST
    pub ast: Arc<Statement>,

    /// Type annotations for all expressions in the AST
    pub type_annotations: TypeAnnotations,

    /// Statement metadata for optimization and validation
    pub metadata: StatementMetadata,

    /// Parameter slots with full context
    pub parameter_slots: Vec<ParameterSlot>,
}

/// Type annotations for expressions in the AST
#[derive(Debug, Clone, Default)]
pub struct TypeAnnotations {
    /// Maps expression IDs to their type information
    /// The ID is computed based on the expression's position in the AST
    annotations: HashMap<ExpressionId, TypeInfo>,

    /// Column index mappings for resolved column references
    /// Maps (table_alias, column_name) to column index in row
    column_indices: HashMap<(Option<String>, String), usize>,
}

/// Unique identifier for an expression in the AST
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ExpressionId(Vec<usize>);

/// Type information for an expression
#[derive(Debug, Clone)]
pub struct TypeInfo {
    /// The inferred data type
    pub data_type: DataType,

    /// Whether this expression can be NULL
    pub nullable: bool,

    /// If this is an aggregate expression
    pub is_aggregate: bool,

    /// If this is deterministic
    pub is_deterministic: bool,
}

/// Enhanced parameter slot with full context
#[derive(Debug, Clone)]
pub struct ParameterSlot {
    /// Parameter index (0-based)
    pub index: usize,

    /// Location in the AST (for error reporting and binding)
    pub expression_id: ExpressionId,

    /// Acceptable types in preference order
    pub acceptable_types: Vec<DataType>,

    /// Coercion context and hints
    pub coercion_context: CoercionContext,

    /// Human-readable description
    pub description: String,
}

/// Context for parameter coercion
#[derive(Debug, Clone)]
pub struct CoercionContext {
    /// The SQL context (WHERE, INSERT, etc.)
    pub sql_context: SqlContext,

    /// Whether NULL is acceptable
    pub nullable: bool,
}

/// SQL context where a parameter appears
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlContext {
    WhereClause,
    InsertValue {
        column_index: usize,
    },
    UpdateAssignment {
        column_name: String,
    },
    HavingClause,
    JoinCondition,
    SelectProjection,
    OrderBy,
    GroupBy,
    FunctionArgument {
        function_name: String,
        arg_index: usize,
    },
}

/// Statement metadata for planning and optimization
#[derive(Debug, Clone, Default)]
pub struct StatementMetadata {
    /// Statement type
    pub statement_type: StatementType,

    /// Tables referenced with their access patterns
    pub table_access: Vec<TableAccess>,

    /// All columns referenced
    pub referenced_columns: HashSet<(String, String)>, // (table, column)

    /// Output schema for SELECT statements
    pub output_schema: Option<Vec<(String, DataType)>>,

    /// Whether statement modifies data
    pub is_mutation: bool,

    /// Whether all expressions are deterministic
    pub is_deterministic: bool,

    /// Whether statement contains aggregates
    pub has_aggregates: bool,

    /// Estimated row count for optimization
    pub estimated_rows: Option<usize>,
}

/// How a table is accessed in the query
#[derive(Debug, Clone)]
pub struct TableAccess {
    /// Table name
    pub table: String,

    /// Alias if any
    pub alias: Option<String>,

    /// Access pattern
    pub access_type: AccessType,

    /// Columns used from this table
    pub columns_used: Vec<String>,

    /// Predicates that could use indexes
    pub index_opportunities: Vec<IndexOpportunity>,
}

/// Type of table access
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    FullScan,
    IndexScan,
    PrimaryKeyLookup,
    Insert,
    Update,
    Delete,
}

/// Opportunity to use an index
#[derive(Debug, Clone)]
pub struct IndexOpportunity {
    /// Column(s) involved
    pub columns: Vec<String>,

    /// Type of predicate
    pub predicate_type: PredicateType,

    /// Estimated selectivity (0.0 to 1.0)
    pub selectivity: f64,
}

/// Type of predicate for index usage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateType {
    Equality,
    Range,
    Prefix,
    In,
}

/// Statement type for quick identification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    Other,
}

impl Default for StatementType {
    fn default() -> Self {
        StatementType::Other
    }
}

// Implementation methods
impl AnalyzedStatement {
    /// Create a new analyzed statement
    pub fn new(ast: Arc<Statement>) -> Self {
        Self {
            ast,
            type_annotations: TypeAnnotations::default(),
            metadata: StatementMetadata::default(),
            parameter_slots: Vec::new(),
        }
    }

    /// Get type info for an expression
    pub fn get_type(&self, id: &ExpressionId) -> Option<&TypeInfo> {
        self.type_annotations.get(id)
    }

    /// Add a type annotation
    pub fn annotate(&mut self, id: ExpressionId, info: TypeInfo) {
        self.type_annotations.annotate(id, info);
    }

    /// Add a parameter slot
    pub fn add_parameter(&mut self, slot: ParameterSlot) {
        self.parameter_slots.push(slot);
    }

    /// Check if statement has parameters
    pub fn has_parameters(&self) -> bool {
        !self.parameter_slots.is_empty()
    }

    /// Get parameter count
    pub fn parameter_count(&self) -> usize {
        self.parameter_slots.len()
    }

    /// Sort parameter slots by index
    pub fn sort_parameters(&mut self) {
        self.parameter_slots.sort_by_key(|s| s.index);
    }

    /// Convert to a statement by cloning the Arc contents
    /// This is used for compatibility with existing planner
    pub fn into_statement(self) -> Statement {
        (*self.ast).clone()
    }
}

impl TypeAnnotations {
    /// Add a type annotation
    pub fn annotate(&mut self, id: ExpressionId, info: TypeInfo) {
        self.annotations.insert(id, info);
    }

    /// Get type information
    pub fn get(&self, id: &ExpressionId) -> Option<&TypeInfo> {
        self.annotations.get(id)
    }

    /// Add a column index mapping
    pub fn add_column_index(&mut self, table: Option<String>, column: String, index: usize) {
        self.column_indices.insert((table, column), index);
    }

    /// Get column index
    pub fn get_column_index(&self, table: Option<&str>, column: &str) -> Option<usize> {
        self.column_indices
            .get(&(table.map(String::from), column.to_string()))
            .copied()
    }
}

impl ExpressionId {
    /// Create from a path of indices
    pub fn from_path(path: Vec<usize>) -> Self {
        Self(path)
    }

    /// Create for root expression
    pub fn root() -> Self {
        Self(vec![])
    }

    /// Extend with another level
    pub fn child(&self, index: usize) -> Self {
        let mut path = self.0.clone();
        path.push(index);
        Self(path)
    }

    /// Get the path
    pub fn path(&self) -> &[usize] {
        &self.0
    }
}

impl StatementMetadata {
    /// Check if this is a read-only query
    pub fn is_readonly(&self) -> bool {
        !self.is_mutation
    }

    /// Add a table access pattern
    pub fn add_table_access(&mut self, access: TableAccess) {
        self.table_access.push(access);
    }

    /// Add a referenced column
    pub fn add_column_reference(&mut self, table: String, column: String) {
        self.referenced_columns.insert((table, column));
    }

    /// Set the output schema
    pub fn set_output_schema(&mut self, schema: Vec<(String, DataType)>) {
        self.output_schema = Some(schema);
    }
}
