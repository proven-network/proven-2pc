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
}

/// Enhanced parameter slot with full context
#[derive(Debug, Clone)]
pub struct ParameterSlot {
    /// Parameter index (0-based)
    pub index: usize,

    /// Location in the AST (for error reporting and binding)
    pub expression_id: ExpressionId,

    /// The actual type if known
    pub actual_type: Option<DataType>,

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

    /// All columns referenced
    pub referenced_columns: HashSet<(String, String)>, // (table, column)

    /// Output schema for SELECT statements
    pub output_schema: Option<Vec<(String, DataType)>>,

    /// Whether statement modifies data
    pub is_mutation: bool,

    /// Whether statement contains aggregates
    pub has_aggregates: bool,
}

/// Statement type for quick identification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    #[default]
    Other,
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
}

impl ExpressionId {
    /// Create from a path of indices
    pub fn from_path(path: Vec<usize>) -> Self {
        Self(path)
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
    /// Add a referenced column
    pub fn add_column_reference(&mut self, table: String, column: String) {
        self.referenced_columns.insert((table, column));
    }

    /// Set the output schema
    pub fn set_output_schema(&mut self, schema: Vec<(String, DataType)>) {
        self.output_schema = Some(schema);
    }
}
