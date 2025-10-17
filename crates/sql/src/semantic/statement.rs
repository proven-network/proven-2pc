//! Analyzed statement with zero-copy AST sharing
//!
//! This module defines the output of semantic analysis - a lightweight
//! structure that references the original AST via Arc and maintains
//! all type annotations and metadata separately.

use super::predicate::{Predicate, PredicateCondition, QueryPredicates};
use crate::parsing::ast::Statement;
use crate::types::Value;
use crate::types::data_type::DataType;
use indexmap::IndexMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

/// Complete output of semantic analysis
#[derive(Debug, Clone)]
pub struct AnalyzedStatement {
    /// Shared, immutable reference to the original AST
    pub ast: Arc<Statement>,

    /// Statement metadata for optimization and validation
    pub metadata: StatementMetadata,

    /// Parameter slots with full context
    pub parameter_slots: Vec<ParameterSlot>,

    /// Pre-extracted predicate templates for conflict detection
    pub predicate_templates: Vec<PredicateTemplate>,

    /// Pre-resolved column mappings for O(1) lookups
    pub column_resolution_map: ColumnResolutionMap,

    /// Aggregate expressions in the statement (structural info)
    pub aggregate_expressions: HashSet<ExpressionId>,

    /// Nullability of output columns (parallel to output_schema)
    pub output_nullability: Vec<bool>,

    /// Join optimization hints from semantic analysis
    pub join_hints: Vec<crate::semantic::analyzer::JoinHint>,

    /// Analyzed statements for subqueries in FROM clauses
    /// These have their own local column resolution maps with offsets starting from 0
    /// Key: subquery alias name -> analyzed subquery
    pub subquery_analyses: std::collections::HashMap<String, Arc<AnalyzedStatement>>,
}

/// Template for a predicate that can be evaluated with parameters
#[derive(Debug, Clone)]
pub enum PredicateTemplate {
    /// Full table access (no specific predicate)
    FullTable { table: String },

    /// Equality predicate (column = value)
    Equality {
        table: String,
        column_name: String,
        value_expr: PredicateValue,
    },

    /// Range predicate (column BETWEEN x AND y)
    Range {
        table: String,
        column_name: String,
        lower: Option<(PredicateValue, bool)>, // (value, inclusive)
        upper: Option<(PredicateValue, bool)>, // (value, inclusive)
    },

    /// IN list predicate (column IN (...))
    InList {
        table: String,
        column_name: String,
        values: Vec<PredicateValue>,
    },

    /// Primary key access
    PrimaryKey {
        table: String,
        value: PredicateValue,
    },

    /// Indexed column predicate
    IndexedColumn {
        table: String,
        column: String,
        value: PredicateValue,
    },

    /// LIKE pattern matching predicate
    Like {
        table: String,
        column_name: String,
        pattern: String,
    },

    /// IS NULL predicate
    IsNull { table: String, column_name: String },

    /// IS NOT NULL predicate
    IsNotNull { table: String, column_name: String },
}

/// Value in a predicate - either constant, parameter, or expression
#[derive(Debug, Clone)]
pub enum PredicateValue {
    Constant(Value),
    Parameter(usize),
    Expression(ExpressionId), // For complex expressions we evaluate at runtime
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

    /// The actual type if known
    pub actual_type: Option<DataType>,
}

/// Statement metadata for planning and optimization
#[derive(Debug, Clone, Default)]
pub struct StatementMetadata {
    /// Statement type
    pub statement_type: StatementType,

    /// Whether statement contains aggregates
    pub has_aggregates: bool,

    /// Optimization hints from semantic analysis
    pub optimization_output: Option<Box<crate::semantic::analyzer::OptimizationOutput>>,
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
    AlterTable,
    CreateIndex,
    DropIndex,
    Explain,
    #[default]
    Other,
}

/// Pre-resolved column mappings for O(1) lookups during planning
#[derive(Debug, Clone, Default)]
pub struct ColumnResolutionMap {
    /// Direct lookup: (table_qualifier, column_name) -> resolution
    /// Using IndexMap to preserve insertion order for deterministic column ordering
    pub columns: IndexMap<(Option<String>, String), ColumnResolution>,

    /// Ambiguous columns that need table qualification
    pub ambiguous: HashSet<String>,
}

/// Pre-resolved column information
#[derive(Debug, Clone)]
pub struct ColumnResolution {
    /// Column offset for execution
    pub offset: usize,

    /// Actual table name (not alias)
    pub table_name: String,

    /// Column metadata
    pub data_type: DataType,
    pub nullable: bool,
    pub is_indexed: bool,
}

// Implementation methods
impl AnalyzedStatement {
    /// Create a new analyzed statement
    pub fn new(ast: Arc<Statement>) -> Self {
        Self {
            ast,
            metadata: StatementMetadata::default(),
            parameter_slots: Vec::new(),
            predicate_templates: Vec::new(),
            column_resolution_map: ColumnResolutionMap::default(),
            aggregate_expressions: HashSet::new(),
            output_nullability: Vec::new(),
            join_hints: Vec::new(),
            subquery_analyses: std::collections::HashMap::new(),
        }
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

    /// Add a predicate template
    pub fn add_predicate_template(&mut self, template: PredicateTemplate) {
        self.predicate_templates.push(template);
    }

    /// Extract predicates using actual parameter values
    /// This doesn't walk the tree - just evaluates pre-extracted templates
    pub fn extract_predicates(&self, params: &[Value]) -> QueryPredicates {
        use crate::parsing::ast::{DmlStatement, Statement};

        // Determine the operation type
        let (is_read, is_write, is_insert) = match &*self.ast {
            Statement::Dml(dml) => match dml {
                DmlStatement::Select(_) => (true, false, false),
                DmlStatement::Insert { .. } => (false, false, true), // Only insert, not write
                DmlStatement::Update { .. } => (true, true, false),  // Read then write
                DmlStatement::Delete { .. } => (true, true, false),  // Read then write
                DmlStatement::Values(_) => (false, false, false),    // No table access
            },
            _ => (false, false, false), // DDL operations
        };

        let mut reads = Vec::new();
        let mut writes = Vec::new();
        let mut inserts = Vec::new();

        // Evaluate each template with parameters
        for template in &self.predicate_templates {
            if let Some(predicate) = self.evaluate_template(template, params) {
                if is_read {
                    reads.push(predicate.clone());
                }
                if is_write {
                    writes.push(predicate.clone());
                }
                if is_insert {
                    inserts.push(predicate);
                }
            }
        }

        QueryPredicates {
            reads,
            writes,
            inserts,
        }
    }

    /// Evaluate a predicate template with parameter values
    fn evaluate_template(
        &self,
        template: &PredicateTemplate,
        params: &[Value],
    ) -> Option<Predicate> {
        match template {
            PredicateTemplate::FullTable { table } => Some(Predicate::full_table(table.clone())),

            PredicateTemplate::Equality {
                table,
                column_name,
                value_expr,
                ..
            } => {
                let value = self.evaluate_predicate_value(value_expr, params)?;
                Some(Predicate {
                    table: table.clone(),
                    condition: PredicateCondition::Equals {
                        column: column_name.clone(),
                        value,
                    },
                })
            }

            PredicateTemplate::PrimaryKey { table, value } => {
                let pk_value = self.evaluate_predicate_value(value, params)?;
                Some(Predicate::primary_key(table.clone(), pk_value))
            }

            PredicateTemplate::IndexedColumn {
                table,
                column,
                value,
            } => {
                let evaluated = self.evaluate_predicate_value(value, params)?;
                Some(Predicate {
                    table: table.clone(),
                    condition: PredicateCondition::Equals {
                        column: column.clone(),
                        value: evaluated,
                    },
                })
            }

            PredicateTemplate::Range {
                table,
                column_name,
                lower,
                upper,
                ..
            } => {
                let start = lower
                    .as_ref()
                    .and_then(|(val, inclusive)| {
                        self.evaluate_predicate_value(val, params).map(|v| {
                            if *inclusive {
                                Bound::Included(v)
                            } else {
                                Bound::Excluded(v)
                            }
                        })
                    })
                    .unwrap_or(Bound::Unbounded);

                let end = upper
                    .as_ref()
                    .and_then(|(val, inclusive)| {
                        self.evaluate_predicate_value(val, params).map(|v| {
                            if *inclusive {
                                Bound::Included(v)
                            } else {
                                Bound::Excluded(v)
                            }
                        })
                    })
                    .unwrap_or(Bound::Unbounded);

                Some(Predicate {
                    table: table.clone(),
                    condition: PredicateCondition::Range {
                        column: column_name.clone(),
                        start,
                        end,
                    },
                })
            }

            PredicateTemplate::InList {
                table,
                column_name,
                values,
                ..
            } => {
                let mut evaluated_values = Vec::new();
                for val in values {
                    if let Some(v) = self.evaluate_predicate_value(val, params) {
                        evaluated_values.push(v);
                    }
                }

                if evaluated_values.is_empty() {
                    return None;
                }

                // Convert IN list to OR of equalities
                Some(Predicate {
                    table: table.clone(),
                    condition: PredicateCondition::Or(
                        evaluated_values
                            .into_iter()
                            .map(|v| PredicateCondition::Equals {
                                column: column_name.clone(),
                                value: v,
                            })
                            .collect(),
                    ),
                })
            }

            PredicateTemplate::Like {
                table,
                column_name,
                pattern,
            } => Some(Predicate {
                table: table.clone(),
                condition: PredicateCondition::Like {
                    column: column_name.clone(),
                    pattern: pattern.clone(),
                },
            }),

            PredicateTemplate::IsNull { table, column_name } => Some(Predicate {
                table: table.clone(),
                condition: PredicateCondition::IsNull {
                    column: column_name.clone(),
                },
            }),

            PredicateTemplate::IsNotNull { table, column_name } => Some(Predicate {
                table: table.clone(),
                condition: PredicateCondition::IsNotNull {
                    column: column_name.clone(),
                },
            }),
        }
    }

    /// Evaluate a predicate value with parameters
    fn evaluate_predicate_value(&self, value: &PredicateValue, params: &[Value]) -> Option<Value> {
        match value {
            PredicateValue::Constant(v) => Some(v.clone()),
            PredicateValue::Parameter(idx) => params.get(*idx).cloned(),
            PredicateValue::Expression(_expr_id) => {
                // For now, we don't evaluate complex expressions
                // This could be enhanced to evaluate simple arithmetic
                None
            }
        }
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
}

impl StatementMetadata {}

impl ColumnResolutionMap {
    /// Add a column resolution
    pub fn add_resolution(
        &mut self,
        table_alias: Option<String>,
        column_name: String,
        resolution: ColumnResolution,
    ) {
        // Check if this column is ambiguous (exists without qualifier)
        if table_alias.is_some() {
            // Also add without qualifier if it's unambiguous
            let unqualified_key = (None, column_name.clone());
            if !self.columns.contains_key(&unqualified_key)
                && !self.ambiguous.contains(&column_name)
            {
                self.columns.insert(unqualified_key, resolution.clone());
            }
        }

        self.columns.insert((table_alias, column_name), resolution);
    }

    /// Mark a column as ambiguous (needs qualification)
    pub fn mark_ambiguous(&mut self, column_name: String) {
        self.ambiguous.insert(column_name.clone());
        // Remove any unqualified entry
        // Using shift_remove to maintain order of remaining elements
        self.columns.shift_remove(&(None, column_name));
    }

    /// Check if a column is ambiguous
    pub fn is_ambiguous(&self, column_name: &str) -> bool {
        self.ambiguous.contains(column_name)
    }

    /// Resolve a column reference (tries qualified, then unqualified if not ambiguous)
    pub fn resolve(
        &self,
        table_alias: Option<&str>,
        column_name: &str,
    ) -> Option<&ColumnResolution> {
        // First try with the exact qualifier
        if let Some(res) = self
            .columns
            .get(&(table_alias.map(|s| s.to_string()), column_name.to_string()))
        {
            return Some(res);
        }

        // If no qualifier given and column is not ambiguous, try unqualified
        if table_alias.is_none() && !self.is_ambiguous(column_name) {
            return self.columns.get(&(None, column_name.to_string()));
        }

        None
    }

    /// Extract column names in order by offset
    /// Returns column names as they should appear in query results
    pub fn get_ordered_column_names(&self) -> Vec<String> {
        // Build a map of offset -> column name, preferring unqualified entries
        let mut offset_to_name: std::collections::HashMap<usize, String> =
            std::collections::HashMap::new();

        // First pass: add all entries
        for ((qualifier, col_name), resolution) in &self.columns {
            let offset = resolution.offset;

            // Only set if not already set, OR if this is an unqualified entry (prefer unqualified)
            if !offset_to_name.contains_key(&offset) || qualifier.is_none() {
                offset_to_name.insert(offset, col_name.clone());
            }
        }

        // Convert to sorted vector
        let mut columns: Vec<(usize, String)> = offset_to_name.into_iter().collect();
        columns.sort_by_key(|(offset, _)| *offset);

        // Extract just the names
        columns.into_iter().map(|(_, name)| name).collect()
    }
}
