//! Semantic analysis module for SQL statements
//!
//! This module performs semantic validation and type checking on parsed SQL statements.
//! It operates between parsing and planning, ensuring that statements are not just
//! syntactically correct but also semantically valid.
//!
//! The semantic analyzer:
//! - Resolves table and column references
//! - Performs type checking on expressions
//! - Validates constraints and rules
//! - Infers parameter types from context
//! - Produces a typed AST with metadata

pub mod analyzer;
pub mod annotated_ast;
pub mod coercion;
pub mod context;
pub mod resolver;
pub mod type_checker;
pub mod types;
pub mod validator;

pub use analyzer::{AnalyzedStatement, SemanticAnalyzer};
pub use annotated_ast::AnnotatedStatement;
pub use coercion::{can_coerce, coerce_value, coercion_cost};
pub use context::AnalysisContext;
pub use types::{ParameterExpectation, StatementMetadata, TypedExpression};