//! The SQL data model, including data types, expressions, and schema objects.

pub mod context;
pub mod data_type;
// pub mod evaluator; // Deprecated: use operators module instead
pub mod expression;
pub mod index;
pub mod query;
pub mod schema;
// pub mod statistics;
pub mod value;

// Re-export key types - matching toydb's pattern
pub use data_type::DataType;
pub use value::Value;
