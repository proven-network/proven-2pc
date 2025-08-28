//! The SQL data model, including data types, expressions, and schema objects.

pub mod expression;
pub mod functions;
pub mod schema;
pub mod value;

// Re-export key types - matching toydb's pattern
pub use expression::Expression;
pub use schema::{Column, Label, Table};
pub use value::{DataType, Row, Value};
