//! The SQL data model, including data types, expressions, and schema objects.

pub mod coercion;
pub mod data_type;
pub mod evaluator;
pub mod expression;
pub mod functions;
pub mod query;
pub mod schema;
pub mod statistics;
pub mod value;

// Re-export key types - matching toydb's pattern
pub use data_type::DataType;
pub use expression::Expression;
pub use query::{Direction, JoinType, RowRef, Rows};
pub use schema::{Column, Label, Table};
pub use value::{Row, Value};
