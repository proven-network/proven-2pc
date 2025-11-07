//! The SQL data model, including data types, expressions, and schema objects.

pub mod change_data;
pub mod context;
pub mod data_type;
pub mod expression;
pub mod index;
pub mod operation;
pub mod plan;
pub mod query;
pub mod response;
pub mod schema;
pub mod value_ext;
// pub mod statistics;

// Re-export key types
pub use data_type::DataType;

// Re-export Value and related types from proven-value
pub use proven_value::{Interval, Point, Value};

// Re-export value extension trait
pub use value_ext::ValueExt;

// SQL-specific type: A row is a vector of values
pub type Row = Vec<Value>;
