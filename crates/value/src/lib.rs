//! Proven Value - Universal value type for Proven database components
//!
//! This crate provides a comprehensive Value type that can represent:
//! - All SQL data types (integers, floats, decimals, strings, dates, etc.)
//! - Collection types (arrays, lists, maps, structs)
//! - JSON for schemaless data
//!
//! It also provides efficient binary encoding for storage and indexes.

pub mod encoding;
pub mod types;

#[cfg(feature = "mvcc")]
pub mod mvcc;

pub use encoding::{Error, Result, decode_value, encode_value};
pub use types::{Interval, Point, Value};
