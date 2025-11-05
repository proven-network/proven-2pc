//! Proven Value - Universal value type for Proven database components
//!
//! This crate provides a comprehensive Value type that can represent:
//! - All SQL data types (integers, floats, decimals, strings, dates, etc.)
//! - Collection types (arrays, lists, maps, structs)
//! - JSON for schemaless data
//!
//! It also provides efficient binary encoding for storage and indexes.

pub mod codec;
pub mod identity;
pub mod interval;
pub mod point;
pub mod private_key;
pub mod public_key;
pub mod types;
pub mod vault;

#[cfg(feature = "mvcc")]
pub mod mvcc;

pub use codec::{Error, Result, decode_value, encode_value};
pub use identity::Identity;
pub use interval::Interval;
pub use point::Point;
pub use private_key::PrivateKey;
pub use public_key::PublicKey;
pub use types::Value;
pub use vault::Vault;
