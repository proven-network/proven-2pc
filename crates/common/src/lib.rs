//! Common types for Proven SQL
//!
//! This crate defines:
//! - Core abstractions for operations and responses within transactions
//! - Transaction IDs (UUIDv7-based)
//! - Physical timestamps (microseconds since Unix epoch)

mod operation;
mod response;
mod timestamp;
mod transaction_id;

pub use operation::Operation;
pub use operation::OperationType;
pub use response::Response;
pub use timestamp::Timestamp;
pub use transaction_id::TransactionId;
