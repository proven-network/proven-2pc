//! Common types for Proven SQL
//!
//! This crate defines:
//! - Core abstractions for operations and responses within transactions
//! - Transaction IDs (UUIDv7-based)
//! - Physical timestamps (microseconds since Unix epoch)
//! - Processor types for stream processing

mod change_data;
mod operation;
mod processor_type;
mod response;
mod timestamp;
mod transaction_id;

pub use change_data::ChangeData;
pub use operation::Operation;
pub use operation::OperationType;
pub use processor_type::ProcessorType;
pub use response::Response;
pub use timestamp::Timestamp;
pub use transaction_id::TransactionId;
