//! Generic stream processing for distributed transactions
//!
//! This crate provides a generic stream processor that handles message
//! consumption, transaction coordination, and retry logic. Storage systems
//! (SQL, KV, etc.) implement the TransactionEngine trait to plug into
//! this framework.
//!
//! ## Architecture
//!
//! The stream processor handles:
//! - Message parsing and dispatch
//! - Transaction control (prepare, commit, abort)
//! - Wound-wait deadlock prevention
//! - Deferred operation management
//! - Coordinator communication
//!
//! Storage engines provide:
//! - Operation execution
//! - Lock management
//! - Transaction isolation
//! - Persistence

pub mod deferred;
pub mod engine;
pub mod error;
pub mod processor;
pub mod recovery;

#[cfg(test)]
mod test;
#[cfg(test)]
mod wound_wait_tests;

pub use deferred::DeferredOperationsManager;
pub use engine::{OperationResult, RetryOn, TransactionEngine};
pub use error::{ProcessorError, Result};
pub use processor::StreamProcessor;
pub use recovery::{RecoveryManager, RecoveryState, TransactionDecision};
