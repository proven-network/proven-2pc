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
//!
//! ## Execution Modes
//!
//! The processor supports three execution modes:
//! - **Read-Only**: Snapshot isolation reads without locking
//! - **Ad-Hoc**: Auto-commit operations without explicit transactions
//! - **Read-Write**: Full ACID transactions with 2PC support

pub mod engine;
pub mod error;
pub mod execution;
pub mod processor;
pub mod router;
pub mod transaction;

#[cfg(test)]
mod test;
#[cfg(test)]
mod wound_wait_tests;

// Re-export from engine module
pub use engine::{BlockingInfo, OperationResult, RetryOn, TransactionEngine, TransactionMode};

// Re-export from error module
pub use error::{ProcessorError, Result};

// Re-export from processor module
pub use processor::{ProcessorPhase, SnapshotConfig, StreamProcessor};

// Re-export from transaction module (including recovery)
pub use transaction::{
    DeferredOperationsManager, RecoveryManager, RecoveryState, TransactionContext,
    TransactionDecision, TransactionState,
};
