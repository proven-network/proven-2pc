//! Clean stream processing for distributed transactions
//!
//! This crate provides a reimagined stream processor with clear separation of concerns:
//! - **Message Dispatch**: Route messages by transaction mode
//! - **Executors**: Self-contained execution for each mode (read-only, ad-hoc, read-write)
//! - **Transaction Management**: Unified state management with clear lifecycle
//! - **Deferral**: Simple, understandable blocking and retry logic

mod engine;
mod error;
mod executor;
mod flow;
mod processor;
mod support;
mod transaction;

// Re-export key types
pub use engine::{
    AutoBatchEngine, BatchOperations, BlockingInfo, OperationResult, RetryOn, TransactionEngine,
};
pub use error::{Error, Result};
pub use processor::{ProcessorPhase, StreamProcessor};
