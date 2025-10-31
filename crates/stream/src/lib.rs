//! Clean stream processing for distributed transactions
//!
//! This crate provides a reimagined stream processor with clear separation of concerns:
//! - **Message Dispatch**: Route messages by transaction mode
//! - **Executors**: Self-contained execution for each mode (read-only, ad-hoc, read-write)
//! - **Transaction Management**: Unified state management with clear lifecycle
//! - **Deferral**: Simple, understandable blocking and retry logic

pub mod dispatcher;
pub mod engine;
pub mod error;
pub mod executor;
pub mod processor;
pub mod support;
pub mod transaction;

// Re-export key types
pub use dispatcher::MessageDispatcher;
pub use engine::{
    AutoBatchEngine, BlockingInfo, OperationResult, RetryOn, TransactionEngine, TransactionMode,
};
pub use error::{ProcessorError, Result};
pub use executor::{AdHocExecutor, ReadOnlyExecutor, ReadWriteExecutor};
pub use processor::{ProcessorPhase, StreamProcessor};
pub use support::ResponseSender;
pub use transaction::{
    AbortReason, DeferralManager, DeferredOp, RecoveryManager, TransactionDecision,
    TransactionManager, TransactionPhase, TransactionState, WaitingFor,
};
