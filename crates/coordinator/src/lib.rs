//! Distributed transaction coordinator
//!
//! This crate provides transaction coordination across multiple storage engines
//! using two-phase commit protocol with speculative execution support.

mod coordinator;
mod error;
mod executor;
mod responses;
mod speculation;
mod transaction;

pub use coordinator::Coordinator;
pub use error::{CoordinatorError, Result};
pub use executor::Executor;
pub use responses::{ResponseCollector, ResponseMessage};
pub use transaction::{PrepareVote, Transaction, TransactionState};
