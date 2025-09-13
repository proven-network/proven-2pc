//! Distributed transaction coordinator
//!
//! This crate provides transaction coordination across multiple storage engines
//! using two-phase commit protocol.

mod coordinator;
mod error;
mod responses;
mod transaction;

pub use coordinator::Coordinator;
pub use error::{CoordinatorError, Result};
pub use responses::{ResponseCollector, ResponseMessage};
pub use transaction::{PrepareVote, Transaction, TransactionState};
