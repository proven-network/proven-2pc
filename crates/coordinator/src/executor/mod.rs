//! Executor trait and implementations for different transaction types
//!
//! This module provides three executor types:
//! - ReadWriteExecutor: Full 2PC transactions with speculation
//! - ReadOnlyExecutor: Snapshot isolation for read-only operations
//! - AdHocExecutor: Auto-commit operations without transactions

mod adhoc;
pub(crate) mod common;
mod read_only;
mod read_write;

pub use adhoc::AdHocExecutor;
pub use read_only::ReadOnlyExecutor;
pub use read_write::ReadWriteExecutor;

use crate::error::Result;
use async_trait::async_trait;
use proven_common::Operation;

/// Common interface for all executor types
#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute an operation on a stream
    async fn execute<O: Operation + Send + Sync>(
        &self,
        stream: String,
        operation: &O,
    ) -> Result<Vec<u8>>;

    /// Complete the transaction/session successfully
    async fn finish(&self) -> Result<()>;

    /// Cancel/abort the transaction/session
    async fn cancel(&self) -> Result<()>;
}
