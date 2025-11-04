//! Read-only flow - direct execution for snapshot isolation reads
//!
//! Read-only operations bypass the ordered stream and use pubsub for
//! immediate execution with snapshot isolation.

use crate::engine::TransactionEngine;
use crate::error::Result;
use crate::executor::ReadOnlyExecution;
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_protocol::ReadOnlyMessage;

/// Handles unordered messages from pubsub (no batching, immediate response)
pub struct UnorderedFlow;

impl UnorderedFlow {
    /// Process a unordered message (no batching, immediate response)
    pub fn process<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        message: ReadOnlyMessage<E::Operation>,
    ) -> Result<()> {
        ReadOnlyExecution::execute(
            engine,
            tx_manager,
            response,
            message.operation,
            message.read_timestamp,
            message.coordinator_id,
            message.request_id,
        )
    }
}
