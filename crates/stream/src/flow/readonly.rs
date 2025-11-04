//! Read-only flow - direct execution for snapshot isolation reads
//!
//! Read-only operations bypass the ordered stream and use pubsub for
//! immediate execution with snapshot isolation.

use crate::engine::TransactionEngine;
use crate::error::Result;
use crate::executor::ReadOnlyExecution;
use crate::support::ResponseSender;
use crate::transaction::TransactionManager;
use proven_protocol::CoordinatorMessage;

/// Handles read-only messages from pubsub
pub struct ReadOnlyFlow;

impl ReadOnlyFlow {
    /// Process a read-only message (no batching, immediate response)
    pub fn process<E: TransactionEngine>(
        engine: &mut E,
        tx_manager: &mut TransactionManager<E>,
        response: &ResponseSender,
        message: CoordinatorMessage<E::Operation>,
    ) -> Result<()> {
        match message {
            CoordinatorMessage::ReadOnly {
                read_timestamp,
                coordinator_id,
                request_id,
                operation,
            } => {
                ReadOnlyExecution::execute(
                    engine,
                    tx_manager,
                    response,
                    operation,
                    read_timestamp,
                    coordinator_id,
                    request_id,
                )
            }
            _ => Err("Only read-only messages allowed on pubsub channel".into()),
        }
    }
}
