//! Recovery manager for handling coordinator failures
//!
//! This module handles recovery of transactions after their deadlines have passed.

use proven_common::{Timestamp, TransactionId};
use proven_engine::{DeadlineStreamItem, MockClient};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Decision made about a transaction during recovery
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionDecision {
    Commit,
    Abort,
    Unknown, // No decision found yet
}

/// Manager for transaction recovery after coordinator failure
pub struct RecoveryManager {
    /// Engine client for reading from other streams
    client: Arc<MockClient>,

    /// Name of this stream
    stream_name: String,
}

impl RecoveryManager {
    /// Create a new recovery manager
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            client,
            stream_name,
        }
    }

    /// Execute recovery for a transaction by reading from participant streams
    pub async fn recover(
        &self,
        txn_id: TransactionId,
        participants: &HashMap<String, u64>,
        current_time: Timestamp,
    ) -> TransactionDecision {
        let txn_id_str = txn_id.to_string();

        tracing::info!(
            "[{}] Recovering transaction {} by querying {} participants",
            self.stream_name,
            txn_id,
            participants.len()
        );

        // Read each participant stream to find decision
        for (stream_name, start_offset) in participants {
            // Skip our own stream
            if stream_name == &self.stream_name {
                continue;
            }

            tracing::debug!(
                "[{}] Querying participant {} from offset {}",
                self.stream_name,
                stream_name,
                start_offset
            );

            // Read from participant stream until current time
            match self.client.stream_messages_until_deadline(
                stream_name,
                Some(*start_offset),
                current_time,
            ) {
                Ok(stream) => {
                    let decision = self.scan_stream_for_decision(stream, &txn_id_str).await;

                    match decision {
                        TransactionDecision::Commit => {
                            // If any participant committed, all must commit
                            tracing::info!(
                                "[{}] Found COMMIT decision in {} for transaction {}",
                                self.stream_name,
                                stream_name,
                                txn_id
                            );
                            return TransactionDecision::Commit;
                        }
                        TransactionDecision::Abort => {
                            // If any participant aborted, all must abort
                            tracing::info!(
                                "[{}] Found ABORT decision in {} for transaction {}",
                                self.stream_name,
                                stream_name,
                                txn_id
                            );
                            return TransactionDecision::Abort;
                        }
                        TransactionDecision::Unknown => {
                            // Continue checking other participants
                            continue;
                        }
                    }
                }
                Err(e) => {
                    // Failed to read stream - treat as unknown
                    tracing::warn!(
                        "[{}] Error reading stream {}: {:?}",
                        self.stream_name,
                        stream_name,
                        e
                    );
                    continue;
                }
            }
        }

        // No decision found from participants - default to abort (safe choice)
        tracing::info!(
            "[{}] No decision found for transaction {}, defaulting to abort",
            self.stream_name,
            txn_id
        );
        TransactionDecision::Abort
    }

    /// Scan a stream for transaction decisions
    async fn scan_stream_for_decision(
        &self,
        stream: impl tokio_stream::Stream<Item = DeadlineStreamItem>,
        txn_id: &str,
    ) -> TransactionDecision {
        use tokio::pin;
        pin!(stream);

        while let Some(item) = stream.next().await {
            match item {
                DeadlineStreamItem::Message(msg, _timestamp, _offset) => {
                    // Check if this message is for our transaction
                    if msg.txn_id() == Some(txn_id) {
                        // Check for commit/abort decision
                        if let Some(phase) = msg.txn_phase() {
                            match phase {
                                "commit" => {
                                    return TransactionDecision::Commit;
                                }
                                "abort" => {
                                    return TransactionDecision::Abort;
                                }
                                _ => {
                                    continue;
                                }
                            }
                        }
                    }
                }
                DeadlineStreamItem::DeadlineReached => {
                    // Reached deadline without finding a decision
                    break;
                }
            }
        }

        TransactionDecision::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_decision_equality() {
        assert_eq!(TransactionDecision::Commit, TransactionDecision::Commit);
        assert_eq!(TransactionDecision::Abort, TransactionDecision::Abort);
        assert_eq!(TransactionDecision::Unknown, TransactionDecision::Unknown);

        assert_ne!(TransactionDecision::Commit, TransactionDecision::Abort);
        assert_ne!(TransactionDecision::Commit, TransactionDecision::Unknown);
    }
}
