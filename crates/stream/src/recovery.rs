//! Recovery manager for handling coordinator failures
//!
//! This module handles recovery of transactions after their deadlines have passed.
//! It reads from other participant streams to determine the outcome of a transaction
//! and writes recovery decisions to the local stream.

use crate::engine::TransactionEngine;
use proven_engine::{DeadlineStreamItem, Message, MockClient};
use proven_hlc::HlcTimestamp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Tracks the state of a transaction during recovery
#[derive(Debug, Clone)]
pub enum RecoveryState {
    /// Transaction is prepared and waiting
    Prepared {
        deadline: HlcTimestamp,
        participants: HashMap<String, u64>, // participant stream -> offset
    },
    /// Recovery in progress for this transaction
    Recovering {
        deadline: HlcTimestamp,
        participants: HashMap<String, u64>,
        decisions: HashMap<String, TransactionDecision>, // decisions from each participant
    },
    /// Recovery completed with a decision
    Completed { decision: TransactionDecision },
}

/// Decision made about a transaction
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionDecision {
    Commit,
    Abort,
    Unknown, // No decision found yet
}

/// Manager for transaction recovery after coordinator failure
pub struct RecoveryManager<E: TransactionEngine> {
    /// Transactions that need recovery (txn_id -> state)
    recovery_states: HashMap<HlcTimestamp, RecoveryState>,

    /// Scheduled recovery tasks (txn_id -> deadline)
    scheduled_recoveries: HashMap<HlcTimestamp, HlcTimestamp>,

    /// Engine client for reading from other streams
    client: Arc<MockClient>,

    /// Name of this stream
    stream_name: String,

    /// Phantom data for engine type
    _engine: std::marker::PhantomData<E>,
}

impl<E: TransactionEngine> RecoveryManager<E> {
    /// Create a new recovery manager
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        Self {
            recovery_states: HashMap::new(),
            scheduled_recoveries: HashMap::new(),
            client,
            stream_name,
            _engine: std::marker::PhantomData,
        }
    }

    /// Schedule recovery for a transaction after it enters prepared state
    pub fn schedule_recovery(
        &mut self,
        txn_id: HlcTimestamp,
        deadline: HlcTimestamp,
        participants: HashMap<String, u64>,
    ) {
        self.recovery_states.insert(
            txn_id,
            RecoveryState::Prepared {
                deadline,
                participants,
            },
        );
        self.scheduled_recoveries.insert(txn_id, deadline);
    }

    /// Check if a transaction needs recovery (called periodically or on conflict)
    pub fn needs_recovery(&self, txn_id: &HlcTimestamp, current_time: HlcTimestamp) -> bool {
        if let Some(&deadline) = self.scheduled_recoveries.get(txn_id) {
            current_time > deadline
        } else {
            false
        }
    }

    /// Execute recovery for a transaction (called by processor)
    pub async fn execute_recovery(
        &mut self,
        txn_id: HlcTimestamp,
        participants: HashMap<String, u64>,
        current_time: HlcTimestamp,
    ) -> TransactionDecision {
        let txn_id_str = txn_id.to_string();

        // Read each participant stream to find decision
        for (stream_name, start_offset) in participants {
            // Skip our own stream
            if stream_name == self.stream_name {
                continue;
            }

            // Read from participant stream until current time (acting as deadline)
            let decision = match self.client.stream_messages_until_deadline(
                &stream_name,
                Some(start_offset),
                current_time,
            ) {
                Ok(stream) => self.scan_stream_for_decision(stream, &txn_id_str).await,
                Err(_) => {
                    // Failed to read stream - treat as unknown
                    TransactionDecision::Unknown
                }
            };

            match decision {
                TransactionDecision::Commit => {
                    // If any participant committed, all must commit
                    return TransactionDecision::Commit;
                }
                TransactionDecision::Abort => {
                    // If any participant aborted, all must abort
                    return TransactionDecision::Abort;
                }
                TransactionDecision::Unknown => {
                    // Continue checking other participants
                    continue;
                }
            }
        }

        // No decision found from participants
        // If we're past the deadline and all were prepared, abort
        TransactionDecision::Abort
    }

    /// Start recovery for a transaction
    pub async fn start_recovery(&mut self, txn_id: HlcTimestamp) -> TransactionDecision {
        // Get the recovery state
        let state = match self.recovery_states.get(&txn_id) {
            Some(RecoveryState::Prepared {
                deadline,
                participants,
            }) => RecoveryState::Recovering {
                deadline: *deadline,
                participants: participants.clone(),
                decisions: HashMap::new(),
            },
            Some(RecoveryState::Recovering { .. }) => {
                // Already recovering
                return TransactionDecision::Unknown;
            }
            Some(RecoveryState::Completed { decision }) => {
                // Already completed
                return decision.clone();
            }
            None => {
                // No recovery state - shouldn't happen
                return TransactionDecision::Unknown;
            }
        };

        // Update state to recovering
        self.recovery_states.insert(txn_id, state.clone());

        // Read from participant streams
        if let RecoveryState::Recovering {
            deadline,
            participants,
            ..
        } = state
        {
            let decision = self
                .read_participant_decisions(txn_id, deadline, participants)
                .await;

            // Store the decision
            self.recovery_states.insert(
                txn_id,
                RecoveryState::Completed {
                    decision: decision.clone(),
                },
            );

            decision
        } else {
            TransactionDecision::Unknown
        }
    }

    /// Read decisions from participant streams
    async fn read_participant_decisions(
        &mut self,
        txn_id: HlcTimestamp,
        deadline: HlcTimestamp,
        participants: HashMap<String, u64>,
    ) -> TransactionDecision {
        let mut decisions = HashMap::new();
        let txn_id_str = txn_id.to_string();

        for (participant_stream, start_offset) in participants {
            // Skip our own stream
            if participant_stream == self.stream_name {
                continue;
            }

            // Read from participant stream until deadline
            let decision = match self.client.stream_messages_until_deadline(
                &participant_stream,
                Some(start_offset),
                deadline,
            ) {
                Ok(stream) => self.scan_stream_for_decision(stream, &txn_id_str).await,
                Err(_) => {
                    // Failed to read stream - treat as unknown
                    TransactionDecision::Unknown
                }
            };

            decisions.insert(participant_stream, decision);
        }

        // Determine overall decision based on participant decisions
        self.determine_recovery_decision(decisions)
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
                DeadlineStreamItem::Message(msg, _timestamp, _sequence) => {
                    // Check if this message is for our transaction
                    if msg.txn_id() == Some(txn_id) {
                        // Check for commit/abort decision
                        if let Some(phase) = msg.txn_phase() {
                            match phase {
                                "commit" => return TransactionDecision::Commit,
                                "abort" => return TransactionDecision::Abort,
                                _ => continue,
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

    /// Determine the overall recovery decision based on participant decisions
    fn determine_recovery_decision(
        &self,
        decisions: HashMap<String, TransactionDecision>,
    ) -> TransactionDecision {
        // If any participant committed, we must commit
        for (_, decision) in &decisions {
            if *decision == TransactionDecision::Commit {
                return TransactionDecision::Commit;
            }
        }

        // If all participants that we could read from aborted, abort
        let has_abort = decisions.values().any(|d| *d == TransactionDecision::Abort);
        let has_unknown = decisions
            .values()
            .any(|d| *d == TransactionDecision::Unknown);

        if has_abort && !has_unknown {
            // All known decisions are abort
            return TransactionDecision::Abort;
        }

        // Default to abort if we can't determine (safe choice)
        TransactionDecision::Abort
    }

    /// Create a recovery decision message
    pub fn create_recovery_message(&self, txn_id: &str, decision: TransactionDecision) -> Message {
        let phase = match decision {
            TransactionDecision::Commit => "commit",
            TransactionDecision::Abort => "abort",
            TransactionDecision::Unknown => return Message::with_body(vec![]), // Shouldn't happen
        };

        Message::with_body(vec![])
            .with_header("txn_id".to_string(), txn_id.to_string())
            .with_header("txn_phase".to_string(), phase.to_string())
            .with_header("recovery".to_string(), "true".to_string())
            .with_header("participant".to_string(), self.stream_name.clone())
    }

    /// Clean up completed recoveries
    pub fn cleanup_completed(&mut self) {
        self.recovery_states
            .retain(|_, state| !matches!(state, RecoveryState::Completed { .. }));
    }
}
