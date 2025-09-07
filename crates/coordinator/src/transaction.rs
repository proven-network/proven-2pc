//! Transaction state and metadata types
//!
//! This module defines the core transaction types used by the coordinator
//! to track distributed transaction state across multiple participants.

use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};

/// Transaction state in the coordinator
///
/// Represents the lifecycle of a distributed transaction:
/// Active -> Preparing -> Prepared -> Committing -> Committed
///                    \-> Aborting -> Aborted
#[derive(Debug, Clone)]
pub enum TransactionState {
    /// Transaction is active and accepting operations
    Active,
    /// Prepare phase has started, waiting for votes
    Preparing,
    /// All participants have voted to prepare
    Prepared,
    /// Commit phase has started
    Committing,
    /// Transaction has been committed on all participants
    Committed,
    /// Abort phase has started
    Aborting,
    /// Transaction has been aborted on all participants
    Aborted,
}

/// Transaction metadata tracked by the coordinator
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Unique transaction identifier
    pub id: String,

    /// Current state of the transaction
    pub state: TransactionState,

    /// List of participating streams
    /// TODO: In phase 2, this will be built dynamically as operations are sent
    pub participants: Vec<String>,

    /// HLC timestamp for the transaction
    pub timestamp: HlcTimestamp,

    /// Transaction deadline (timestamp + timeout)
    pub deadline: HlcTimestamp,

    /// Prepare votes collected from participants
    pub prepare_votes: HashMap<String, PrepareVote>,

    /// Track which streams have received the deadline
    pub streams_with_deadline: HashSet<String>,

    /// Track which participants each stream knows about
    /// Key: stream name, Value: set of other participants it has been told about
    pub participant_awareness: HashMap<String, HashSet<String>>,

    /// Log offset when each participant first received an operation
    /// Used as a hint for where to start looking during recovery
    pub participant_offsets: HashMap<String, u64>,
}

/// Prepare vote from a participant during 2PC
#[derive(Debug, Clone)]
pub enum PrepareVote {
    /// Participant successfully prepared
    Prepared,

    /// Participant was wounded by another transaction
    Wounded {
        /// Transaction that caused the wound
        wounded_by: String,
    },

    /// Participant encountered an error
    Error(String),
}
