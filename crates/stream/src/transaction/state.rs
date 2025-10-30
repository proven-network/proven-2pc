//! Transaction state machine and types

use proven_common::{Timestamp, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete state for a single transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionState {
    /// Coordinator ID for sending responses
    pub coordinator_id: String,

    /// Deadline for recovery
    pub deadline: Timestamp,

    /// Other participants in this transaction (stream_name -> start_offset)
    pub participants: HashMap<String, u64>,

    /// Current lifecycle phase
    pub phase: TransactionPhase,
}

impl TransactionState {
    /// Create a new active transaction
    pub fn new(
        coordinator_id: String,
        deadline: Timestamp,
        participants: HashMap<String, u64>,
    ) -> Self {
        Self {
            coordinator_id,
            deadline,
            participants,
            phase: TransactionPhase::Active,
        }
    }

    /// Check if this transaction is still active (not completed)
    pub fn is_active(&self) -> bool {
        matches!(
            self.phase,
            TransactionPhase::Active | TransactionPhase::Prepared
        )
    }

    /// Check if this transaction is prepared
    pub fn is_prepared(&self) -> bool {
        matches!(self.phase, TransactionPhase::Prepared)
    }

    /// Check if this transaction is completed (committed or aborted)
    pub fn is_completed(&self) -> bool {
        matches!(
            self.phase,
            TransactionPhase::Committed | TransactionPhase::Aborted { .. }
        )
    }

    /// Check if this transaction was wounded
    pub fn is_wounded(&self) -> Option<TransactionId> {
        match self.phase {
            TransactionPhase::Aborted {
                reason: AbortReason::Wounded { by },
                ..
            } => Some(by),
            _ => None,
        }
    }
}

/// Clear lifecycle phases for a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionPhase {
    /// Transaction begun, operations being applied
    Active,

    /// Prepared - read locks released, write locks held
    /// Can participate in recovery from this point
    Prepared,

    /// Committed - all locks released, state can be GC'd
    Committed,

    /// Aborted - all locks released
    /// Keep state briefly to handle late messages, then GC
    Aborted {
        /// When aborted (for GC)
        aborted_at: Timestamp,

        /// Why aborted (for debugging and late responses)
        reason: AbortReason,
    },
}

/// Reason why a transaction was aborted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortReason {
    /// Wounded by an older transaction
    Wounded { by: TransactionId },

    /// Exceeded deadline
    DeadlineExceeded,

    /// Coordinator requested abort
    Explicit,

    /// Recovery decision
    Recovery,
}

/// Information about a completed transaction (for late message handling)
#[derive(Debug, Clone)]
pub struct CompletedInfo {
    pub coordinator_id: String,
    pub phase: TransactionPhase,
    pub completed_at: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_state() -> TransactionState {
        TransactionState::new(
            "coord-1".to_string(),
            Timestamp::from_micros(1000),
            HashMap::new(),
        )
    }

    #[test]
    fn test_new_transaction_is_active() {
        let state = make_state();
        assert_eq!(state.phase, TransactionPhase::Active);
        assert!(state.is_active());
        assert!(!state.is_prepared());
        assert!(!state.is_completed());
    }

    #[test]
    fn test_prepared_state() {
        let mut state = make_state();
        state.phase = TransactionPhase::Prepared;

        assert!(state.is_active());
        assert!(state.is_prepared());
        assert!(!state.is_completed());
    }

    #[test]
    fn test_committed_state() {
        let mut state = make_state();
        state.phase = TransactionPhase::Committed;

        assert!(!state.is_active());
        assert!(!state.is_prepared());
        assert!(state.is_completed());
    }

    #[test]
    fn test_aborted_state() {
        let mut state = make_state();
        state.phase = TransactionPhase::Aborted {
            aborted_at: Timestamp::from_micros(2000),
            reason: AbortReason::Explicit,
        };

        assert!(!state.is_active());
        assert!(!state.is_prepared());
        assert!(state.is_completed());
        assert!(state.is_wounded().is_none());
    }

    #[test]
    fn test_wounded_state() {
        let mut state = make_state();
        let wounded_by = TransactionId::new();
        state.phase = TransactionPhase::Aborted {
            aborted_at: Timestamp::from_micros(2000),
            reason: AbortReason::Wounded { by: wounded_by },
        };

        assert!(state.is_completed());
        assert_eq!(state.is_wounded(), Some(wounded_by));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let state = TransactionState {
            coordinator_id: "test-coord".to_string(),
            deadline: Timestamp::from_micros(5000),
            participants: {
                let mut map = HashMap::new();
                map.insert("stream1".to_string(), 10);
                map.insert("stream2".to_string(), 20);
                map
            },
            phase: TransactionPhase::Prepared,
        };

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: TransactionState = serde_json::from_str(&json).unwrap();

        assert_eq!(state.coordinator_id, deserialized.coordinator_id);
        assert_eq!(state.deadline, deserialized.deadline);
        assert_eq!(state.participants, deserialized.participants);
        assert_eq!(state.phase, deserialized.phase);
    }

    #[test]
    fn test_abort_reason_wounded_serialization() {
        let reason = AbortReason::Wounded {
            by: TransactionId::new(),
        };

        let json = serde_json::to_string(&reason).unwrap();
        let deserialized: AbortReason = serde_json::from_str(&json).unwrap();

        assert_eq!(reason, deserialized);
    }
}
