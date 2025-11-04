//! Transaction state machine and types

use proven_common::{Timestamp, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Complete state for a single transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionState<O = ()> {
    /// Coordinator ID for sending responses
    pub coordinator_id: String,

    /// Deadline for recovery
    pub deadline: Timestamp,

    /// Other participants in this transaction (stream_name -> start_offset)
    pub participants: HashMap<String, u64>,

    /// Current lifecycle phase
    pub phase: TransactionPhase,

    /// Deferred operations waiting to retry (NEW - for crash recovery)
    /// Persisted so operations survive crashes
    #[serde(bound(deserialize = "O: for<'de2> Deserialize<'de2>"))]
    pub deferred_operations: Vec<crate::transaction::DeferredOp<O>>,
}

impl<O> TransactionState<O> {
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
            deferred_operations: Vec::new(),
        }
    }

    /// Check if this transaction is still active (not completed)
    #[cfg(test)]
    pub fn is_active(&self) -> bool {
        matches!(
            self.phase,
            TransactionPhase::Active | TransactionPhase::Prepared
        )
    }

    /// Check if this transaction is prepared
    #[cfg(test)]
    pub fn is_prepared(&self) -> bool {
        matches!(self.phase, TransactionPhase::Prepared)
    }

    /// Check if this transaction is completed (committed or aborted)
    #[cfg(test)]
    pub fn is_completed(&self) -> bool {
        matches!(
            self.phase,
            TransactionPhase::Committed | TransactionPhase::Aborted { .. }
        )
    }

    /// Check if this transaction was wounded
    #[cfg(test)]
    pub fn is_wounded(&self) -> Option<TransactionId> {
        match self.phase {
            TransactionPhase::Aborted {
                reason: AbortReason::Wounded { by },
                ..
            } => Some(by),
            _ => None,
        }
    }

    /// Serialize to bytes for persistence
    pub fn to_bytes(&self) -> Result<Vec<u8>, String>
    where
        O: serde::Serialize,
    {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(self, &mut bytes)
            .map_err(|e| format!("Failed to serialize transaction state: {}", e))?;
        Ok(bytes)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        O: for<'de> serde::Deserialize<'de>,
    {
        ciborium::de::from_reader(bytes)
            .map_err(|e| format!("Failed to deserialize transaction state: {}", e))
    }

    /// Get metadata key for this transaction
    /// Note: Generic parameter O is not used, just for type safety
    pub fn metadata_key(txn_id: TransactionId) -> Vec<u8> {
        let mut key = b"_txn_meta_".to_vec();
        key.extend_from_slice(&txn_id.to_bytes());
        key
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedInfo {
    pub coordinator_id: String,
    pub phase: TransactionPhase,
    pub completed_at: Timestamp,
    pub deadline: Timestamp, // NEW: for deadline-based GC
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
        let state = TransactionState::<()> {
            coordinator_id: "test-coord".to_string(),
            deadline: Timestamp::from_micros(5000),
            participants: {
                let mut map = HashMap::new();
                map.insert("stream1".to_string(), 10);
                map.insert("stream2".to_string(), 20);
                map
            },
            phase: TransactionPhase::Prepared,
            deferred_operations: Vec::new(),
        };

        let json = serde_json::to_string(&state).unwrap();
        let deserialized: TransactionState<()> = serde_json::from_str(&json).unwrap();

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
