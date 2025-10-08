//! Entity and Delta definitions for Queue MVCC storage
//!
//! This module defines the MvccEntity implementation for queues.
//! Key design decisions:
//! - Key = u64 (entry_id for FIFO ordering)
//! - Value = QueueValue (the actual queue data)
//! - Deltas track Enqueue/Dequeue/Clear operations
//! - Head/tail pointers stored in metadata for FIFO semantics

use crate::types::QueueValue;
use proven_hlc::HlcTimestamp;
use proven_mvcc::{Decode, Encode, Error as MvccError, MvccDelta, MvccEntity};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, MvccError>;

// ============================================================================
// Queue Entity
// ============================================================================

/// Entity type for queue data
///
/// Each queue gets its own MvccStorage<QueueEntity> instance with separate partitions.
/// The Key is the entry_id (monotonically increasing for FIFO ordering).
pub struct QueueEntity;

impl MvccEntity for QueueEntity {
    type Key = u64; // entry_id (for FIFO ordering)
    type Value = QueueValue; // Queue value
    type Delta = QueueDelta;

    fn entity_name() -> &'static str {
        "queue"
    }
}

/// Delta operations for queue data
///
/// Tracks enqueue, dequeue, and clear operations for time-travel and recovery.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum QueueDelta {
    /// Enqueue a value to the back of the queue
    Enqueue {
        entry_id: u64,
        value: QueueValue,
        enqueued_at: HlcTimestamp,
    },
    /// Dequeue a value from the front of the queue
    Dequeue {
        entry_id: u64,
        old_value: QueueValue, // For unapply
    },
    /// Clear all values from the queue
    /// Note: This stores all cleared entries for unapply (can be expensive)
    Clear {
        cleared_entries: Vec<(u64, QueueValue)>, // For unapply
    },
}

impl MvccDelta<QueueEntity> for QueueDelta {
    fn key(&self) -> u64 {
        match self {
            QueueDelta::Enqueue { entry_id, .. } => *entry_id,
            QueueDelta::Dequeue { entry_id, .. } => *entry_id,
            // For Clear, we use 0 as a sentinel (won't be used for lookups)
            // Clear deltas are special and affect multiple keys
            QueueDelta::Clear { .. } => 0,
        }
    }

    fn apply(&self, _current: Option<QueueValue>) -> Option<QueueValue> {
        match self {
            QueueDelta::Enqueue { value, .. } => Some(value.clone()),
            QueueDelta::Dequeue { .. } => None, // Entry is removed
            QueueDelta::Clear { .. } => None,   // Entry is removed
        }
    }

    fn unapply(&self, _current: Option<QueueValue>) -> Option<QueueValue> {
        match self {
            QueueDelta::Enqueue { .. } => None, // Didn't exist before enqueue
            QueueDelta::Dequeue { old_value, .. } => Some(old_value.clone()), // Restore
            QueueDelta::Clear { .. } => {
                // Clear is complex - each entry needs individual handling
                // For simplicity, we don't support full time-travel for Clear
                // (would need to track each cleared entry separately)
                None
            }
        }
    }

    fn merge(self, next: Self) -> Self {
        // For queue operations, merging is straightforward:
        // - Enqueue then Dequeue on same entry = Dequeue wins (but keep old_value as None)
        // - Clear always wins (clears everything)
        // - Otherwise, last operation wins
        match (self, next) {
            // Enqueue followed by Dequeue on same entry
            (
                QueueDelta::Enqueue {
                    entry_id, value, ..
                },
                QueueDelta::Dequeue {
                    entry_id: next_id, ..
                },
            ) if entry_id == next_id => QueueDelta::Dequeue {
                entry_id,
                old_value: value, // Use the enqueued value as old_value
            },
            // Clear always wins
            (_, clear @ QueueDelta::Clear { .. }) => clear,
            // Otherwise, next wins (last write semantics)
            (_, next) => next,
        }
    }
}

// Encode/Decode for QueueDelta
impl Encode for QueueDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

impl Decode for QueueDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

// Note: Encode/Decode for QueueValue (proven_value::Value) is implemented in proven-value crate
// Note: u64 Encode/Decode is already implemented in proven-mvcc

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::NodeId;

    fn create_timestamp(seconds: u64) -> HlcTimestamp {
        HlcTimestamp::new(seconds, 0, NodeId::new(1))
    }

    #[test]
    fn test_enqueue_delta() {
        let delta = QueueDelta::Enqueue {
            entry_id: 1,
            value: QueueValue::Str("test".to_string()),
            enqueued_at: create_timestamp(100),
        };

        assert_eq!(delta.key(), 1);
        assert_eq!(delta.apply(None), Some(QueueValue::Str("test".to_string())));
        assert_eq!(delta.unapply(None), None);
    }

    #[test]
    fn test_dequeue_delta() {
        let delta = QueueDelta::Dequeue {
            entry_id: 1,
            old_value: QueueValue::I64(42),
        };

        assert_eq!(delta.key(), 1);
        assert_eq!(delta.apply(Some(QueueValue::I64(42))), None);
        assert_eq!(delta.unapply(None), Some(QueueValue::I64(42)));
    }

    #[test]
    fn test_delta_merge_enqueue_dequeue() {
        let enqueue = QueueDelta::Enqueue {
            entry_id: 1,
            value: QueueValue::Bool(true),
            enqueued_at: create_timestamp(100),
        };

        let dequeue = QueueDelta::Dequeue {
            entry_id: 1,
            old_value: QueueValue::Bool(false), // Will be replaced
        };

        let merged = enqueue.merge(dequeue);

        match merged {
            QueueDelta::Dequeue {
                entry_id,
                old_value,
            } => {
                assert_eq!(entry_id, 1);
                assert_eq!(old_value, QueueValue::Bool(true)); // From enqueue
            }
            _ => panic!("Expected Dequeue delta"),
        }
    }

    #[test]
    fn test_encode_decode_delta() {
        let delta = QueueDelta::Enqueue {
            entry_id: 123,
            value: QueueValue::Str("hello".to_string()),
            enqueued_at: create_timestamp(100),
        };

        let encoded = delta.encode().unwrap();
        let decoded = QueueDelta::decode(&encoded).unwrap();

        assert_eq!(delta, decoded);
    }

    #[test]
    fn test_encode_decode_value() {
        // Test with String instead of Json (bincode has issues with serde_json::Value)
        let value = QueueValue::Str("test value".to_string());

        let encoded = value.encode().unwrap();
        let decoded = QueueValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }
}
