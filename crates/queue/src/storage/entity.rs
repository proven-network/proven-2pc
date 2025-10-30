//! Entity and Delta definitions for Queue MVCC storage
//!
//! This module defines the MvccEntity implementation for queues.
//! Key design decisions:
//! - Key = u64 (entry_id for FIFO ordering)
//! - Value = QueueValue (the actual queue data)
//! - Deltas track Enqueue/Dequeue/Clear operations
//! - Head/tail pointers stored in metadata for FIFO semantics

use crate::types::QueueValue;
use proven_common::TransactionId;
use proven_mvcc::{Decode, Encode, Error as MvccError, MvccDelta, MvccEntity};

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
#[derive(Clone, Debug, PartialEq)]
pub enum QueueDelta {
    /// Enqueue a value to the back of the queue
    Enqueue {
        entry_id: u64,
        value: QueueValue,
        enqueued_at: TransactionId,
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
        use proven_value::encode_value;

        let mut buf = Vec::new();
        match self {
            QueueDelta::Enqueue {
                entry_id,
                value,
                enqueued_at,
            } => {
                buf.push(1); // Tag for Enqueue
                buf.extend_from_slice(&entry_id.to_be_bytes());
                buf.extend_from_slice(&encode_value(value));
                // Encode TransactionId using lexicographic encoding
                buf.extend_from_slice(&enqueued_at.to_bytes());
            }
            QueueDelta::Dequeue {
                entry_id,
                old_value,
            } => {
                buf.push(2); // Tag for Dequeue
                buf.extend_from_slice(&entry_id.to_be_bytes());
                buf.extend_from_slice(&encode_value(old_value));
            }
            QueueDelta::Clear { cleared_entries } => {
                buf.push(3); // Tag for Clear
                buf.extend_from_slice(&(cleared_entries.len() as u32).to_be_bytes());
                for (id, value) in cleared_entries {
                    buf.extend_from_slice(&id.to_be_bytes());
                    buf.extend_from_slice(&encode_value(value));
                }
            }
        }
        Ok(buf)
    }
}

impl Decode for QueueDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use proven_value::decode_value;
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(bytes);
        let mut tag = [0u8; 1];
        cursor
            .read_exact(&mut tag)
            .map_err(|e| MvccError::Encoding(e.to_string()))?;

        match tag[0] {
            1 => {
                // Enqueue
                let mut entry_id_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut entry_id_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let entry_id = u64::from_be_bytes(entry_id_bytes);

                let pos = cursor.position() as usize;
                let remaining = &cursor.get_ref()[pos..];
                let value =
                    decode_value(remaining).map_err(|e| MvccError::Encoding(e.to_string()))?;
                let value_encoded = proven_value::encode_value(&value);
                cursor.set_position((pos + value_encoded.len()) as u64);

                // Decode TransactionId (16 bytes for UUIDv7)
                let mut ts_bytes = [0u8; 16];
                cursor
                    .read_exact(&mut ts_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let enqueued_at = TransactionId::from_bytes(ts_bytes);

                Ok(QueueDelta::Enqueue {
                    entry_id,
                    value,
                    enqueued_at,
                })
            }
            2 => {
                // Dequeue
                let mut entry_id_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut entry_id_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let entry_id = u64::from_be_bytes(entry_id_bytes);

                let pos = cursor.position() as usize;
                let remaining = &cursor.get_ref()[pos..];
                let old_value =
                    decode_value(remaining).map_err(|e| MvccError::Encoding(e.to_string()))?;

                Ok(QueueDelta::Dequeue {
                    entry_id,
                    old_value,
                })
            }
            3 => {
                // Clear
                let mut len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut len_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let len = u32::from_be_bytes(len_bytes) as usize;

                let mut cleared_entries = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut entry_id_bytes = [0u8; 8];
                    cursor
                        .read_exact(&mut entry_id_bytes)
                        .map_err(|e| MvccError::Encoding(e.to_string()))?;
                    let entry_id = u64::from_be_bytes(entry_id_bytes);

                    let pos = cursor.position() as usize;
                    let remaining = &cursor.get_ref()[pos..];
                    let value =
                        decode_value(remaining).map_err(|e| MvccError::Encoding(e.to_string()))?;
                    let value_encoded = proven_value::encode_value(&value);
                    cursor.set_position((pos + value_encoded.len()) as u64);

                    cleared_entries.push((entry_id, value));
                }

                Ok(QueueDelta::Clear { cleared_entries })
            }
            _ => Err(MvccError::Encoding(format!(
                "Unknown delta tag: {}",
                tag[0]
            ))),
        }
    }
}

// Note: Encode/Decode for QueueValue (proven_value::Value) is implemented in proven-value crate
// Note: u64 Encode/Decode is already implemented in proven-mvcc

#[cfg(test)]
mod tests {
    use super::*;

    fn create_timestamp() -> TransactionId {
        TransactionId::new()
    }

    #[test]
    fn test_enqueue_delta() {
        let delta = QueueDelta::Enqueue {
            entry_id: 1,
            value: QueueValue::Str("test".to_string()),
            enqueued_at: create_timestamp(),
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
            enqueued_at: create_timestamp(),
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
            enqueued_at: create_timestamp(),
        };

        let encoded = delta.encode().unwrap();
        let decoded = QueueDelta::decode(&encoded).unwrap();

        assert_eq!(delta, decoded);
    }

    #[test]
    fn test_encode_decode_value() {
        let value = QueueValue::Str("test value".to_string());

        let encoded = value.encode().unwrap();
        let decoded = QueueValue::decode(&encoded).unwrap();

        assert_eq!(value, decoded);
    }
}
