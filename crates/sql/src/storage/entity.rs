//! Entity and Delta definitions for SQL MVCC storage
//!
//! This module defines the MvccEntity implementations for SQL tables and indexes.
//! Key design decisions:
//! - Value = Vec<u8> (opaque bytes, schema-aware encoding done at Storage layer)
//! - One MvccStorage<TableEntity> per table
//! - One MvccStorage<IndexEntity> per index
//! - Deltas contain pre-encoded row bytes (preserves 36.1 bytes/row optimization)

use proven_mvcc::{Decode, Encode, Error as MvccError, MvccDelta, MvccEntity};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, MvccError>;

// ============================================================================
// Table Entity
// ============================================================================

/// Entity type for table data
///
/// Each table gets its own MvccStorage<TableEntity> instance with separate partitions.
/// The Key is just the RowId since the table is implicit (per-storage-instance).
pub struct TableEntity;

impl MvccEntity for TableEntity {
    type Key = u64; // RowId (table is implicit)
    type Value = Vec<u8>; // Opaque encoded row bytes
    type Delta = TableDelta;

    fn entity_name() -> &'static str {
        "table_data"
    }
}

/// Delta operations for table data
///
/// Contains pre-encoded row bytes to preserve schema-aware encoding optimization.
/// The Storage layer encodes rows with schema before creating these deltas.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TableDelta {
    /// Insert a new row
    Insert {
        row_id: u64,
        encoded_row: Vec<u8>, // Pre-encoded with schema
    },
    /// Update an existing row
    Update {
        row_id: u64,
        encoded_old: Vec<u8>, // For unapply
        encoded_new: Vec<u8>,
    },
    /// Delete a row
    Delete {
        row_id: u64,
        encoded_old: Vec<u8>, // For unapply
    },
}

impl MvccDelta<TableEntity> for TableDelta {
    fn key(&self) -> u64 {
        match self {
            TableDelta::Insert { row_id, .. }
            | TableDelta::Update { row_id, .. }
            | TableDelta::Delete { row_id, .. } => *row_id,
        }
    }

    fn apply(&self, _current: Option<Vec<u8>>) -> Option<Vec<u8>> {
        match self {
            TableDelta::Insert { encoded_row, .. } => Some(encoded_row.clone()),
            TableDelta::Update { encoded_new, .. } => Some(encoded_new.clone()),
            TableDelta::Delete { .. } => None,
        }
    }

    fn unapply(&self, _current: Option<Vec<u8>>) -> Option<Vec<u8>> {
        match self {
            TableDelta::Insert { .. } => None,
            TableDelta::Update { encoded_old, .. } => Some(encoded_old.clone()),
            TableDelta::Delete { encoded_old, .. } => Some(encoded_old.clone()),
        }
    }

    fn merge(self, next: Self) -> Self {
        // For coarse-grained table deltas (full row operations), last write wins
        // The later delta completely replaces the earlier one
        // Examples:
        // - Insert then Update = Update (with Insert's old value)
        // - Update then Delete = Delete (with original old value)
        // - Insert then Delete = could be optimized to nothing, but we keep Delete for history
        match (self, next) {
            // Insert followed by Update -> becomes Update with Insert's row_id
            (TableDelta::Insert { row_id, .. }, TableDelta::Update { encoded_new, .. }) => {
                // The "old" value for this update is None (didn't exist before insert)
                // But we need encoded_old for unapply, so use empty vec to represent "didn't exist"
                TableDelta::Update {
                    row_id,
                    encoded_old: vec![],
                    encoded_new,
                }
            }
            // Insert followed by Delete -> Delete wins (row created then deleted)
            (TableDelta::Insert { row_id, .. }, TableDelta::Delete { .. }) => TableDelta::Delete {
                row_id,
                encoded_old: vec![],
            },
            // Update followed by Update -> last Update wins, keep original old value
            (
                TableDelta::Update {
                    row_id,
                    encoded_old,
                    ..
                },
                TableDelta::Update { encoded_new, .. },
            ) => TableDelta::Update {
                row_id,
                encoded_old,
                encoded_new,
            },
            // Update followed by Delete -> Delete wins, keep original old value
            (
                TableDelta::Update {
                    row_id,
                    encoded_old,
                    ..
                },
                TableDelta::Delete { .. },
            ) => TableDelta::Delete {
                row_id,
                encoded_old,
            },
            // Any other combination (including same operation type for Insert/Delete) - next wins
            (_, next) => next,
        }
    }
}

// Encode/Decode for TableDelta (u64 and Vec<u8> are implemented in proven-mvcc)
impl Encode for TableDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

impl Decode for TableDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

// ============================================================================
// Index Entity
// ============================================================================

/// Entity type for index data
///
/// Each index gets its own MvccStorage<IndexEntity> instance with separate partitions.
pub struct IndexEntity;

/// Index key contains the indexed values plus row_id for uniqueness
///
/// Example for index on (name, age):
/// IndexKey { values: [Value::String("Alice"), Value::I32(25)], row_id: 123 }
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexKey {
    /// The indexed column values (in index column order)
    pub values: Vec<crate::types::Value>,
    /// The row_id for uniqueness (also points back to table row)
    pub row_id: u64,
}

impl MvccEntity for IndexEntity {
    type Key = IndexKey;
    type Value = (); // Index entries have no separate value (key contains everything)
    type Delta = IndexDelta;

    fn entity_name() -> &'static str {
        "index_data"
    }
}

/// Delta operations for index data
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IndexDelta {
    /// Insert index entry
    Insert { key: IndexKey },
    /// Delete index entry
    Delete { key: IndexKey },
}

impl MvccDelta<IndexEntity> for IndexDelta {
    fn key(&self) -> IndexKey {
        match self {
            IndexDelta::Insert { key } | IndexDelta::Delete { key } => key.clone(),
        }
    }

    fn apply(&self, _current: Option<()>) -> Option<()> {
        match self {
            IndexDelta::Insert { .. } => Some(()),
            IndexDelta::Delete { .. } => None,
        }
    }

    fn unapply(&self, _current: Option<()>) -> Option<()> {
        match self {
            IndexDelta::Insert { .. } => None,
            IndexDelta::Delete { .. } => Some(()),
        }
    }

    fn merge(self, next: Self) -> Self {
        // For index entries, last operation wins
        // Insert followed by Delete = Delete (entry added then removed)
        // Delete followed by Insert = Insert (entry removed then re-added)
        // Same operation twice = keep last one
        next
    }
}

// Encode/Decode for IndexKey
impl Encode for IndexKey {
    fn encode(&self) -> Result<Vec<u8>> {
        // Use existing optimized sortable encoding for index keys!
        let encoded = crate::storage::encoding::encode_index_key(&self.values, self.row_id);
        Ok(encoded)
    }
}

impl Decode for IndexKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        // Index keys are encoded as: sortable_values || row_id (8 bytes big-endian)
        // We need at least 8 bytes for the row_id
        if bytes.len() < 8 {
            return Err(MvccError::Encoding(format!(
                "IndexKey too short: {} bytes (need at least 8 for row_id)",
                bytes.len()
            )));
        }

        // Extract row_id from last 8 bytes
        let row_id_start = bytes.len() - 8;
        let mut row_id_bytes = [0u8; 8];
        row_id_bytes.copy_from_slice(&bytes[row_id_start..]);
        let row_id = u64::from_be_bytes(row_id_bytes);

        // For now, we don't decode the values since we don't know the schema
        // We only need row_id for lookups anyway
        Ok(IndexKey {
            values: vec![], // Values not decoded (not needed for row_id extraction)
            row_id,
        })
    }
}

// Encode/Decode for IndexDelta (()  is implemented in proven-mvcc)
impl Encode for IndexDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

impl Decode for IndexDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| MvccError::Encoding(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_delta_roundtrip() {
        let delta = TableDelta::Insert {
            row_id: 123,
            encoded_row: vec![1, 2, 3, 4, 5],
        };

        let bytes = delta.encode().unwrap();
        let decoded = TableDelta::decode(&bytes).unwrap();

        assert_eq!(delta, decoded);
    }

    #[test]
    fn test_table_delta_apply() {
        let delta = TableDelta::Insert {
            row_id: 123,
            encoded_row: vec![1, 2, 3],
        };

        let result = delta.apply(None);
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_table_delta_unapply() {
        let delta = TableDelta::Update {
            row_id: 123,
            encoded_old: vec![1, 2, 3],
            encoded_new: vec![4, 5, 6],
        };

        let result = delta.unapply(Some(vec![4, 5, 6]));
        assert_eq!(result, Some(vec![1, 2, 3]));
    }

    #[test]
    fn test_index_delta_roundtrip() {
        use crate::types::Value;

        let delta = IndexDelta::Insert {
            key: IndexKey {
                values: vec![Value::string("test"), Value::integer(42)],
                row_id: 123,
            },
        };

        let bytes = delta.encode().unwrap();
        let decoded = IndexDelta::decode(&bytes).unwrap();

        assert_eq!(delta, decoded);
    }
}
