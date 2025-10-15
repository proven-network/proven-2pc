//! Entity and Delta definitions for SQL MVCC storage
//!
//! This module defines the MvccEntity implementations for SQL tables and indexes.
//! Key design decisions:
//! - Value = Vec<u8> (opaque bytes, schema-aware encoding done at Storage layer)
//! - One MvccStorage<TableEntity> per table
//! - One MvccStorage<IndexEntity> per index
//! - Deltas contain pre-encoded row bytes (preserves 36.1 bytes/row optimization)

use proven_mvcc::{Decode, Encode, Error as MvccError, MvccDelta, MvccEntity};

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
#[derive(Clone, Debug, PartialEq)]
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
        use std::io::Write;
        let mut buf = Vec::new();

        match self {
            TableDelta::Insert {
                row_id,
                encoded_row,
            } => {
                buf.push(1); // Tag for Insert
                buf.write_all(&row_id.to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(&(encoded_row.len() as u32).to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(encoded_row)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
            }
            TableDelta::Update {
                row_id,
                encoded_old,
                encoded_new,
            } => {
                buf.push(2); // Tag for Update
                buf.write_all(&row_id.to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(&(encoded_old.len() as u32).to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(encoded_old)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(&(encoded_new.len() as u32).to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(encoded_new)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
            }
            TableDelta::Delete {
                row_id,
                encoded_old,
            } => {
                buf.push(3); // Tag for Delete
                buf.write_all(&row_id.to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(&(encoded_old.len() as u32).to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                buf.write_all(encoded_old)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
            }
        }
        Ok(buf)
    }
}

impl Decode for TableDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(bytes);
        let mut tag = [0u8; 1];
        cursor
            .read_exact(&mut tag)
            .map_err(|e| MvccError::Encoding(e.to_string()))?;

        match tag[0] {
            1 => {
                // Insert
                let mut row_id_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut row_id_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let row_id = u64::from_be_bytes(row_id_bytes);

                let mut len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut len_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let len = u32::from_be_bytes(len_bytes) as usize;

                let mut encoded_row = vec![0u8; len];
                cursor
                    .read_exact(&mut encoded_row)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                Ok(TableDelta::Insert {
                    row_id,
                    encoded_row,
                })
            }
            2 => {
                // Update
                let mut row_id_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut row_id_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let row_id = u64::from_be_bytes(row_id_bytes);

                let mut len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut len_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let old_len = u32::from_be_bytes(len_bytes) as usize;

                let mut encoded_old = vec![0u8; old_len];
                cursor
                    .read_exact(&mut encoded_old)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                cursor
                    .read_exact(&mut len_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let new_len = u32::from_be_bytes(len_bytes) as usize;

                let mut encoded_new = vec![0u8; new_len];
                cursor
                    .read_exact(&mut encoded_new)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                Ok(TableDelta::Update {
                    row_id,
                    encoded_old,
                    encoded_new,
                })
            }
            3 => {
                // Delete
                let mut row_id_bytes = [0u8; 8];
                cursor
                    .read_exact(&mut row_id_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let row_id = u64::from_be_bytes(row_id_bytes);

                let mut len_bytes = [0u8; 4];
                cursor
                    .read_exact(&mut len_bytes)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;
                let len = u32::from_be_bytes(len_bytes) as usize;

                let mut encoded_old = vec![0u8; len];
                cursor
                    .read_exact(&mut encoded_old)
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                Ok(TableDelta::Delete {
                    row_id,
                    encoded_old,
                })
            }
            _ => Err(MvccError::Encoding(format!(
                "Unknown TableDelta tag: {}",
                tag[0]
            ))),
        }
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
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
#[derive(Clone, Debug, PartialEq)]
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
        // Use sortable encoding for index keys (required for MVCC key comparisons)
        let encoded = crate::storage::codec::encode_index_key(&self.values, self.row_id);
        Ok(encoded)
    }
}

impl Decode for IndexKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        // Index keys are sortable-encoded: sortable_values || row_id (8 bytes big-endian)
        // We can extract row_id but not the values (would need schema).
        // This is OK since decode is only used for recovery, and we reconstruct
        // the IndexKey from deltas which include the full values.
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

        // Values remain empty - they're not needed for MVCC key operations
        // and will be populated from deltas during recovery
        Ok(IndexKey {
            values: vec![],
            row_id,
        })
    }
}

// Encode/Decode for IndexDelta
// Note: We store values separately (not using sortable encoding) so IndexKey can be fully reconstructed
impl Encode for IndexDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        use std::io::Write;
        let mut buf = Vec::new();

        match self {
            IndexDelta::Insert { key } | IndexDelta::Delete { key } => {
                // Tag
                buf.push(match self {
                    IndexDelta::Insert { .. } => 1,
                    IndexDelta::Delete { .. } => 2,
                });

                // Encode row_id
                buf.write_all(&key.row_id.to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                // Encode values count
                buf.write_all(&(key.values.len() as u32).to_be_bytes())
                    .map_err(|e| MvccError::Encoding(e.to_string()))?;

                // Encode each value using proven_value encoding
                for value in &key.values {
                    let encoded = proven_value::encode_value(value);
                    buf.write_all(&(encoded.len() as u32).to_be_bytes())
                        .map_err(|e| MvccError::Encoding(e.to_string()))?;
                    buf.write_all(&encoded)
                        .map_err(|e| MvccError::Encoding(e.to_string()))?;
                }
            }
        }
        Ok(buf)
    }
}

impl Decode for IndexDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use std::io::{Cursor, Read};

        if bytes.is_empty() {
            return Err(MvccError::Encoding("IndexDelta is empty".to_string()));
        }

        let mut cursor = Cursor::new(bytes);
        let mut tag = [0u8; 1];
        cursor
            .read_exact(&mut tag)
            .map_err(|e| MvccError::Encoding(e.to_string()))?;

        // Decode row_id
        let mut row_id_bytes = [0u8; 8];
        cursor
            .read_exact(&mut row_id_bytes)
            .map_err(|e| MvccError::Encoding(e.to_string()))?;
        let row_id = u64::from_be_bytes(row_id_bytes);

        // Decode values count
        let mut len_bytes = [0u8; 4];
        cursor
            .read_exact(&mut len_bytes)
            .map_err(|e| MvccError::Encoding(e.to_string()))?;
        let num_values = u32::from_be_bytes(len_bytes) as usize;

        // Decode each value
        let mut values = Vec::with_capacity(num_values);
        for _ in 0..num_values {
            cursor
                .read_exact(&mut len_bytes)
                .map_err(|e| MvccError::Encoding(e.to_string()))?;
            let value_len = u32::from_be_bytes(len_bytes) as usize;

            let pos = cursor.position() as usize;
            let remaining = &cursor.get_ref()[pos..];
            if remaining.len() < value_len {
                return Err(MvccError::Encoding(format!(
                    "IndexDelta value truncated: expected {} bytes, got {}",
                    value_len,
                    remaining.len()
                )));
            }

            let value = proven_value::decode_value(remaining)
                .map_err(|e| MvccError::Encoding(e.to_string()))?;
            cursor.set_position(pos as u64 + value_len as u64);

            values.push(value);
        }

        let key = IndexKey { values, row_id };

        match tag[0] {
            1 => Ok(IndexDelta::Insert { key }),
            2 => Ok(IndexDelta::Delete { key }),
            _ => Err(MvccError::Encoding(format!(
                "Unknown IndexDelta tag: {}",
                tag[0]
            ))),
        }
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

        // IndexDelta now stores values separately so it can be fully reconstructed
        assert_eq!(delta, decoded);
    }
}
