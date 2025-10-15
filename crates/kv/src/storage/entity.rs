//! KV entity implementation for proven-mvcc
//!
//! This module defines how KV data is stored in the generic MVCC storage layer.

use crate::types::Value;
use proven_mvcc::{Decode, Encode, MvccDelta, MvccEntity, Result};

/// KV entity for proven-mvcc storage
pub struct KvEntity;

impl MvccEntity for KvEntity {
    type Key = KvKey;
    type Value = Value;
    type Delta = KvDelta;

    fn entity_name() -> &'static str {
        "kv"
    }
}

/// Delta operations for KV storage
#[derive(Clone)]
pub enum KvDelta {
    /// Put a new value (or update existing)
    Put {
        key: String,
        new_value: Value,
        old_value: Option<Value>,
    },
    /// Delete a value
    Delete { key: String, old_value: Value },
}

// Newtype wrappers to avoid orphan rules

/// Wrapper for KV keys (String)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KvKey(String);

impl From<String> for KvKey {
    fn from(s: String) -> Self {
        KvKey(s)
    }
}

impl From<&str> for KvKey {
    fn from(s: &str) -> Self {
        KvKey(s.to_string())
    }
}

impl From<KvKey> for String {
    fn from(k: KvKey) -> Self {
        k.0
    }
}

impl AsRef<str> for KvKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Encode for KvKey {
    fn encode(&self) -> Result<Vec<u8>> {
        Ok(self.0.as_bytes().to_vec())
    }
}

impl Decode for KvKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map(KvKey)
            .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))
    }
}

// Note: Encode/Decode for Value is implemented in proven-value crate with mvcc feature

// Implement Encode/Decode for KvDelta
impl Encode for KvDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        match self {
            KvDelta::Put {
                key,
                new_value,
                old_value,
            } => {
                let mut buf = vec![0u8]; // Put tag

                // Encode key
                let key_bytes = key.as_bytes();
                buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(key_bytes);

                // Encode new value
                let new_value_bytes = new_value.encode()?;
                buf.extend_from_slice(&(new_value_bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(&new_value_bytes);

                // Encode optional old value
                if let Some(old) = old_value {
                    buf.push(1);
                    let old_value_bytes = old.encode()?;
                    buf.extend_from_slice(&(old_value_bytes.len() as u32).to_be_bytes());
                    buf.extend_from_slice(&old_value_bytes);
                } else {
                    buf.push(0);
                }

                Ok(buf)
            }
            KvDelta::Delete { key, old_value } => {
                let mut buf = vec![1u8]; // Delete tag

                // Encode key
                let key_bytes = key.as_bytes();
                buf.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(key_bytes);

                // Encode old value
                let old_value_bytes = old_value.encode()?;
                buf.extend_from_slice(&(old_value_bytes.len() as u32).to_be_bytes());
                buf.extend_from_slice(&old_value_bytes);

                Ok(buf)
            }
        }
    }
}

impl Decode for KvDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        let tag = bytes[0];
        let mut pos = 1;

        // Decode key
        let key_len =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;
        let key = String::from_utf8(bytes[pos..pos + key_len].to_vec())
            .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
        pos += key_len;

        match tag {
            0 => {
                // Put
                let new_value_len = u32::from_be_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]) as usize;
                pos += 4;
                let new_value = Value::decode(&bytes[pos..pos + new_value_len])?;
                pos += new_value_len;

                let has_old = bytes[pos];
                pos += 1;

                let old_value = if has_old == 1 {
                    let old_value_len = u32::from_be_bytes([
                        bytes[pos],
                        bytes[pos + 1],
                        bytes[pos + 2],
                        bytes[pos + 3],
                    ]) as usize;
                    pos += 4;
                    Some(Value::decode(&bytes[pos..pos + old_value_len])?)
                } else {
                    None
                };

                Ok(KvDelta::Put {
                    key,
                    new_value,
                    old_value,
                })
            }
            1 => {
                // Delete
                let old_value_len = u32::from_be_bytes([
                    bytes[pos],
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                ]) as usize;
                pos += 4;
                let old_value = Value::decode(&bytes[pos..pos + old_value_len])?;

                Ok(KvDelta::Delete { key, old_value })
            }
            _ => Err(proven_mvcc::Error::Encoding("Invalid tag".to_string())),
        }
    }
}

impl MvccDelta<KvEntity> for KvDelta {
    fn key(&self) -> KvKey {
        match self {
            KvDelta::Put { key, .. } => KvKey(key.clone()),
            KvDelta::Delete { key, .. } => KvKey(key.clone()),
        }
    }

    fn apply(&self, _current: Option<Value>) -> Option<Value> {
        match self {
            KvDelta::Put { new_value, .. } => Some(new_value.clone()),
            KvDelta::Delete { .. } => None,
        }
    }

    fn unapply(&self, _current: Option<Value>) -> Option<Value> {
        match self {
            KvDelta::Put { old_value, .. } => old_value.clone(),
            KvDelta::Delete { old_value, .. } => Some(old_value.clone()),
        }
    }

    fn merge(self, next: Self) -> Self {
        // For KV storage, last write wins (coarse-grained deltas)
        // Put followed by Put = last Put wins (keep original old_value)
        // Put followed by Delete = Delete wins (keep original old_value)
        // Delete followed by Put = Put wins (old_value is None since it was deleted)
        // Delete followed by Delete = last Delete wins (shouldn't happen normally)
        match (self, next) {
            // Put followed by Put -> last Put wins, preserve original old_value
            (KvDelta::Put { key, old_value, .. }, KvDelta::Put { new_value, .. }) => KvDelta::Put {
                key,
                new_value,
                old_value,
            },
            // Put followed by Delete -> Delete wins, preserve original old_value
            (KvDelta::Put { key, old_value, .. }, KvDelta::Delete { .. }) => {
                KvDelta::Delete {
                    key,
                    old_value: old_value.unwrap_or_else(|| {
                        // This shouldn't happen in normal operation, but handle it gracefully
                        panic!("Put followed by Delete with no old_value")
                    }),
                }
            }
            // Delete followed by Put -> Put wins, old_value is None (was deleted)
            (KvDelta::Delete { key, .. }, KvDelta::Put { new_value, .. }) => KvDelta::Put {
                key,
                new_value,
                old_value: None,
            },
            // Any other case (Delete followed by Delete) - next wins
            (_, next) => next,
        }
    }
}
