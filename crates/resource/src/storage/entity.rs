//! Resource entity implementation for MVCC storage
//!
//! Defines the entity types, keys, values, and deltas for resource operations
//! in the proven-mvcc storage layer.

use crate::storage::ResourceMetadata;
use crate::types::Amount;
use proven_mvcc::{Decode, Encode, MvccDelta, MvccEntity, Result};
use serde::{Deserialize, Serialize};

/// Resource entity for MVCC storage
pub struct ResourceEntity;

impl MvccEntity for ResourceEntity {
    type Key = ResourceKey;
    type Value = ResourceValue;
    type Delta = ResourceDelta;

    fn entity_name() -> &'static str {
        "resource"
    }
}

/// Key type for resource storage
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceKey {
    /// Global metadata key
    Metadata,
    /// Total supply key
    Supply,
    /// Account balance key
    Account(String),
}

impl Encode for ResourceKey {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to encode ResourceKey: {}", e))
        })
    }
}

impl Decode for ResourceKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to decode ResourceKey: {}", e))
        })
    }
}

/// Value type for resource storage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResourceValue {
    /// Metadata value
    Metadata(ResourceMetadata),
    /// Supply amount
    Supply(Amount),
    /// Account balance
    Balance(Amount),
}

impl Encode for ResourceValue {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to encode ResourceValue: {}", e))
        })
    }
}

impl Decode for ResourceValue {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to decode ResourceValue: {}", e))
        })
    }
}

/// Delta type for resource operations
///
/// Each delta affects exactly ONE key in MVCC storage.
/// Multi-key operations (like Mint/Burn) are handled by creating multiple transactions
/// or multiple deltas in the engine layer.
#[derive(Clone, Serialize, Deserialize)]
pub enum ResourceDelta {
    /// Update metadata
    SetMetadata {
        old: Option<ResourceMetadata>,
        new: ResourceMetadata,
    },

    /// Update supply
    SetSupply { old: Amount, new: Amount },

    /// Update account balance
    SetBalance {
        account: String,
        old: Amount,
        new: Amount,
    },
}

impl Encode for ResourceDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to encode ResourceDelta: {}", e))
        })
    }
}

impl Decode for ResourceDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes).map_err(|e| {
            proven_mvcc::Error::Encoding(format!("Failed to decode ResourceDelta: {}", e))
        })
    }
}

impl MvccDelta<ResourceEntity> for ResourceDelta {
    fn key(&self) -> ResourceKey {
        match self {
            ResourceDelta::SetMetadata { .. } => ResourceKey::Metadata,
            ResourceDelta::SetSupply { .. } => ResourceKey::Supply,
            ResourceDelta::SetBalance { account, .. } => ResourceKey::Account(account.clone()),
        }
    }

    fn apply(&self, _current: Option<ResourceValue>) -> Option<ResourceValue> {
        match self {
            ResourceDelta::SetMetadata { new, .. } => Some(ResourceValue::Metadata(new.clone())),
            ResourceDelta::SetSupply { new, .. } => Some(ResourceValue::Supply(*new)),
            ResourceDelta::SetBalance { new, .. } => Some(ResourceValue::Balance(*new)),
        }
    }

    fn unapply(&self, _current: Option<ResourceValue>) -> Option<ResourceValue> {
        match self {
            ResourceDelta::SetMetadata { old, .. } => old.clone().map(ResourceValue::Metadata),
            ResourceDelta::SetSupply { old, .. } => Some(ResourceValue::Supply(*old)),
            ResourceDelta::SetBalance { old, .. } => Some(ResourceValue::Balance(*old)),
        }
    }

    fn merge(self, next: Self) -> Self {
        // For resource storage, last write wins
        // Preserve the original old value when merging
        match (self, next) {
            (ResourceDelta::SetMetadata { old, .. }, ResourceDelta::SetMetadata { new, .. }) => {
                ResourceDelta::SetMetadata { old, new }
            }
            (ResourceDelta::SetSupply { old, .. }, ResourceDelta::SetSupply { new, .. }) => {
                ResourceDelta::SetSupply { old, new }
            }
            (
                ResourceDelta::SetBalance { account, old, .. },
                ResourceDelta::SetBalance { new, .. },
            ) => ResourceDelta::SetBalance { account, old, new },
            // Mixing different delta types for the same key shouldn't happen
            (_, next) => next,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_metadata(name: &str) -> ResourceMetadata {
        ResourceMetadata {
            name: name.to_string(),
            symbol: "TEST".to_string(),
            decimals: 8,
            initialized: true,
        }
    }

    #[test]
    fn test_encode_decode_key() {
        let keys = vec![
            ResourceKey::Metadata,
            ResourceKey::Supply,
            ResourceKey::Account("alice".to_string()),
        ];

        for key in keys {
            let encoded = key.encode().unwrap();
            let decoded = ResourceKey::decode(&encoded).unwrap();
            assert_eq!(key, decoded);
        }
    }

    #[test]
    fn test_encode_decode_value() {
        let values = vec![
            ResourceValue::Metadata(make_metadata("Test")),
            ResourceValue::Supply(Amount::from_integer(1000, 0)),
            ResourceValue::Balance(Amount::from_integer(500, 0)),
        ];

        for value in values {
            let encoded = value.encode().unwrap();
            let decoded = ResourceValue::decode(&encoded).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_delta_apply() {
        let delta = ResourceDelta::SetBalance {
            account: "alice".to_string(),
            old: Amount::from_integer(50, 0),
            new: Amount::from_integer(150, 0),
        };

        let old_value = Some(ResourceValue::Balance(Amount::from_integer(50, 0)));
        let new_value = delta.apply(old_value);
        assert_eq!(
            new_value,
            Some(ResourceValue::Balance(Amount::from_integer(150, 0)))
        );
    }

    #[test]
    fn test_delta_unapply() {
        let delta = ResourceDelta::SetBalance {
            account: "alice".to_string(),
            old: Amount::from_integer(50, 0),
            new: Amount::from_integer(150, 0),
        };

        let current_value = Some(ResourceValue::Balance(Amount::from_integer(150, 0)));
        let old_value = delta.unapply(current_value);
        assert_eq!(
            old_value,
            Some(ResourceValue::Balance(Amount::from_integer(50, 0)))
        );
    }

    #[test]
    fn test_delta_merge() {
        let delta1 = ResourceDelta::SetBalance {
            account: "alice".to_string(),
            old: Amount::from_integer(100, 0),
            new: Amount::from_integer(150, 0),
        };

        let delta2 = ResourceDelta::SetBalance {
            account: "alice".to_string(),
            old: Amount::from_integer(150, 0),
            new: Amount::from_integer(200, 0),
        };

        let merged = delta1.merge(delta2);
        match merged {
            ResourceDelta::SetBalance { account, old, new } => {
                assert_eq!(account, "alice");
                assert_eq!(old, Amount::from_integer(100, 0)); // Original old value
                assert_eq!(new, Amount::from_integer(200, 0)); // Final new value
            }
            _ => panic!("Expected SetBalance delta"),
        }
    }

    #[test]
    fn test_delta_key() {
        let delta = ResourceDelta::SetBalance {
            account: "alice".to_string(),
            old: Amount::from_integer(100, 0),
            new: Amount::from_integer(150, 0),
        };

        assert_eq!(delta.key(), ResourceKey::Account("alice".to_string()));
    }
}
