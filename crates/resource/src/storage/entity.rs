//! Resource entity implementation for MVCC storage
//!
//! Defines the entity types, keys, values, and deltas for resource operations
//! in the proven-mvcc storage layer.

use crate::storage::ResourceMetadata;
use crate::types::Amount;
use proven_mvcc::{Decode, Encode, MvccDelta, MvccEntity, Result};
use proven_value::Vault;
use rust_decimal::Decimal;
use std::io::{Cursor, Read, Write};
use std::str::FromStr;

// Helper functions for encoding/decoding Amount and ResourceMetadata

fn encode_amount(amount: Amount, buf: &mut Vec<u8>) -> Result<()> {
    // Encode Decimal as string (preserves precision)
    let decimal_str = amount.0.to_string();
    let bytes = decimal_str.as_bytes();
    buf.write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    buf.write_all(bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    Ok(())
}

fn decode_amount(cursor: &mut Cursor<&[u8]>) -> Result<Amount> {
    let mut len_bytes = [0u8; 4];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut decimal_bytes = vec![0u8; len];
    cursor
        .read_exact(&mut decimal_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

    let decimal_str = String::from_utf8(decimal_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(format!("Invalid UTF-8: {}", e)))?;

    let decimal = Decimal::from_str(&decimal_str)
        .map_err(|e| proven_mvcc::Error::Encoding(format!("Invalid decimal: {}", e)))?;

    Ok(Amount::new(decimal))
}

fn encode_metadata(metadata: &ResourceMetadata, buf: &mut Vec<u8>) -> Result<()> {
    // Encode name (String)
    let name_bytes = metadata.name.as_bytes();
    buf.write_all(&(name_bytes.len() as u32).to_be_bytes())
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    buf.write_all(name_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

    // Encode symbol (String)
    let symbol_bytes = metadata.symbol.as_bytes();
    buf.write_all(&(symbol_bytes.len() as u32).to_be_bytes())
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    buf.write_all(symbol_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

    // Encode decimals (u32)
    buf.write_all(&metadata.decimals.to_be_bytes())
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

    // Encode initialized (bool)
    buf.push(if metadata.initialized { 1 } else { 0 });

    Ok(())
}

fn decode_metadata(cursor: &mut Cursor<&[u8]>) -> Result<ResourceMetadata> {
    // Decode name
    let mut len_bytes = [0u8; 4];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let name_len = u32::from_be_bytes(len_bytes) as usize;

    let mut name_bytes = vec![0u8; name_len];
    cursor
        .read_exact(&mut name_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let name = String::from_utf8(name_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(format!("Invalid UTF-8: {}", e)))?;

    // Decode symbol
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let symbol_len = u32::from_be_bytes(len_bytes) as usize;

    let mut symbol_bytes = vec![0u8; symbol_len];
    cursor
        .read_exact(&mut symbol_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let symbol = String::from_utf8(symbol_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(format!("Invalid UTF-8: {}", e)))?;

    // Decode decimals
    let mut decimals_bytes = [0u8; 4];
    cursor
        .read_exact(&mut decimals_bytes)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let decimals = u32::from_be_bytes(decimals_bytes);

    // Decode initialized
    let mut initialized_byte = [0u8; 1];
    cursor
        .read_exact(&mut initialized_byte)
        .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
    let initialized = initialized_byte[0] == 1;

    Ok(ResourceMetadata {
        name,
        symbol,
        decimals,
        initialized,
    })
}

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResourceKey {
    /// Global metadata key
    Metadata,
    /// Total supply key
    Supply,
    /// Account balance key
    Account(Vault),
}

impl Encode for ResourceKey {
    fn encode(&self) -> Result<Vec<u8>> {
        use std::io::Write;
        let mut buf = Vec::new();

        match self {
            ResourceKey::Metadata => {
                buf.push(1); // Tag for Metadata
            }
            ResourceKey::Supply => {
                buf.push(2); // Tag for Supply
            }
            ResourceKey::Account(vault) => {
                buf.push(3); // Tag for Account
                // Encode Vault UUID (16 bytes)
                buf.write_all(vault.as_bytes())
                    .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
            }
        }
        Ok(buf)
    }
}

impl Decode for ResourceKey {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use std::io::{Cursor, Read};

        if bytes.is_empty() {
            return Err(proven_mvcc::Error::Encoding(
                "ResourceKey is empty".to_string(),
            ));
        }

        let mut cursor = Cursor::new(bytes);
        let mut tag = [0u8; 1];
        cursor
            .read_exact(&mut tag)
            .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

        match tag[0] {
            1 => Ok(ResourceKey::Metadata),
            2 => Ok(ResourceKey::Supply),
            3 => {
                // Decode Vault UUID (16 bytes)
                let mut uuid_bytes = [0u8; 16];
                cursor
                    .read_exact(&mut uuid_bytes)
                    .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

                let vault = Vault::from_bytes(uuid_bytes);
                Ok(ResourceKey::Account(vault))
            }
            _ => Err(proven_mvcc::Error::Encoding(format!(
                "Unknown ResourceKey tag: {}",
                tag[0]
            ))),
        }
    }
}

/// Value type for resource storage
#[derive(Debug, Clone, PartialEq)]
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
        let mut buf = Vec::new();

        match self {
            ResourceValue::Metadata(metadata) => {
                buf.push(1); // Tag for Metadata
                encode_metadata(metadata, &mut buf)?;
            }
            ResourceValue::Supply(amount) => {
                buf.push(2); // Tag for Supply
                encode_amount(*amount, &mut buf)?;
            }
            ResourceValue::Balance(amount) => {
                buf.push(3); // Tag for Balance
                encode_amount(*amount, &mut buf)?;
            }
        }
        Ok(buf)
    }
}

impl Decode for ResourceValue {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use std::io::Cursor;

        if bytes.is_empty() {
            return Err(proven_mvcc::Error::Encoding(
                "ResourceValue is empty".to_string(),
            ));
        }

        let mut cursor = Cursor::new(bytes);
        let tag = bytes[0];
        cursor.set_position(1);

        match tag {
            1 => {
                let metadata = decode_metadata(&mut cursor)?;
                Ok(ResourceValue::Metadata(metadata))
            }
            2 => {
                let amount = decode_amount(&mut cursor)?;
                Ok(ResourceValue::Supply(amount))
            }
            3 => {
                let amount = decode_amount(&mut cursor)?;
                Ok(ResourceValue::Balance(amount))
            }
            _ => Err(proven_mvcc::Error::Encoding(format!(
                "Unknown ResourceValue tag: {}",
                tag
            ))),
        }
    }
}

/// Delta type for resource operations
///
/// Each delta affects exactly ONE key in MVCC storage.
/// Multi-key operations (like Mint/Burn) are handled by creating multiple transactions
/// or multiple deltas in the engine layer.
#[derive(Clone)]
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
        account: Vault,
        old: Amount,
        new: Amount,
    },
}

impl Encode for ResourceDelta {
    fn encode(&self) -> Result<Vec<u8>> {
        use std::io::Write;
        let mut buf = Vec::new();

        match self {
            ResourceDelta::SetMetadata { old, new } => {
                buf.push(1); // Tag for SetMetadata
                if let Some(old_meta) = old {
                    buf.push(1); // Has old value
                    encode_metadata(old_meta, &mut buf)?;
                } else {
                    buf.push(0); // No old value
                }
                encode_metadata(new, &mut buf)?;
            }
            ResourceDelta::SetSupply { old, new } => {
                buf.push(2); // Tag for SetSupply
                encode_amount(*old, &mut buf)?;
                encode_amount(*new, &mut buf)?;
            }
            ResourceDelta::SetBalance { account, old, new } => {
                buf.push(3); // Tag for SetBalance
                // Encode Vault UUID (16 bytes)
                buf.write_all(account.as_bytes())
                    .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;
                encode_amount(*old, &mut buf)?;
                encode_amount(*new, &mut buf)?;
            }
        }
        Ok(buf)
    }
}

impl Decode for ResourceDelta {
    fn decode(bytes: &[u8]) -> Result<Self> {
        use std::io::{Cursor, Read};

        if bytes.is_empty() {
            return Err(proven_mvcc::Error::Encoding(
                "ResourceDelta is empty".to_string(),
            ));
        }

        let mut cursor = Cursor::new(bytes);
        let mut tag = [0u8; 1];
        cursor
            .read_exact(&mut tag)
            .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

        match tag[0] {
            1 => {
                // SetMetadata
                let mut has_old = [0u8; 1];
                cursor
                    .read_exact(&mut has_old)
                    .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

                let old = if has_old[0] == 1 {
                    Some(decode_metadata(&mut cursor)?)
                } else {
                    None
                };
                let new = decode_metadata(&mut cursor)?;

                Ok(ResourceDelta::SetMetadata { old, new })
            }
            2 => {
                // SetSupply
                let old = decode_amount(&mut cursor)?;
                let new = decode_amount(&mut cursor)?;
                Ok(ResourceDelta::SetSupply { old, new })
            }
            3 => {
                // SetBalance
                // Decode Vault UUID (16 bytes)
                let mut uuid_bytes = [0u8; 16];
                cursor
                    .read_exact(&mut uuid_bytes)
                    .map_err(|e| proven_mvcc::Error::Encoding(e.to_string()))?;

                let account = Vault::from_bytes(uuid_bytes);
                let old = decode_amount(&mut cursor)?;
                let new = decode_amount(&mut cursor)?;

                Ok(ResourceDelta::SetBalance { account, old, new })
            }
            _ => Err(proven_mvcc::Error::Encoding(format!(
                "Unknown ResourceDelta tag: {}",
                tag[0]
            ))),
        }
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
        use uuid::Uuid;

        let alice_vault =
            Vault::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let keys = vec![
            ResourceKey::Metadata,
            ResourceKey::Supply,
            ResourceKey::Account(alice_vault),
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
        use uuid::Uuid;

        let alice_vault =
            Vault::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let delta = ResourceDelta::SetBalance {
            account: alice_vault,
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
        use uuid::Uuid;

        let alice_vault =
            Vault::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let delta = ResourceDelta::SetBalance {
            account: alice_vault,
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
        use uuid::Uuid;

        let alice_vault =
            Vault::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let delta1 = ResourceDelta::SetBalance {
            account: alice_vault.clone(),
            old: Amount::from_integer(100, 0),
            new: Amount::from_integer(150, 0),
        };

        let delta2 = ResourceDelta::SetBalance {
            account: alice_vault.clone(),
            old: Amount::from_integer(150, 0),
            new: Amount::from_integer(200, 0),
        };

        let merged = delta1.merge(delta2);
        match merged {
            ResourceDelta::SetBalance { account, old, new } => {
                assert_eq!(account, alice_vault);
                assert_eq!(old, Amount::from_integer(100, 0)); // Original old value
                assert_eq!(new, Amount::from_integer(200, 0)); // Final new value
            }
            _ => panic!("Expected SetBalance delta"),
        }
    }

    #[test]
    fn test_delta_key() {
        use uuid::Uuid;

        let alice_vault =
            Vault::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap());

        let delta = ResourceDelta::SetBalance {
            account: alice_vault.clone(),
            old: Amount::from_integer(100, 0),
            new: Amount::from_integer(150, 0),
        };

        assert_eq!(delta.key(), ResourceKey::Account(alice_vault));
    }
}
