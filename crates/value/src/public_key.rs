//! Public key type for cryptographic operations in TEE environments
//!
//! This module provides a type-safe way to store public keys, separate from
//! private keys to prevent accidental misuse.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Public key types for cryptographic operations
///
/// Unlike PrivateKey, public keys are safe to display and can be freely shared.
/// Uses fixed-length byte arrays to avoid unnecessary allocations and provide
/// type safety without coupling to specific cryptographic libraries.
#[derive(Clone, PartialEq, Eq)]
pub enum PublicKey {
    /// Ed25519 public key (32 bytes)
    /// Used in modern cryptographic systems, Solana, etc.
    Ed25519([u8; 32]),

    /// Secp256k1 public key (33 bytes compressed, 65 bytes uncompressed)
    /// Used in Bitcoin, Ethereum, and other cryptocurrencies
    /// We store the compressed form (33 bytes) for efficiency
    Secp256k1Compressed([u8; 33]),

    /// Secp256k1 public key (uncompressed, 65 bytes)
    Secp256k1Uncompressed([u8; 65]),
}

impl PublicKey {
    /// Create a new Ed25519 public key from bytes
    pub fn ed25519(bytes: [u8; 32]) -> Self {
        PublicKey::Ed25519(bytes)
    }

    /// Create a new compressed Secp256k1 public key from bytes
    pub fn secp256k1_compressed(bytes: [u8; 33]) -> Self {
        PublicKey::Secp256k1Compressed(bytes)
    }

    /// Create a new uncompressed Secp256k1 public key from bytes
    pub fn secp256k1_uncompressed(bytes: [u8; 65]) -> Self {
        PublicKey::Secp256k1Uncompressed(bytes)
    }

    /// Get the raw bytes of the public key
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            PublicKey::Ed25519(bytes) => bytes,
            PublicKey::Secp256k1Compressed(bytes) => bytes,
            PublicKey::Secp256k1Uncompressed(bytes) => bytes,
        }
    }

    /// Get the key type as a string
    pub fn key_type(&self) -> &'static str {
        match self {
            PublicKey::Ed25519(_) => "ed25519",
            PublicKey::Secp256k1Compressed(_) => "secp256k1_compressed",
            PublicKey::Secp256k1Uncompressed(_) => "secp256k1_uncompressed",
        }
    }

    /// Convert to hex string (commonly used format)
    pub fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
    }

    /// Create from hex string
    pub fn from_hex(hex_str: &str, key_type: &str) -> Result<Self, String> {
        let bytes = hex::decode(hex_str).map_err(|e| format!("Invalid hex: {}", e))?;

        match key_type {
            "ed25519" => {
                if bytes.len() != 32 {
                    return Err(format!("Ed25519 key must be 32 bytes, got {}", bytes.len()));
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                Ok(PublicKey::Ed25519(arr))
            }
            "secp256k1_compressed" => {
                if bytes.len() != 33 {
                    return Err(format!(
                        "Compressed Secp256k1 key must be 33 bytes, got {}",
                        bytes.len()
                    ));
                }
                let mut arr = [0u8; 33];
                arr.copy_from_slice(&bytes);
                Ok(PublicKey::Secp256k1Compressed(arr))
            }
            "secp256k1_uncompressed" => {
                if bytes.len() != 65 {
                    return Err(format!(
                        "Uncompressed Secp256k1 key must be 65 bytes, got {}",
                        bytes.len()
                    ));
                }
                let mut arr = [0u8; 65];
                arr.copy_from_slice(&bytes);
                Ok(PublicKey::Secp256k1Uncompressed(arr))
            }
            _ => Err(format!("Unknown key type: {}", key_type)),
        }
    }
}

// Normal Debug implementation - public keys are safe to display
impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PublicKey::Ed25519(bytes) => write!(f, "PublicKey::Ed25519({})", hex::encode(bytes)),
            PublicKey::Secp256k1Compressed(bytes) => {
                write!(f, "PublicKey::Secp256k1Compressed({})", hex::encode(bytes))
            }
            PublicKey::Secp256k1Uncompressed(bytes) => write!(
                f,
                "PublicKey::Secp256k1Uncompressed({})",
                hex::encode(bytes)
            ),
        }
    }
}

// Normal Display implementation - shows hex format
impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

// Hash implementation
impl std::hash::Hash for PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first
        std::mem::discriminant(self).hash(state);
        // Hash the key bytes
        self.as_bytes().hash(state);
    }
}

// Ordering for public keys (useful for indexing and sorting)
impl PartialOrd for PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // Compare by variant tag first, then by bytes
        match (self, other) {
            (PublicKey::Ed25519(a), PublicKey::Ed25519(b)) => a.cmp(b),
            (PublicKey::Secp256k1Compressed(a), PublicKey::Secp256k1Compressed(b)) => a.cmp(b),
            (PublicKey::Secp256k1Uncompressed(a), PublicKey::Secp256k1Uncompressed(b)) => a.cmp(b),
            // Different variants - compare by variant order
            (PublicKey::Ed25519(_), _) => Ordering::Less,
            (_, PublicKey::Ed25519(_)) => Ordering::Greater,
            (PublicKey::Secp256k1Compressed(_), PublicKey::Secp256k1Uncompressed(_)) => {
                Ordering::Less
            }
            (PublicKey::Secp256k1Uncompressed(_), PublicKey::Secp256k1Compressed(_)) => {
                Ordering::Greater
            }
        }
    }
}

// Manual Serialize implementation
impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PublicKey::Ed25519(bytes) => {
                serializer.serialize_newtype_variant("PublicKey", 0, "Ed25519", &bytes[..])
            }
            PublicKey::Secp256k1Compressed(bytes) => serializer.serialize_newtype_variant(
                "PublicKey",
                1,
                "Secp256k1Compressed",
                &bytes[..],
            ),
            PublicKey::Secp256k1Uncompressed(bytes) => serializer.serialize_newtype_variant(
                "PublicKey",
                2,
                "Secp256k1Uncompressed",
                &bytes[..],
            ),
        }
    }
}

// Manual Deserialize implementation
impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{Error, VariantAccess, Visitor};

        struct PublicKeyVisitor;

        impl<'de> Visitor<'de> for PublicKeyVisitor {
            type Value = PublicKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a PublicKey enum variant")
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::EnumAccess<'de>,
            {
                let (variant, variant_access) = data.variant::<String>()?;

                match variant.as_str() {
                    "Ed25519" => {
                        let bytes: Vec<u8> = variant_access.newtype_variant()?;
                        if bytes.len() != 32 {
                            return Err(Error::custom(format!(
                                "Ed25519 key must be 32 bytes, got {}",
                                bytes.len()
                            )));
                        }
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes);
                        Ok(PublicKey::Ed25519(arr))
                    }
                    "Secp256k1Compressed" => {
                        let bytes: Vec<u8> = variant_access.newtype_variant()?;
                        if bytes.len() != 33 {
                            return Err(Error::custom(format!(
                                "Secp256k1Compressed key must be 33 bytes, got {}",
                                bytes.len()
                            )));
                        }
                        let mut arr = [0u8; 33];
                        arr.copy_from_slice(&bytes);
                        Ok(PublicKey::Secp256k1Compressed(arr))
                    }
                    "Secp256k1Uncompressed" => {
                        let bytes: Vec<u8> = variant_access.newtype_variant()?;
                        if bytes.len() != 65 {
                            return Err(Error::custom(format!(
                                "Secp256k1Uncompressed key must be 65 bytes, got {}",
                                bytes.len()
                            )));
                        }
                        let mut arr = [0u8; 65];
                        arr.copy_from_slice(&bytes);
                        Ok(PublicKey::Secp256k1Uncompressed(arr))
                    }
                    _ => Err(Error::unknown_variant(
                        &variant,
                        &["Ed25519", "Secp256k1Compressed", "Secp256k1Uncompressed"],
                    )),
                }
            }
        }

        deserializer.deserialize_enum(
            "PublicKey",
            &["Ed25519", "Secp256k1Compressed", "Secp256k1Uncompressed"],
            PublicKeyVisitor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_creation() {
        let bytes = [42u8; 32];
        let key = PublicKey::ed25519(bytes);
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(key.key_type(), "ed25519");
    }

    #[test]
    fn test_secp256k1_compressed_creation() {
        let bytes = [99u8; 33];
        let key = PublicKey::secp256k1_compressed(bytes);
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(key.key_type(), "secp256k1_compressed");
    }

    #[test]
    fn test_secp256k1_uncompressed_creation() {
        let bytes = [55u8; 65];
        let key = PublicKey::secp256k1_uncompressed(bytes);
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(key.key_type(), "secp256k1_uncompressed");
    }

    #[test]
    fn test_debug_shows_hex() {
        let key = PublicKey::ed25519([1u8; 32]);
        let debug_str = format!("{:?}", key);
        // Should contain hex representation
        assert!(debug_str.contains("0101010101"));
        assert!(debug_str.contains("PublicKey::Ed25519"));
    }

    #[test]
    fn test_display_shows_hex() {
        let key = PublicKey::ed25519([255u8; 32]);
        let display_str = format!("{}", key);
        // Should be just hex
        assert_eq!(display_str, "ff".repeat(32));
    }

    #[test]
    fn test_hex_conversion() {
        let bytes = [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0];
        let mut full_bytes = [0u8; 32];
        full_bytes[..8].copy_from_slice(&bytes);

        let key = PublicKey::ed25519(full_bytes);
        let hex = key.to_hex();

        assert_eq!(hex.len(), 64); // 32 bytes = 64 hex chars
        assert!(hex.starts_with("123456789abcdef0"));

        // Round trip
        let key2 = PublicKey::from_hex(&hex, "ed25519").unwrap();
        assert_eq!(key, key2);
    }

    #[test]
    fn test_ordering() {
        let ed_key1 = PublicKey::ed25519([1u8; 32]);
        let ed_key2 = PublicKey::ed25519([2u8; 32]);
        let secp_key = PublicKey::secp256k1_compressed([0u8; 33]);

        // Ed25519 sorts before Secp256k1 (based on discriminant)
        assert!(ed_key1 < secp_key);

        // Within same type, compare by bytes
        assert!(ed_key1 < ed_key2);
    }

    #[test]
    fn test_from_hex_invalid() {
        // Wrong length
        let result = PublicKey::from_hex("aabb", "ed25519");
        assert!(result.is_err());

        // Invalid hex
        let result = PublicKey::from_hex("xyz", "ed25519");
        assert!(result.is_err());

        // Unknown key type
        let hex = "aa".repeat(32);
        let result = PublicKey::from_hex(&hex, "unknown");
        assert!(result.is_err());
    }
}
