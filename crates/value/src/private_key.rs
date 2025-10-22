//! Private key type for cryptographic operations in TEE environments
//!
//! This module provides a type-safe way to store private keys, preventing
//! accidental misuse like casting to strings or displaying in logs.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Private key types for cryptographic operations
///
/// Designed for use in Trusted Execution Environments (TEEs) like Nitro Enclaves.
/// Uses fixed-length byte arrays to avoid unnecessary allocations and provide
/// type safety without coupling to specific cryptographic libraries.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivateKey {
    /// Ed25519 private key (32 bytes)
    /// Used in modern cryptographic systems, Solana, etc.
    Ed25519([u8; 32]),

    /// Secp256k1 private key (32 bytes)
    /// Used in Bitcoin, Ethereum, and other cryptocurrencies
    Secp256k1([u8; 32]),
}

impl PrivateKey {
    /// Create a new Ed25519 private key from bytes
    pub fn ed25519(bytes: [u8; 32]) -> Self {
        PrivateKey::Ed25519(bytes)
    }

    /// Create a new Secp256k1 private key from bytes
    pub fn secp256k1(bytes: [u8; 32]) -> Self {
        PrivateKey::Secp256k1(bytes)
    }

    /// Get the raw bytes of the private key
    pub fn as_bytes(&self) -> &[u8; 32] {
        match self {
            PrivateKey::Ed25519(bytes) => bytes,
            PrivateKey::Secp256k1(bytes) => bytes,
        }
    }

    /// Get the key type as a string
    pub fn key_type(&self) -> &'static str {
        match self {
            PrivateKey::Ed25519(_) => "ed25519",
            PrivateKey::Secp256k1(_) => "secp256k1",
        }
    }
}

// Redacted Debug implementation to prevent accidental key exposure
impl fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrivateKey::Ed25519(_) => write!(f, "PrivateKey::Ed25519([REDACTED])"),
            PrivateKey::Secp256k1(_) => write!(f, "PrivateKey::Secp256k1([REDACTED])"),
        }
    }
}

// Redacted Display implementation to prevent accidental key exposure
impl fmt::Display for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED {} private key]", self.key_type())
    }
}

// Hash implementation using constant-time comparison where possible
impl std::hash::Hash for PrivateKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the discriminant first
        std::mem::discriminant(self).hash(state);
        // Hash the key bytes
        self.as_bytes().hash(state);
    }
}

// Ordering for private keys (needed for Value::Ord)
// Note: Ordering private keys doesn't have cryptographic meaning,
// but is required for database operations like sorting
impl PartialOrd for PrivateKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PrivateKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        // First compare by type
        match (self, other) {
            (PrivateKey::Ed25519(_), PrivateKey::Secp256k1(_)) => Ordering::Less,
            (PrivateKey::Secp256k1(_), PrivateKey::Ed25519(_)) => Ordering::Greater,
            // Same type, compare bytes
            (PrivateKey::Ed25519(a), PrivateKey::Ed25519(b)) => a.cmp(b),
            (PrivateKey::Secp256k1(a), PrivateKey::Secp256k1(b)) => a.cmp(b),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ed25519_creation() {
        let bytes = [42u8; 32];
        let key = PrivateKey::ed25519(bytes);
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(key.key_type(), "ed25519");
    }

    #[test]
    fn test_secp256k1_creation() {
        let bytes = [99u8; 32];
        let key = PrivateKey::secp256k1(bytes);
        assert_eq!(key.as_bytes(), &bytes);
        assert_eq!(key.key_type(), "secp256k1");
    }

    #[test]
    fn test_debug_redaction() {
        let key = PrivateKey::ed25519([42u8; 32]);
        let debug_str = format!("{:?}", key);
        assert_eq!(debug_str, "PrivateKey::Ed25519([REDACTED])");
        assert!(!debug_str.contains("42")); // Ensure no key bytes leaked
    }

    #[test]
    fn test_display_redaction() {
        let key = PrivateKey::secp256k1([99u8; 32]);
        let display_str = format!("{}", key);
        assert_eq!(display_str, "[REDACTED secp256k1 private key]");
        assert!(!display_str.contains("99")); // Ensure no key bytes leaked
    }

    #[test]
    fn test_ordering() {
        let ed_key1 = PrivateKey::ed25519([1u8; 32]);
        let ed_key2 = PrivateKey::ed25519([2u8; 32]);
        let secp_key = PrivateKey::secp256k1([0u8; 32]);

        // Ed25519 sorts before Secp256k1
        assert!(ed_key1 < secp_key);

        // Within same type, compare by bytes
        assert!(ed_key1 < ed_key2);
    }
}
