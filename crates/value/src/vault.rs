//! Vault type for resource accounts
//!
//! Vaults represent accounts that hold resources. They are identified by UUIDs
//! and redacted in output for security.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Vault type for resource accounts
///
/// Vaults represent accounts that can hold resources (tokens, assets, etc).
/// They are identified by UUIDs and fully redacted in all output (Debug and Display)
/// for security.
///
/// While the UUID is hidden in output, Vault can still be:
/// - Compared for equality
/// - Used in WHERE clauses and JOINs
/// - Hashed for indexing
/// - Used as account identifiers in resource operations
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Vault {
    uuid: Uuid,
}

impl Vault {
    /// Create a new vault from a UUID
    pub fn new(uuid: Uuid) -> Self {
        Self { uuid }
    }

    /// Get the underlying UUID
    ///
    /// Use this when you need the actual UUID value (e.g., for database operations).
    /// Be careful not to leak this in logs or user-facing output.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Create from raw bytes (16 bytes)
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self {
            uuid: Uuid::from_bytes(bytes),
        }
    }

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.uuid.as_bytes()
    }
}

// Redacted Debug implementation - never shows the actual UUID
impl fmt::Debug for Vault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Vault([REDACTED])")
    }
}

// Redacted Display implementation - never shows the actual UUID
impl fmt::Display for Vault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED VAULT]")
    }
}

// Ordering for vaults (useful for database operations)
impl PartialOrd for Vault {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vault {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uuid.cmp(&other.uuid)
    }
}

// Conversion from Uuid
impl From<Uuid> for Vault {
    fn from(uuid: Uuid) -> Self {
        Self::new(uuid)
    }
}

// Conversion to Uuid
impl From<Vault> for Uuid {
    fn from(vault: Vault) -> Self {
        vault.uuid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vault_creation() {
        let uuid = Uuid::new_v4();
        let vault = Vault::new(uuid);
        assert_eq!(vault.uuid(), uuid);
    }

    #[test]
    fn test_from_uuid() {
        let uuid = Uuid::new_v4();
        let vault: Vault = uuid.into();
        assert_eq!(vault.uuid(), uuid);
    }

    #[test]
    fn test_to_uuid() {
        let uuid = Uuid::new_v4();
        let vault = Vault::new(uuid);
        let extracted: Uuid = vault.into();
        assert_eq!(extracted, uuid);
    }

    #[test]
    fn test_debug_redaction() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let vault = Vault::new(uuid);
        let debug_str = format!("{:?}", vault);

        assert_eq!(debug_str, "Vault([REDACTED])");
        // Ensure no part of the UUID leaked
        assert!(!debug_str.contains("550e"));
        assert!(!debug_str.contains("8400"));
        assert!(!debug_str.contains("e29b"));
    }

    #[test]
    fn test_display_redaction() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let vault = Vault::new(uuid);
        let display_str = format!("{}", vault);

        assert_eq!(display_str, "[REDACTED VAULT]");
        // Ensure no part of the UUID leaked
        assert!(!display_str.contains("550e"));
        assert!(!display_str.contains("8400"));
        assert!(!display_str.contains("e29b"));
    }

    #[test]
    fn test_equality() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        let vault1a = Vault::new(uuid1);
        let vault1b = Vault::new(uuid1);
        let vault2 = Vault::new(uuid2);

        assert_eq!(vault1a, vault1b);
        assert_ne!(vault1a, vault2);
    }

    #[test]
    fn test_ordering() {
        let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let vault1 = Vault::new(uuid1);
        let vault2 = Vault::new(uuid2);

        assert!(vault1 < vault2);
        assert!(vault2 > vault1);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let uuid = Uuid::new_v4();
        let vault = Vault::new(uuid);

        let bytes = vault.as_bytes();
        let vault2 = Vault::from_bytes(*bytes);

        assert_eq!(vault, vault2);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let vault1 = Vault::new(uuid);
        let vault2 = Vault::new(uuid);

        let mut set = HashSet::new();
        set.insert(vault1);

        // Should find vault2 because it has the same UUID
        assert!(set.contains(&vault2));
    }
}
