//! Identity type for authenticated users in TEE environments
//!
//! Identities represent authenticated users and can only be created through
//! the authentication system. They are fully redacted in output to prevent
//! identity leakage across TEE boundaries.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Identity type for authenticated users
///
/// Identities can only be created through authentication (via `CURRENT_IDENTITY()`)
/// and represent the current authenticated user. They are fully redacted in all
/// output (Debug and Display) to prevent identity leakage.
///
/// While the UUID is hidden in output, Identity can still be:
/// - Compared for equality
/// - Used in WHERE clauses and JOINs
/// - Hashed for indexing
/// - Used for row-level security policies
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Identity {
    uuid: Uuid,
}

impl Identity {
    /// Create a new identity from a UUID
    ///
    /// This should only be called by the authentication system.
    /// In SQL, use `CURRENT_IDENTITY()` instead.
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
impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Identity([REDACTED])")
    }
}

// Redacted Display implementation - never shows the actual UUID
impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED IDENTITY]")
    }
}

// Ordering for identities (useful for database operations)
impl PartialOrd for Identity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Identity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uuid.cmp(&other.uuid)
    }
}

// Conversion from Uuid
impl From<Uuid> for Identity {
    fn from(uuid: Uuid) -> Self {
        Self::new(uuid)
    }
}

// Conversion to Uuid
impl From<Identity> for Uuid {
    fn from(identity: Identity) -> Self {
        identity.uuid
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_creation() {
        let uuid = Uuid::new_v4();
        let identity = Identity::new(uuid);
        assert_eq!(identity.uuid(), uuid);
    }

    #[test]
    fn test_from_uuid() {
        let uuid = Uuid::new_v4();
        let identity: Identity = uuid.into();
        assert_eq!(identity.uuid(), uuid);
    }

    #[test]
    fn test_to_uuid() {
        let uuid = Uuid::new_v4();
        let identity = Identity::new(uuid);
        let extracted: Uuid = identity.into();
        assert_eq!(extracted, uuid);
    }

    #[test]
    fn test_debug_redaction() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let identity = Identity::new(uuid);
        let debug_str = format!("{:?}", identity);

        assert_eq!(debug_str, "Identity([REDACTED])");
        // Ensure no part of the UUID leaked
        assert!(!debug_str.contains("550e"));
        assert!(!debug_str.contains("8400"));
        assert!(!debug_str.contains("e29b"));
    }

    #[test]
    fn test_display_redaction() {
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let identity = Identity::new(uuid);
        let display_str = format!("{}", identity);

        assert_eq!(display_str, "[REDACTED IDENTITY]");
        // Ensure no part of the UUID leaked
        assert!(!display_str.contains("550e"));
        assert!(!display_str.contains("8400"));
        assert!(!display_str.contains("e29b"));
    }

    #[test]
    fn test_equality() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        let identity1a = Identity::new(uuid1);
        let identity1b = Identity::new(uuid1);
        let identity2 = Identity::new(uuid2);

        assert_eq!(identity1a, identity1b);
        assert_ne!(identity1a, identity2);
    }

    #[test]
    fn test_ordering() {
        let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

        let identity1 = Identity::new(uuid1);
        let identity2 = Identity::new(uuid2);

        assert!(identity1 < identity2);
        assert!(identity2 > identity1);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let uuid = Uuid::new_v4();
        let identity = Identity::new(uuid);

        let bytes = identity.as_bytes();
        let identity2 = Identity::from_bytes(*bytes);

        assert_eq!(identity, identity2);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let uuid = Uuid::new_v4();
        let identity1 = Identity::new(uuid);
        let identity2 = Identity::new(uuid);

        let mut set = HashSet::new();
        set.insert(identity1);

        // Should find identity2 because it has the same UUID
        assert!(set.contains(&identity2));
    }
}
