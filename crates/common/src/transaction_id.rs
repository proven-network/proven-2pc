//! Transaction identifier using UUIDv7
//!
//! UUIDv7 provides time-ordered uniqueness with deterministic total ordering,
//! which is all that's needed for wound-wait deadlock resolution.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Transaction identifier using UUIDv7 for time-ordered uniqueness
///
/// UUIDv7 provides:
/// - 16 bytes total
/// - Time-ordered at millisecond precision
/// - Deterministic total ordering for wound-wait
/// - Standard format with library support
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(Uuid);

impl TransactionId {
    /// Generate a new transaction ID using UUIDv7
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Create from existing UUID (for testing/deserialization)
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Convert to bytes (16 bytes, big-endian)
    pub fn to_bytes(&self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// Parse from bytes
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Result<Self, String> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| format!("Invalid transaction ID: {}", e))
    }

    /// Extract timestamp from UUIDv7 for time-bucketed partition management
    ///
    /// This is ONLY used for bucketing partitions for efficient cleanup.
    /// NEVER use this for MVCC snapshot isolation or transaction ordering -
    /// those must use the full TransactionId to maintain consistency with wound-wait.
    ///
    /// UUIDv7 encodes Unix timestamp in milliseconds in the first 48 bits.
    /// We convert this to microseconds for consistency with Timestamp type.
    pub fn to_timestamp_for_bucketing(&self) -> crate::Timestamp {
        // Extract the 48-bit Unix timestamp in milliseconds from UUIDv7
        let bytes = self.0.as_bytes();

        // First 48 bits (6 bytes) contain Unix timestamp in milliseconds
        let timestamp_ms = u64::from_be_bytes([
            0, 0, // pad with zeros
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
        ]);

        // Convert milliseconds to microseconds
        crate::Timestamp::from_micros(timestamp_ms * 1000)
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for TransactionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransactionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Lexicographic comparison of bytes provides total ordering
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ordering() {
        let id1 = TransactionId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = TransactionId::new();

        // Later transaction should have higher ID (roughly)
        // Note: Not guaranteed due to millisecond precision, but likely
        assert!(id1 <= id2);
    }

    #[test]
    fn test_roundtrip() {
        let id = TransactionId::new();
        let s = id.to_string();
        let parsed = TransactionId::parse(&s).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let id = TransactionId::new();
        let bytes = id.to_bytes();
        let from_bytes = TransactionId::from_bytes(bytes);
        assert_eq!(id, from_bytes);
    }

    #[test]
    fn test_deterministic_ordering() {
        // Create multiple IDs and verify they maintain consistent ordering
        let mut ids = [TransactionId::new(); 10];
        for id in ids.iter_mut().skip(1) {
            std::thread::sleep(std::time::Duration::from_millis(1));
            *id = TransactionId::new();
        }

        // Verify ordering is maintained
        for i in 0..9 {
            assert!(ids[i] <= ids[i + 1]);
        }
    }

    #[test]
    fn test_hash_eq_consistency() {
        use std::collections::HashMap;

        let id1 = TransactionId::new();
        let id2 = id1; // Copy

        let mut map = HashMap::new();
        map.insert(id1, "value");

        // Should be able to retrieve with copy
        assert_eq!(map.get(&id2), Some(&"value"));
    }

    #[test]
    fn test_from_uuid() {
        let uuid = Uuid::now_v7();
        let txn_id = TransactionId::from_uuid(uuid);
        assert_eq!(txn_id.as_uuid(), &uuid);
    }
}
