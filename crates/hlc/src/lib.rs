//! HLC timestamp implementation for transaction ordering.
//!
//! This module provides Hybrid Logical Clock timestamps that serve as both
//! transaction IDs and ordering mechanism, eliminating the need for separate
//! priority tracking.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Node identifier for HLC timestamps.
///
/// In a real distributed system, this would be a cryptographic key.
/// For our SQL engine, we use a simple u64 for demonstration.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        NodeId(id)
    }

    pub fn from_seed(seed: u8) -> Self {
        NodeId(seed as u64)
    }

    pub fn from_hex(hex: &str) -> Result<Self, String> {
        u64::from_str_radix(hex, 16)
            .map(NodeId)
            .map_err(|e| e.to_string())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Immutable HLC timestamp with total ordering.
///
/// Provides globally unique, monotonic timestamps for distributed transactions.
/// The total ordering is: physical time, then logical counter, then node ID.
///
/// This serves as both the transaction ID and the priority mechanism:
/// - Earlier timestamps (lower values) have higher priority
/// - Used for wound-wait: older transactions wound younger ones
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    /// Physical time component (microseconds since Unix epoch)
    pub physical: u64,
    /// Logical counter for uniqueness within same physical time
    pub logical: u32,
    /// Node that generated this timestamp
    pub node_id: NodeId,
}

impl HlcTimestamp {
    /// Create a new HLC timestamp.
    pub const fn new(physical: u64, logical: u32, node_id: NodeId) -> Self {
        Self {
            physical,
            logical,
            node_id,
        }
    }

    /// Create a new HLC timestamp from physical time and node ID.
    pub fn from_physical_time(physical: u64, node_id: NodeId) -> Self {
        Self {
            physical,
            logical: 0,
            node_id,
        }
    }

    /// Check if this timestamp is older (has higher priority) than another.
    /// In wound-wait, older transactions wound younger ones.
    pub fn is_older_than(&self, other: &Self) -> bool {
        self < other
    }

    /// Check if two timestamps might be concurrent.
    pub fn might_be_concurrent(&self, other: &Self) -> bool {
        self.physical == other.physical && self.node_id != other.node_id
    }

    /// Convert to nanoseconds representation for compatibility
    pub fn as_nanos(&self) -> u64 {
        // Convert microseconds to nanoseconds and add logical component
        self.physical * 1_000 + self.logical as u64
    }

    /// Parse from string format: "physical_logical_nodeid"
    /// Returns Result for better error reporting.
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.split('_').collect();
        if parts.len() != 3 {
            return Err(format!(
                "Invalid HLC timestamp format: {} (expected physical_logical_nodeid)",
                s
            ));
        }

        let physical = parts[0]
            .parse()
            .map_err(|_| format!("Invalid physical component: {}", parts[0]))?;
        let logical = parts[1]
            .parse()
            .map_err(|_| format!("Invalid logical component: {}", parts[1]))?;
        let node_id =
            NodeId::from_hex(parts[2]).map_err(|_| format!("Invalid node ID: {}", parts[2]))?;

        Ok(Self::new(physical, logical, node_id))
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Total ordering: physical time, logical counter, then node ID
        match self.physical.cmp(&other.physical) {
            std::cmp::Ordering::Equal => match self.logical.cmp(&other.logical) {
                std::cmp::Ordering::Equal => self.node_id.cmp(&other.node_id),
                other => other,
            },
            other => other,
        }
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}_{}", self.physical, self.logical, self.node_id)
    }
}

/// HLC clock for generating timestamps.
pub struct HlcClock {
    node_id: NodeId,
    last_physical: AtomicU64,
    logical: AtomicU32,
}

impl HlcClock {
    /// Create a new HLC clock for a node.
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            last_physical: AtomicU64::new(0),
            logical: AtomicU32::new(0),
        }
    }

    /// Generate a new HLC timestamp.
    pub fn now(&self) -> HlcTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let last = self.last_physical.load(Ordering::SeqCst);

        if physical > last {
            // New physical time, reset logical counter
            self.last_physical.store(physical, Ordering::SeqCst);
            self.logical.store(0, Ordering::SeqCst);
            HlcTimestamp::new(physical, 0, self.node_id)
        } else {
            // Same physical time, increment logical counter
            let logical = self.logical.fetch_add(1, Ordering::SeqCst) + 1;
            HlcTimestamp::new(last, logical, self.node_id)
        }
    }

    /// Update the clock based on a received timestamp.
    pub fn update(&self, received: &HlcTimestamp) -> HlcTimestamp {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;

        let last = self.last_physical.load(Ordering::SeqCst);
        let max_physical = physical.max(received.physical).max(last);

        if max_physical > last {
            self.last_physical.store(max_physical, Ordering::SeqCst);

            if max_physical > received.physical && max_physical > physical {
                // Our clock was ahead
                self.logical.store(0, Ordering::SeqCst);
                HlcTimestamp::new(max_physical, 0, self.node_id)
            } else if received.physical > physical && received.physical > last {
                // Received timestamp is ahead
                let logical = received.logical + 1;
                self.logical.store(logical, Ordering::SeqCst);
                HlcTimestamp::new(max_physical, logical, self.node_id)
            } else {
                // Physical time advanced
                self.logical.store(0, Ordering::SeqCst);
                HlcTimestamp::new(max_physical, 0, self.node_id)
            }
        } else {
            // Same physical time, increment logical
            let logical = self.logical.fetch_add(1, Ordering::SeqCst) + 1;
            HlcTimestamp::new(last, logical, self.node_id)
        }
    }
}

/// Thread-safe HLC clock that can be shared across threads.
pub type SharedHlcClock = Arc<HlcClock>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_ordering() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let ts1 = HlcTimestamp::new(100, 0, node1);
        let ts2 = HlcTimestamp::new(100, 1, node1);
        let ts3 = HlcTimestamp::new(101, 0, node1);
        let ts4 = HlcTimestamp::new(100, 0, node2);

        // Physical time dominates
        assert!(ts1 < ts3);
        assert!(ts2 < ts3);

        // Logical counter breaks ties
        assert!(ts1 < ts2);

        // Node ID breaks final ties
        assert_ne!(ts1, ts4); // Different nodes
        assert!(ts1 < ts4 || ts4 < ts1); // Deterministic order
    }

    #[test]
    fn test_string_roundtrip() {
        let node = NodeId::new(42);
        let ts = HlcTimestamp::new(123_456_789, 10, node);

        let s = ts.to_string();
        let parsed = HlcTimestamp::parse(&s).unwrap();

        assert_eq!(ts, parsed);
    }

    #[test]
    fn test_concurrency_detection() {
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);

        let ts1 = HlcTimestamp::new(100, 0, node1);
        let ts2 = HlcTimestamp::new(100, 0, node2);
        let ts3 = HlcTimestamp::new(101, 0, node1);

        assert!(ts1.might_be_concurrent(&ts2));
        assert!(!ts1.might_be_concurrent(&ts3));
        assert!(!ts1.might_be_concurrent(&ts1)); // Same node
    }

    #[test]
    fn test_clock_generation() {
        let clock = HlcClock::new(NodeId::new(1));

        let ts1 = clock.now();
        let ts2 = clock.now();
        let ts3 = clock.now();

        // Timestamps should be monotonically increasing
        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_is_older_than() {
        let node = NodeId::new(1);

        let older = HlcTimestamp::new(100, 0, node);
        let younger = HlcTimestamp::new(200, 0, node);

        assert!(older.is_older_than(&younger));
        assert!(!younger.is_older_than(&older));
    }
}
