//! Snapshot storage trait and types for stream processors
//!
//! Snapshots now only track stream offsets, not engine data.
//! Engines manage their own persistence internally.

/// Trait for snapshot storage backends
///
/// Snapshots only track which offset each stream has processed.
/// Engine state is managed internally by the engines themselves.
pub trait SnapshotStore: Send + Sync {
    /// Update the current stream position
    fn update(&self, stream: &str, offset: u64);

    /// Get the current stream offset
    fn get(&self, stream: &str) -> Option<u64>;
}
