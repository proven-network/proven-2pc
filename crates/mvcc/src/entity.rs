//! MVCC entity traits
//!
//! These traits define the contract between the generic MVCC storage
//! and the crate-specific implementations (KV, SQL, Queue, Resource).

use crate::encoding::{Decode, Encode};

/// Defines how a crate's data is stored in MVCC storage
pub trait MvccEntity: 'static + Sized {
    /// Type of keys (e.g., String for KV, (TableName, RowId) for SQL)
    type Key: Clone + Encode + Decode;

    /// Type of values (e.g., Value for KV, Row for SQL)
    type Value: Clone + Encode + Decode;

    /// Type of deltas - how to apply/unapply operations for time-travel
    /// Each impl can define their own delta format
    type Delta: Clone + Encode + Decode + MvccDelta<Self>;

    /// Entity name for partition naming (e.g., "kv", "sql_data", "sql_index")
    fn entity_name() -> &'static str;
}

/// Delta operations that can be applied/unapplied for time-travel
///
/// This trait is more flexible than storing old_value/new_value pairs,
/// as each crate can define their own delta semantics.
///
/// # Example: KV Delta
///
/// ```ignore
/// enum KvDelta {
///     Put { key: String, new_value: Value, old_value: Option<Value> },
///     Delete { key: String, old_value: Value },
/// }
///
/// impl MvccDelta<KvEntity> for KvDelta {
///     fn apply(&self, _current: Option<Value>) -> Option<Value> {
///         match self {
///             KvDelta::Put { new_value, .. } => Some(new_value.clone()),
///             KvDelta::Delete { .. } => None,
///         }
///     }
///
///     fn unapply(&self, _current: Option<Value>) -> Option<Value> {
///         match self {
///             KvDelta::Put { old_value, .. } => old_value.clone(),
///             KvDelta::Delete { old_value, .. } => Some(old_value.clone()),
///         }
///     }
/// }
/// ```
pub trait MvccDelta<E: MvccEntity> {
    /// Extract the key this delta affects
    ///
    /// Returns an owned key to avoid lifetime issues with composite keys.
    /// The clone overhead is typically negligible since keys are usually small.
    fn key(&self) -> E::Key;

    /// Apply this delta forward in time
    ///
    /// Given the current value, return the value after applying this delta.
    /// This is used when reconstructing the state at a past snapshot time.
    ///
    /// # Arguments
    /// - `current`: The current value (or None if the key doesn't exist)
    ///
    /// # Returns
    /// - `Some(value)`: The value after applying this delta
    /// - `None`: The key should not exist after applying this delta (e.g., delete)
    fn apply(&self, current: Option<E::Value>) -> Option<E::Value>;

    /// Unapply this delta backward in time
    ///
    /// Given the current value, return the value before this delta was applied.
    /// This is used when reconstructing the state at a snapshot time before this delta.
    ///
    /// # Arguments
    /// - `current`: The current value (or None if the key doesn't exist)
    ///
    /// # Returns
    /// - `Some(value)`: The value before applying this delta
    /// - `None`: The key didn't exist before this delta (e.g., insert)
    fn unapply(&self, current: Option<E::Value>) -> Option<E::Value>;

    /// Merge two deltas that affect the same key
    ///
    /// When multiple deltas for the same key exist in a transaction, they need to be
    /// consolidated. This method defines how to combine them.
    ///
    /// # Arguments
    /// - `self`: The earlier delta (in sequence order)
    /// - `next`: The later delta (in sequence order)
    ///
    /// # Returns
    /// The consolidated delta that represents the effect of applying both deltas in sequence.
    ///
    /// # Examples
    /// - Coarse-grained deltas (full row updates): `next` wins (last write semantics)
    /// - Fine-grained deltas (increments): combine the amounts (additive semantics)
    /// - Mixed operations: later operation type takes precedence
    fn merge(self, next: Self) -> Self;
}
