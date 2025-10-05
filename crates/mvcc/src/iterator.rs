//! MVCC-aware iterators that merge uncommitted, committed, and historical data
//!
//! The iterator captures all necessary state at construction time to provide
//! consistent snapshot isolation semantics during iteration.

use crate::FjallIter;
use crate::encoding::Decode;
use crate::entity::{MvccDelta, MvccEntity};
use crate::error::Result;
use std::collections::BTreeMap;
use std::marker::PhantomData;

/// Iterator over entities visible at a snapshot time
///
/// This iterator:
/// 1. Pre-loads uncommitted deltas for the transaction
/// 2. Pre-loads history deltas committed after the snapshot
/// 3. Merges them with the committed data partition during iteration
/// 4. Applies deltas to reconstruct the correct snapshot view
pub struct MvccIterator<'a, E: MvccEntity> {
    /// Iterator over committed data partition
    committed_iter: FjallIter<'a>,

    /// Uncommitted deltas from this transaction (key -> delta)
    /// Pre-loaded at iterator creation
    /// Using BTreeMap for lookups while maintaining sorted order
    uncommitted: BTreeMap<Vec<u8>, E::Delta>,

    /// History deltas committed AFTER our snapshot (key -> deltas)
    /// Pre-loaded at iterator creation
    /// These need to be "unapplied" to get the snapshot view
    /// Using BTreeMap for deterministic ordering
    history: BTreeMap<Vec<u8>, Vec<E::Delta>>,

    /// Iterator over uncommitted entries (for entries with no committed version)
    uncommitted_iter: Option<std::vec::IntoIter<(Vec<u8>, E::Delta)>>,

    /// Keys we've already returned (to avoid duplicates)
    seen_keys: std::collections::HashSet<Vec<u8>>,

    /// Phantom data for entity type
    _phantom: PhantomData<E>,
}

impl<'a, E: MvccEntity> MvccIterator<'a, E> {
    /// Create a new iterator
    ///
    /// This captures all MVCC state at construction time:
    /// - Uncommitted deltas for this transaction
    /// - History deltas committed after the snapshot
    pub(crate) fn new(
        committed_iter: FjallIter<'a>,
        uncommitted: BTreeMap<Vec<u8>, E::Delta>,
        history: BTreeMap<Vec<u8>, Vec<E::Delta>>,
    ) -> Self {
        // Note: uncommitted is a BTreeMap<Vec<u8>, E::Delta> which maintains sorted order
        // This means we have ONE delta per key (the storage layer consolidates them)
        // BTreeMap iteration is already sorted, so no need to sort explicitly
        let uncommitted_vec: Vec<(Vec<u8>, E::Delta)> = uncommitted
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Self {
            committed_iter,
            uncommitted,
            history,
            uncommitted_iter: Some(uncommitted_vec.into_iter()),
            seen_keys: std::collections::HashSet::new(),
            _phantom: PhantomData,
        }
    }

    /// Apply MVCC semantics to reconstruct the value at snapshot time
    ///
    /// Process:
    /// 1. Start with committed value
    /// 2. Apply history deltas in reverse to undo changes after snapshot
    /// 3. Check uncommitted deltas for read-your-own-writes
    fn apply_mvcc(&self, key_bytes: &[u8], committed_value: E::Value) -> Option<E::Value> {
        let mut current_value = Some(committed_value);

        // Apply history deltas in REVERSE to undo changes after snapshot
        if let Some(deltas) = self.history.get(key_bytes) {
            for delta in deltas.iter().rev() {
                current_value = delta.unapply(current_value);
            }
        }

        // Check uncommitted deltas for read-your-own-writes
        if let Some(delta) = self.uncommitted.get(key_bytes) {
            current_value = delta.apply(current_value);
        }

        current_value
    }
}

impl<'a, E: MvccEntity> Iterator for MvccIterator<'a, E> {
    type Item = Result<(E::Key, E::Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        // First, iterate over committed data
        while let Some(result) = self.committed_iter.next() {
            match result {
                Ok((ref key_box, ref value_box)) => {
                    let key_bytes: &[u8] = key_box;
                    let value_bytes: &[u8] = value_box;

                    // Mark this key as seen
                    self.seen_keys.insert(key_bytes.to_vec());

                    // Decode key
                    let key = match E::Key::decode(key_bytes) {
                        Ok(k) => k,
                        Err(e) => return Some(Err(e)),
                    };

                    // Decode committed value
                    let committed_value = match E::Value::decode(value_bytes) {
                        Ok(v) => v,
                        Err(e) => return Some(Err(e)),
                    };

                    // Apply MVCC semantics
                    if let Some(final_value) = self.apply_mvcc(key_bytes, committed_value) {
                        return Some(Ok((key, final_value)));
                    }
                    // If None, the entry was deleted - skip it
                }
                Err(e) => return Some(Err(crate::Error::Fjall(e))),
            }
        }

        // After exhausting committed data, iterate over uncommitted entries
        // that don't have committed versions (pure inserts)
        if let Some(ref mut uncommitted_iter) = self.uncommitted_iter {
            for (key_bytes, delta) in uncommitted_iter.by_ref() {
                // Skip if we already returned this key from committed iterator
                if self.seen_keys.contains(&key_bytes) {
                    continue;
                }

                // Apply delta starting from None (no committed value)
                if let Some(final_value) = delta.apply(None) {
                    // Decode the key
                    match E::Key::decode(&key_bytes) {
                        Ok(key) => {
                            self.seen_keys.insert(key_bytes);
                            return Some(Ok((key, final_value)));
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                // If apply returns None, it's a delete - skip it
            }
        }

        None
    }
}
