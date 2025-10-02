//! Streaming iterators for index operations with MVCC support
//!
//! This module provides memory-efficient streaming index scans that correctly
//! handle snapshot isolation by pre-capturing MVCC state.

use crate::error::Result;
use crate::storage::encoding::decode_row_id_from_index_key;
use crate::storage::types::{FjallIterator, RowId};
use crate::storage::uncommitted_index::IndexOp;
use crate::types::value::Value;
use std::collections::{HashMap, HashSet};

/// Streaming iterator for index lookups with MVCC snapshot isolation
pub struct IndexLookupIterator<'a> {
    /// Fjall index partition iterator
    index_iter: FjallIterator<'a>,

    /// Uncommitted operations from current transaction (captured at creation)
    uncommitted_ops: Vec<IndexOp>,

    /// History operations (committed after our snapshot) - captured at creation
    /// Maps row_id -> operations that need to be reversed
    history_ops: HashMap<RowId, Vec<IndexOp>>,

    /// Track which row IDs we've already returned
    seen_rows: HashSet<RowId>,

    /// Position in uncommitted_ops iterator
    uncommitted_position: usize,

    /// The index values we're looking for (for filtering uncommitted ops)
    lookup_values: Vec<Value>,
}

impl<'a> IndexLookupIterator<'a> {
    /// Create new iterator - captures MVCC state at construction time
    pub fn new(
        index_iter: FjallIterator<'a>,
        uncommitted_ops: Vec<IndexOp>,
        history_ops: HashMap<RowId, Vec<IndexOp>>,
        lookup_values: Vec<Value>,
    ) -> Self {
        Self {
            index_iter,
            uncommitted_ops,
            history_ops,
            seen_rows: HashSet::new(),
            uncommitted_position: 0,
            lookup_values,
        }
    }

    /// Check if a row ID should be visible based on MVCC rules
    fn is_row_visible(&self, row_id: RowId) -> bool {
        // Check history: was this row inserted AFTER our snapshot?
        if let Some(ops) = self.history_ops.get(&row_id) {
            // Check if any operation makes this row invisible
            // An INSERT after our snapshot means the row didn't exist at snapshot time
            if ops.iter().any(|op| matches!(op, IndexOp::Insert { .. })) {
                return false;
            }
            // If row was deleted after snapshot, it WAS visible at snapshot time
            // This is handled by the fact that we're checking index entries
        }

        // No INSERT operations after snapshot, row is visible
        true
    }

    /// Get next row ID from fjall index
    fn next_fjall_row_id(&mut self) -> Result<Option<RowId>> {
        while let Some(result) = self.index_iter.next() {
            let (key, _value) = result?;

            if let Some(row_id) = decode_row_id_from_index_key(&key) {
                // Skip if already seen
                if self.seen_rows.contains(&row_id) {
                    continue;
                }

                // Check MVCC visibility
                if self.is_row_visible(row_id) {
                    return Ok(Some(row_id));
                }
            }
        }
        Ok(None)
    }

    /// Get next row ID from uncommitted operations
    fn next_uncommitted_row_id(&mut self) -> Option<RowId> {
        while self.uncommitted_position < self.uncommitted_ops.len() {
            let op = &self.uncommitted_ops[self.uncommitted_position];
            self.uncommitted_position += 1;

            // Only include inserts that match our lookup values
            if let IndexOp::Insert { values, row_id, .. } = op
                && values == &self.lookup_values
                && !self.seen_rows.contains(row_id)
            {
                return Some(*row_id);
            }
        }
        None
    }
}

impl<'a> Iterator for IndexLookupIterator<'a> {
    type Item = Result<RowId>;

    fn next(&mut self) -> Option<Self::Item> {
        // Merge uncommitted and fjall sources
        // Try uncommitted first, then fjall

        // Try uncommitted first
        if let Some(row_id) = self.next_uncommitted_row_id() {
            self.seen_rows.insert(row_id);
            return Some(Ok(row_id));
        }

        // Then try fjall
        match self.next_fjall_row_id() {
            Ok(Some(row_id)) => {
                self.seen_rows.insert(row_id);
                Some(Ok(row_id))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Streaming iterator for index range scans with MVCC support
pub struct IndexRangeScanIterator<'a> {
    /// Fjall index range iterator
    index_iter: FjallIterator<'a>,

    /// Uncommitted operations from current transaction
    uncommitted_ops: Vec<IndexOp>,

    /// History operations (committed after our snapshot)
    history_ops: HashMap<RowId, Vec<IndexOp>>,

    /// Track which row IDs we've already returned
    seen_rows: HashSet<RowId>,

    /// Position in uncommitted_ops iterator
    uncommitted_position: usize,

    /// Range bounds for filtering uncommitted ops
    start_values: Option<Vec<Value>>,
    end_values: Option<Vec<Value>>,
}

impl<'a> IndexRangeScanIterator<'a> {
    pub fn new(
        index_iter: FjallIterator<'a>,
        uncommitted_ops: Vec<IndexOp>,
        history_ops: HashMap<RowId, Vec<IndexOp>>,
        start_values: Option<Vec<Value>>,
        end_values: Option<Vec<Value>>,
    ) -> Self {
        Self {
            index_iter,
            uncommitted_ops,
            history_ops,
            seen_rows: HashSet::new(),
            uncommitted_position: 0,
            start_values,
            end_values,
        }
    }

    fn is_in_range(&self, values: &[Value]) -> bool {
        match (&self.start_values, &self.end_values) {
            (None, None) => true,
            (Some(start), None) => values >= start.as_slice(),
            (None, Some(end)) => values <= end.as_slice(),
            (Some(start), Some(end)) => values >= start.as_slice() && values <= end.as_slice(),
        }
    }

    fn is_row_visible(&self, row_id: RowId) -> bool {
        if let Some(ops) = self.history_ops.get(&row_id) {
            // Check if any operation makes this row invisible
            // An INSERT after our snapshot means the row didn't exist at snapshot time
            if ops.iter().any(|op| matches!(op, IndexOp::Insert { .. })) {
                return false;
            }
        }
        true
    }
}

impl<'a> Iterator for IndexRangeScanIterator<'a> {
    type Item = Result<RowId>;

    fn next(&mut self) -> Option<Self::Item> {
        // First, check uncommitted operations
        while self.uncommitted_position < self.uncommitted_ops.len() {
            let op = &self.uncommitted_ops[self.uncommitted_position];
            self.uncommitted_position += 1;

            if let IndexOp::Insert { values, row_id, .. } = op
                && self.is_in_range(values)
                && !self.seen_rows.contains(row_id)
            {
                self.seen_rows.insert(*row_id);
                return Some(Ok(*row_id));
            }
        }

        // Then check fjall
        while let Some(result) = self.index_iter.next() {
            match result {
                Ok((key, _value)) => {
                    if let Some(row_id) = decode_row_id_from_index_key(&key) {
                        if self.seen_rows.contains(&row_id) {
                            continue;
                        }

                        if self.is_row_visible(row_id) {
                            self.seen_rows.insert(row_id);
                            return Some(Ok(row_id));
                        }
                    }
                }
                Err(e) => return Some(Err(crate::error::Error::from(e))),
            }
        }

        None
    }
}
