//! Streaming iterators for storage_new that merge fjall and in-memory MVCC data

use crate::error::{Error, Result};
use crate::storage::data_history::DataHistoryStore;
use crate::storage::encoding::{decode_row_key, deserialize};
use crate::storage::engine::TableMetadata;
use crate::storage::types::{FjallIterator, Row, RowId, WriteOp};
use crate::storage::uncommitted_data::{TableActiveData, UncommittedDataStore};

use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Iterator over rows visible to a transaction
/// Merges in-memory active writes with persisted fjall data
///
/// This struct holds the necessary locks to ensure the data remains valid
/// for the lifetime of the iterator.
pub struct TableIterator<'a> {
    // Table-specific active data (loaded once at iterator creation)
    active_data: TableActiveData,

    // Iterator position in active writes
    active_position: usize,

    // Buffered row from fjall iterator
    buffered_fjall: Option<(RowId, Arc<Row>)>,

    // Iterator over persisted data in fjall
    data_iter: FjallIterator<'a>,

    // History operations that need to be applied (loaded once)
    // Map from row_id to operations that need to be reversed
    history_ops: HashMap<RowId, Vec<WriteOp>>,

    // Track which rows we've already returned
    seen_rows: HashSet<RowId>,
}

impl<'a> TableIterator<'a> {
    pub(crate) fn new(
        txn_id: HlcTimestamp,
        tables: &'a HashMap<String, Arc<TableMetadata>>,
        uncommitted_data: Arc<UncommittedDataStore>,
        data_history: Arc<DataHistoryStore>,
        table_name: &str,
    ) -> Result<Self> {
        // Get the table metadata
        let table_meta = tables
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        let data_partition = &table_meta.data_partition;

        // Get uncommitted data for this table (single scan of uncommitted_data)
        let active_data = uncommitted_data.get_table_active_data(txn_id, table_name);

        // Load all history operations for this table ONCE
        // This avoids repeated queries during iteration
        // Optimization: skip entirely if data_history is empty
        let history_ops = if data_history.is_empty() {
            HashMap::new()
        } else {
            data_history.get_table_ops_after(txn_id, table_name)?
        };

        // Create fjall iterator - convert Slice to Box<[u8]>
        let data_iter = Box::new(data_partition.iter().map(|result| {
            result.map(|(k, v)| {
                let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                (k_bytes, v_bytes)
            })
        }));

        Ok(Self {
            active_data,
            active_position: 0,
            history_ops,
            data_iter,
            seen_rows: HashSet::new(),
            buffered_fjall: None,
        })
    }

    /// Check if a row from fjall is visible to this transaction
    fn is_fjall_row_visible(&self, row_id: RowId) -> bool {
        // If we deleted it in our transaction, not visible
        if self.active_data.deletes.contains(&row_id) {
            return false;
        }

        // Without version_partition, we assume:
        // - Anything in the data partition is visible (committed)
        // - Unless it was deleted by us (checked above)
        // - History operations are handled separately by history_ops
        true
    }

    /// Get next row from fjall iterator
    fn next_fjall_row(&mut self) -> Result<Option<(RowId, Arc<Row>)>> {
        // Return buffered row if we have one
        if let Some(buffered) = self.buffered_fjall.take() {
            return Ok(Some(buffered));
        }

        // Get next from fjall
        while let Some(result) = self.data_iter.next() {
            let (key_bytes, value_bytes) = result?;

            if let Some(row_id) = decode_row_key(&key_bytes) {
                // Skip if we've already seen this row
                if self.seen_rows.contains(&row_id) {
                    continue;
                }

                // Check visibility
                if self.is_fjall_row_visible(row_id) {
                    let row: Row = deserialize(&value_bytes)?;

                    // Check if this row has any history operations (from our preloaded map)
                    // SEMANTICS: Operations committed AFTER snapshot should be INVISIBLE
                    // - If row was INSERTED after snapshot → hide it completely
                    // - If row was UPDATED after snapshot → show old version
                    // - If row was DELETED after snapshot → show it (was still there at snapshot)
                    if let Some(ops) = self.history_ops.get(&row_id) {
                        // Check if this row was inserted after our snapshot
                        // If so, it shouldn't be visible at all
                        if ops.iter().any(|op| matches!(op, WriteOp::Insert { .. })) {
                            // Row was inserted after our snapshot, skip it entirely
                            continue;
                        }

                        // Start with the current committed state
                        let mut historical_row = row.clone();

                        // Apply operations in REVERSE chronological order to reconstruct historical state
                        // Each operation tells us "what changed", so we undo them one by one
                        for op in ops.iter().rev() {
                            match op {
                                WriteOp::Update {
                                    old_row, new_row, ..
                                } => {
                                    // An update happened after our snapshot
                                    // The current state has new_row, but at snapshot time it was old_row
                                    // Only revert if the current value matches what was updated
                                    if historical_row.values == new_row.values {
                                        historical_row = (**old_row).clone();
                                    }
                                }
                                WriteOp::Delete {
                                    row: deleted_row, ..
                                } => {
                                    // Row was deleted after our snapshot
                                    // At snapshot time, it was still alive with these values
                                    historical_row = (**deleted_row).clone();
                                    historical_row.deleted = false; // Undelete it for the snapshot
                                }
                                WriteOp::Insert { .. } => {
                                    // Already handled above - shouldn't reach here
                                }
                            }
                        }

                        return Ok(Some((row_id, Arc::new(historical_row))));
                    }

                    // No history operations for this row - return it as-is
                    return Ok(Some((row_id, Arc::new(row))));
                }
            }
        }

        Ok(None)
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = Result<Arc<Row>>;

    fn next(&mut self) -> Option<Self::Item> {
        // We need to merge active writes with fjall data in RowId order

        // Get next from active writes if we have any left
        let active_next = if self.active_position < self.active_data.writes.len() {
            let entries: Vec<_> = self.active_data.writes.iter().collect();
            let (row_id, row) = entries[self.active_position];
            Some((*row_id, row.clone()))
        } else {
            None
        };

        // Get next from fjall (if not already buffered)
        if self.buffered_fjall.is_none() {
            match self.next_fjall_row() {
                Ok(Some(row)) => self.buffered_fjall = Some(row),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }
        }

        // Compare and return the smaller RowId
        match (active_next, &self.buffered_fjall) {
            // Both have data - return smaller RowId
            (Some((active_id, active_row)), Some((fjall_id, _))) => {
                if active_id < *fjall_id {
                    // Return active write, advance position
                    self.active_position += 1;
                    self.seen_rows.insert(active_id);
                    Some(Ok(active_row))
                } else if active_id == *fjall_id {
                    // Active write overrides fjall - skip fjall version
                    self.active_position += 1;
                    self.seen_rows.insert(active_id);
                    self.buffered_fjall = None; // Discard fjall version
                    Some(Ok(active_row))
                } else {
                    // Return fjall row, don't advance active position
                    let (fjall_id, fjall_row) = self.buffered_fjall.take().unwrap();
                    self.seen_rows.insert(fjall_id);
                    Some(Ok(fjall_row))
                }
            }

            // Only active write available
            (Some((active_id, active_row)), None) => {
                self.active_position += 1;
                self.seen_rows.insert(active_id);
                Some(Ok(active_row))
            }

            // Only fjall data available
            (None, Some(_)) => {
                let (fjall_id, fjall_row) = self.buffered_fjall.take().unwrap();
                self.seen_rows.insert(fjall_id);
                Some(Ok(fjall_row))
            }

            // No more data
            (None, None) => None,
        }
    }
}

/// Iterator that also returns row IDs (for UPDATE/DELETE operations)
pub struct TableIteratorWithIds<'a> {
    inner: TableIterator<'a>,
}

impl<'a> TableIteratorWithIds<'a> {
    pub fn new(
        txn_id: HlcTimestamp,
        tables: &'a HashMap<String, Arc<TableMetadata>>,
        uncommitted_data: Arc<UncommittedDataStore>,
        data_history: Arc<DataHistoryStore>,
        table_name: &str,
    ) -> Result<Self> {
        Ok(Self {
            inner: TableIterator::new(txn_id, tables, uncommitted_data, data_history, table_name)?,
        })
    }
}

impl<'a> Iterator for TableIteratorWithIds<'a> {
    type Item = Result<(RowId, Arc<Row>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|result| {
            result.map(|row| {
                let row_id = row.id;
                (row_id, row)
            })
        })
    }
}

/// Double-ended iterator for reverse scanning
/// (Fjall supports this natively!)
pub struct TableIteratorReverse<'a> {
    // Table-specific active data
    active_data: TableActiveData,

    // Iterator position in active writes
    active_position: usize,

    // Active writes as Vec for reverse iteration
    active_writes_vec: Vec<(RowId, Arc<Row>)>,

    // Buffered row from fjall iterator
    buffered_fjall: Option<(RowId, Arc<Row>)>,

    // Iterator over persisted data in fjall (reversed)
    data_iter: FjallIterator<'a>,

    // History operations that need to be applied (loaded once)
    history_ops: HashMap<RowId, Vec<WriteOp>>,

    // Track which rows we've already returned
    seen_rows: HashSet<RowId>,
}

impl<'a> TableIteratorReverse<'a> {
    pub fn new(
        txn_id: HlcTimestamp,
        tables: &'a HashMap<String, Arc<TableMetadata>>,
        uncommitted_data: Arc<UncommittedDataStore>,
        data_history: Arc<DataHistoryStore>,
        table_name: &str,
    ) -> Result<Self> {
        // Get the table metadata
        let table_meta = tables
            .get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        let data_partition = &table_meta.data_partition;

        // Get uncommitted data for this table (single scan of uncommitted_data)
        let active_data = uncommitted_data.get_table_active_data(txn_id, table_name);

        // Convert writes to Vec and reverse for reverse iteration
        let mut active_writes_vec: Vec<_> = active_data
            .writes
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        active_writes_vec.reverse();

        // Load all history operations for this table ONCE
        // Optimization: skip entirely if data_history is empty
        let history_ops = if data_history.is_empty() {
            HashMap::new()
        } else {
            data_history.get_table_ops_after(txn_id, table_name)?
        };

        // Create reverse fjall iterator - convert Slice to Box<[u8]>
        let data_iter = Box::new(
            data_partition
                .iter()
                .rev() // Reverse iteration!
                .map(|result| {
                    result.map(|(k, v)| {
                        let k_bytes: Box<[u8]> = k.to_vec().into_boxed_slice();
                        let v_bytes: Box<[u8]> = v.to_vec().into_boxed_slice();
                        (k_bytes, v_bytes)
                    })
                }),
        );

        Ok(Self {
            active_data,
            active_writes_vec,
            active_position: 0,
            buffered_fjall: None,
            data_iter,
            history_ops,
            seen_rows: HashSet::new(),
        })
    }

    /// Check if a row from fjall is visible to this transaction
    fn is_fjall_row_visible(&self, row_id: RowId) -> bool {
        // If we deleted it in our transaction, not visible
        if self.active_data.deletes.contains(&row_id) {
            return false;
        }

        // Without version_partition, we assume:
        // - Anything in the data partition is visible (committed)
        // - Unless it was deleted by us (checked above)
        // - History operations are handled separately by history_ops
        true
    }

    /// Get next row from fjall iterator (in reverse)
    fn next_fjall_row(&mut self) -> Result<Option<(RowId, Arc<Row>)>> {
        // Return buffered row if we have one
        if let Some(buffered) = self.buffered_fjall.take() {
            return Ok(Some(buffered));
        }

        // Get next from fjall (reversed)
        while let Some(result) = self.data_iter.next() {
            let (key_bytes, value_bytes) = result?;

            if let Some(row_id) = decode_row_key(&key_bytes) {
                // Skip if we've already seen this row
                if self.seen_rows.contains(&row_id) {
                    continue;
                }

                // Check visibility
                if self.is_fjall_row_visible(row_id) {
                    let row: Row = deserialize(&value_bytes)?;

                    // Check if this row has any history operations (from our preloaded map)
                    // SEMANTICS: Operations committed AFTER snapshot should be INVISIBLE
                    // - If row was INSERTED after snapshot → hide it completely
                    // - If row was UPDATED after snapshot → show old version
                    // - If row was DELETED after snapshot → show it (was still there at snapshot)
                    if let Some(ops) = self.history_ops.get(&row_id) {
                        // Check if this row was inserted after our snapshot
                        // If so, it shouldn't be visible at all
                        if ops.iter().any(|op| matches!(op, WriteOp::Insert { .. })) {
                            // Row was inserted after our snapshot, skip it entirely
                            continue;
                        }

                        // Start with the current committed state
                        let mut historical_row = row.clone();

                        // Apply operations in REVERSE chronological order to reconstruct historical state
                        // Each operation tells us "what changed", so we undo them one by one
                        for op in ops.iter().rev() {
                            match op {
                                WriteOp::Update {
                                    old_row, new_row, ..
                                } => {
                                    // An update happened after our snapshot
                                    // The current state has new_row, but at snapshot time it was old_row
                                    // Only revert if the current value matches what was updated
                                    if historical_row.values == new_row.values {
                                        historical_row = (**old_row).clone();
                                    }
                                }
                                WriteOp::Delete {
                                    row: deleted_row, ..
                                } => {
                                    // Row was deleted after our snapshot
                                    // At snapshot time, it was still alive with these values
                                    historical_row = (**deleted_row).clone();
                                    historical_row.deleted = false; // Undelete it for the snapshot
                                }
                                WriteOp::Insert { .. } => {
                                    // Already handled above - shouldn't reach here
                                }
                            }
                        }

                        return Ok(Some((row_id, Arc::new(historical_row))));
                    }

                    // No history operations for this row - return it as-is
                    return Ok(Some((row_id, Arc::new(row))));
                }
            }
        }

        Ok(None)
    }
}

impl<'a> Iterator for TableIteratorReverse<'a> {
    type Item = Result<Arc<Row>>;

    fn next(&mut self) -> Option<Self::Item> {
        // We need to merge active writes with fjall data in reverse RowId order

        // Get next from active writes if we have any left
        let active_next = if self.active_position < self.active_writes_vec.len() {
            let (row_id, row) = &self.active_writes_vec[self.active_position];
            Some((*row_id, row.clone()))
        } else {
            None
        };

        // Get next from fjall (if not already buffered)
        if self.buffered_fjall.is_none() {
            match self.next_fjall_row() {
                Ok(Some(row)) => self.buffered_fjall = Some(row),
                Ok(None) => {}
                Err(e) => return Some(Err(e)),
            }
        }

        // Compare and return the larger RowId (reverse order)
        match (active_next, &self.buffered_fjall) {
            // Both have data - return larger RowId (reverse)
            (Some((active_id, active_row)), Some((fjall_id, _))) => {
                if active_id > *fjall_id {
                    // Return active write, advance position
                    self.active_position += 1;
                    self.seen_rows.insert(active_id);
                    Some(Ok(active_row))
                } else if active_id == *fjall_id {
                    // Active write overrides fjall - skip fjall version
                    self.active_position += 1;
                    self.seen_rows.insert(active_id);
                    self.buffered_fjall = None; // Discard fjall version
                    Some(Ok(active_row))
                } else {
                    // Return fjall row, don't advance active position
                    let (fjall_id, fjall_row) = self.buffered_fjall.take().unwrap();
                    self.seen_rows.insert(fjall_id);
                    Some(Ok(fjall_row))
                }
            }

            // Only active write available
            (Some((active_id, active_row)), None) => {
                self.active_position += 1;
                self.seen_rows.insert(active_id);
                Some(Ok(active_row))
            }

            // Only fjall data available
            (None, Some(_)) => {
                let (fjall_id, fjall_row) = self.buffered_fjall.take().unwrap();
                self.seen_rows.insert(fjall_id);
                Some(Ok(fjall_row))
            }

            // No more data
            (None, None) => None,
        }
    }
}
