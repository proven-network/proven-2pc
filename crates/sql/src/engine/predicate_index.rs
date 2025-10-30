//! Fast predicate indexing for conflict detection
//!
//! This module provides O(1) or O(log n) lookups for finding potentially
//! conflicting transactions, avoiding O(n²) checking of all predicates.

use crate::semantic::predicate::{Predicate, PredicateCondition, QueryPredicates};
use crate::types::Value;
use proven_common::TransactionId;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;

/// Fast index for looking up transactions by their predicates
pub struct PredicateIndex {
    /// Table -> transactions operating on that table
    by_table: HashMap<String, TableIndex>,
}

/// Index for predicates within a single table
struct TableIndex {
    /// Primary key -> transactions accessing that specific key
    pk_index: HashMap<Value, HashSet<TransactionId>>,

    /// Column -> transactions with predicates on that column
    column_index: HashMap<String, ColumnIndex>,

    /// Transactions doing full table operations (SELECT * with no WHERE)
    full_table_txns: HashSet<TransactionId>,

    /// All transactions touching this table (for quick membership check)
    all_txns: HashSet<TransactionId>,
}

/// Index for predicates on a specific column
struct ColumnIndex {
    /// Exact value matches (WHERE col = value)
    exact_values: HashMap<Value, HashSet<TransactionId>>,

    /// Range predicates (WHERE col BETWEEN x AND y)
    ranges: Vec<(Bound<Value>, Bound<Value>, TransactionId)>,

    /// LIKE patterns (WHERE col LIKE pattern)
    patterns: Vec<(String, TransactionId)>,

    /// NULL checks (WHERE col IS NULL or IS NOT NULL)
    null_checks: HashSet<TransactionId>,
    not_null_checks: HashSet<TransactionId>,
}

impl PredicateIndex {
    /// Create a new empty index
    pub fn new() -> Self {
        Self {
            by_table: HashMap::new(),
        }
    }

    /// Add a transaction's predicates to the index
    pub fn add_transaction(&mut self, txn_id: TransactionId, predicates: &QueryPredicates) {
        // Index all reads
        for pred in &predicates.reads {
            self.index_predicate(txn_id, pred, PredicateType::Read);
        }

        // Index all writes
        for pred in &predicates.writes {
            self.index_predicate(txn_id, pred, PredicateType::Write);
        }

        // Index all inserts
        for pred in &predicates.inserts {
            self.index_predicate(txn_id, pred, PredicateType::Insert);
        }
    }

    /// Update a transaction's predicates in the index (adds new predicates)
    pub fn update_transaction(&mut self, txn_id: TransactionId, new_predicates: &QueryPredicates) {
        // Add new predicates to the index
        // Note: We don't remove old ones as predicates are additive within a transaction
        self.add_transaction(txn_id, new_predicates);
    }

    /// Remove a transaction from the index (on commit/abort)
    pub fn remove_transaction(&mut self, txn_id: &TransactionId) {
        // Clean up all index entries for this transaction
        let mut empty_tables = Vec::new();

        for (table_name, table_idx) in &mut self.by_table {
            table_idx.remove_transaction(txn_id);

            // Mark empty tables for removal
            if table_idx.is_empty() {
                empty_tables.push(table_name.clone());
            }
        }

        // Remove empty table indices
        for table_name in empty_tables {
            self.by_table.remove(&table_name);
        }
    }

    /// Find transactions that might conflict with given predicates
    /// Returns a set of transaction IDs that need detailed conflict checking
    pub fn find_potential_conflicts(
        &self,
        txn_id: TransactionId,
        predicates: &QueryPredicates,
    ) -> HashSet<TransactionId> {
        let mut potential_conflicts = HashSet::new();

        // Check reads against writes/inserts
        for pred in &predicates.reads {
            if let Some(table_idx) = self.by_table.get(&pred.table) {
                let candidates = table_idx.get_write_candidates(&pred.condition);
                potential_conflicts.extend(candidates);
            }
        }

        // Check writes against reads/writes
        for pred in &predicates.writes {
            if let Some(table_idx) = self.by_table.get(&pred.table) {
                let candidates = table_idx.get_all_candidates(&pred.condition);
                potential_conflicts.extend(candidates);
            }
        }

        // Check inserts against reads/inserts
        for pred in &predicates.inserts {
            if let Some(table_idx) = self.by_table.get(&pred.table) {
                let candidates = table_idx.get_read_insert_candidates(&pred.condition);
                potential_conflicts.extend(candidates);
            }
        }

        // Remove self from candidates
        potential_conflicts.remove(&txn_id);

        potential_conflicts
    }

    /// Index a single predicate
    fn index_predicate(
        &mut self,
        txn_id: TransactionId,
        pred: &Predicate,
        _pred_type: PredicateType,
    ) {
        let table_idx = self
            .by_table
            .entry(pred.table.clone())
            .or_insert_with(TableIndex::new);

        table_idx.add_predicate(txn_id, &pred.condition);
    }
}

/// Type of predicate (for future optimization)
#[derive(Debug, Clone, Copy)]
enum PredicateType {
    Read,
    Write,
    Insert,
}

impl TableIndex {
    fn new() -> Self {
        Self {
            pk_index: HashMap::new(),
            column_index: HashMap::new(),
            full_table_txns: HashSet::new(),
            all_txns: HashSet::new(),
        }
    }

    fn add_predicate(&mut self, txn_id: TransactionId, condition: &PredicateCondition) {
        // Track this transaction
        self.all_txns.insert(txn_id);

        match condition {
            PredicateCondition::All => {
                self.full_table_txns.insert(txn_id);
            }

            PredicateCondition::PrimaryKey(key) => {
                self.pk_index.entry(key.clone()).or_default().insert(txn_id);
            }

            PredicateCondition::Equals { column, value } => {
                let col_idx = self
                    .column_index
                    .entry(column.clone())
                    .or_insert_with(ColumnIndex::new);
                col_idx
                    .exact_values
                    .entry(value.clone())
                    .or_default()
                    .insert(txn_id);
            }

            PredicateCondition::Range { column, start, end } => {
                let col_idx = self
                    .column_index
                    .entry(column.clone())
                    .or_insert_with(ColumnIndex::new);
                col_idx.ranges.push((start.clone(), end.clone(), txn_id));
            }

            PredicateCondition::Like { column, pattern } => {
                let col_idx = self
                    .column_index
                    .entry(column.clone())
                    .or_insert_with(ColumnIndex::new);
                col_idx.patterns.push((pattern.clone(), txn_id));
            }

            PredicateCondition::IsNull { column } => {
                let col_idx = self
                    .column_index
                    .entry(column.clone())
                    .or_insert_with(ColumnIndex::new);
                col_idx.null_checks.insert(txn_id);
            }

            PredicateCondition::IsNotNull { column } => {
                let col_idx = self
                    .column_index
                    .entry(column.clone())
                    .or_insert_with(ColumnIndex::new);
                col_idx.not_null_checks.insert(txn_id);
            }

            PredicateCondition::Or(conditions) => {
                // For OR conditions, index each part
                for cond in conditions {
                    self.add_predicate(txn_id, cond);
                }
            }
        }
    }

    fn remove_transaction(&mut self, txn_id: &TransactionId) {
        // Remove from all indices
        self.all_txns.remove(txn_id);
        self.full_table_txns.remove(txn_id);

        // Remove from PK index
        self.pk_index.retain(|_, txns| {
            txns.remove(txn_id);
            !txns.is_empty()
        });

        // Remove from column indices
        for col_idx in self.column_index.values_mut() {
            col_idx.remove_transaction(txn_id);
        }

        // Remove empty column indices
        self.column_index.retain(|_, col_idx| !col_idx.is_empty());
    }

    fn is_empty(&self) -> bool {
        self.all_txns.is_empty()
    }

    /// Get candidates that might conflict with writes (reads + writes)
    fn get_all_candidates(&self, condition: &PredicateCondition) -> HashSet<TransactionId> {
        let mut candidates = HashSet::new();

        // Full table scans conflict with everything
        candidates.extend(&self.full_table_txns);

        // Add specific candidates based on condition
        match condition {
            PredicateCondition::All => {
                // Conflicts with all transactions on this table
                candidates.extend(&self.all_txns);
            }

            PredicateCondition::PrimaryKey(key) => {
                // Only conflicts with transactions accessing same PK
                if let Some(txns) = self.pk_index.get(key) {
                    candidates.extend(txns);
                }
            }

            PredicateCondition::Equals { column, value } => {
                if let Some(col_idx) = self.column_index.get(column) {
                    // Exact match
                    if let Some(txns) = col_idx.exact_values.get(value) {
                        candidates.extend(txns);
                    }
                    // Ranges that might include this value
                    candidates.extend(col_idx.get_range_candidates(value));
                    // LIKE patterns that might match
                    candidates.extend(col_idx.get_pattern_candidates(value));
                }
            }

            PredicateCondition::Range { column, start, end } => {
                if let Some(col_idx) = self.column_index.get(column) {
                    candidates.extend(col_idx.get_overlapping_range_candidates(start, end));
                }
            }

            _ => {
                // Conservative: check against all transactions on this table
                // This could be optimized further
                candidates.extend(&self.all_txns);
            }
        }

        candidates
    }

    /// Get candidates for write operations
    fn get_write_candidates(&self, condition: &PredicateCondition) -> HashSet<TransactionId> {
        // For now, same as all candidates
        // Could optimize by tracking read vs write predicates separately
        self.get_all_candidates(condition)
    }

    /// Get candidates for read/insert conflicts
    fn get_read_insert_candidates(&self, condition: &PredicateCondition) -> HashSet<TransactionId> {
        // For now, same as all candidates
        // Could optimize by tracking operation types
        self.get_all_candidates(condition)
    }
}

impl ColumnIndex {
    fn new() -> Self {
        Self {
            exact_values: HashMap::new(),
            ranges: Vec::new(),
            patterns: Vec::new(),
            null_checks: HashSet::new(),
            not_null_checks: HashSet::new(),
        }
    }

    fn remove_transaction(&mut self, txn_id: &TransactionId) {
        // Remove from exact values
        self.exact_values.retain(|_, txns| {
            txns.remove(txn_id);
            !txns.is_empty()
        });

        // Remove from ranges
        self.ranges.retain(|(_, _, t)| t != txn_id);

        // Remove from patterns
        self.patterns.retain(|(_, t)| t != txn_id);

        // Remove from null checks
        self.null_checks.remove(txn_id);
        self.not_null_checks.remove(txn_id);
    }

    fn is_empty(&self) -> bool {
        self.exact_values.is_empty()
            && self.ranges.is_empty()
            && self.patterns.is_empty()
            && self.null_checks.is_empty()
            && self.not_null_checks.is_empty()
    }

    /// Get transactions with ranges that might include a value
    fn get_range_candidates(&self, value: &Value) -> HashSet<TransactionId> {
        let mut candidates = HashSet::new();

        for (start, end, txn_id) in &self.ranges {
            if Self::value_in_range(value, start, end) {
                candidates.insert(*txn_id);
            }
        }

        candidates
    }

    /// Get transactions with overlapping ranges
    fn get_overlapping_range_candidates(
        &self,
        start: &Bound<Value>,
        end: &Bound<Value>,
    ) -> HashSet<TransactionId> {
        let mut candidates = HashSet::new();

        // Check exact values that fall in range
        for (value, txns) in &self.exact_values {
            if Self::value_in_range(value, start, end) {
                candidates.extend(txns);
            }
        }

        // Check overlapping ranges
        for (r_start, r_end, txn_id) in &self.ranges {
            if Self::ranges_overlap(start, end, r_start, r_end) {
                candidates.insert(*txn_id);
            }
        }

        candidates
    }

    /// Get transactions with patterns that might match a value
    fn get_pattern_candidates(&self, value: &Value) -> HashSet<TransactionId> {
        let mut candidates = HashSet::new();

        // Only string values can match patterns
        if let Value::Str(s) = value {
            for (pattern, txn_id) in &self.patterns {
                if Self::might_match_pattern(s, pattern) {
                    candidates.insert(*txn_id);
                }
            }
        }

        candidates
    }

    fn value_in_range(value: &Value, start: &Bound<Value>, end: &Bound<Value>) -> bool {
        let after_start = match start {
            Bound::Unbounded => true,
            Bound::Included(s) => value >= s,
            Bound::Excluded(s) => value > s,
        };

        let before_end = match end {
            Bound::Unbounded => true,
            Bound::Included(e) => value <= e,
            Bound::Excluded(e) => value < e,
        };

        after_start && before_end
    }

    fn ranges_overlap(
        s1: &Bound<Value>,
        e1: &Bound<Value>,
        s2: &Bound<Value>,
        e2: &Bound<Value>,
    ) -> bool {
        // Check if start of one range is before end of other
        let start1_before_end2 = match (s1, e2) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Included(s1), Bound::Included(e2)) => s1 <= e2,
            (Bound::Included(s1), Bound::Excluded(e2)) => s1 < e2,
            (Bound::Excluded(s1), Bound::Included(e2)) => s1 < e2,
            (Bound::Excluded(s1), Bound::Excluded(e2)) => s1 < e2,
        };

        let start2_before_end1 = match (s2, e1) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Included(s2), Bound::Included(e1)) => s2 <= e1,
            (Bound::Included(s2), Bound::Excluded(e1)) => s2 < e1,
            (Bound::Excluded(s2), Bound::Included(e1)) => s2 < e1,
            (Bound::Excluded(s2), Bound::Excluded(e1)) => s2 < e1,
        };

        start1_before_end2 && start2_before_end1
    }

    fn might_match_pattern(value: &str, pattern: &str) -> bool {
        // Simple pattern matching for common cases
        if pattern.starts_with('%') && pattern.ends_with('%') {
            let infix = &pattern[1..pattern.len() - 1];
            if !infix.contains('%') {
                return value.contains(infix);
            }
        } else if let Some(prefix) = pattern.strip_suffix('%') {
            if !prefix.contains('%') {
                return value.starts_with(prefix);
            }
        } else if let Some(suffix) = pattern.strip_prefix('%')
            && !suffix.contains('%')
        {
            return value.ends_with(suffix);
        }

        // Conservative: assume might match for complex patterns
        true
    }
}
