//! Predicate-based locking system for conflict detection
//!
//! This module implements a pure predicate-based locking system that determines
//! all conflicts at planning time without needing row-level locks.

use crate::types::Value;
use std::ops::Bound;

/// A predicate representing a condition on a table
#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub table: String,
    pub condition: PredicateCondition,
}

/// The condition part of a predicate
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateCondition {
    /// The entire table (no filter)
    All,

    /// Single row by primary key
    PrimaryKey(Value),

    /// Range on a column
    Range {
        column: String,
        start: Bound<Value>,
        end: Bound<Value>,
    },

    /// Equality check on a column
    Equals { column: String, value: Value },

    /// Conjunction (AND) of multiple conditions
    And(Vec<PredicateCondition>),

    /// Disjunction (OR) of multiple conditions
    Or(Vec<PredicateCondition>),

    /// Arbitrary expression (fallback for complex predicates)
    Expression(crate::types::Expression),
}

impl Predicate {
    /// Create a predicate for a full table scan
    pub fn full_table(table: String) -> Self {
        Self {
            table,
            condition: PredicateCondition::All,
        }
    }

    /// Create a predicate for a primary key lookup
    pub fn primary_key(table: String, key: Value) -> Self {
        Self {
            table,
            condition: PredicateCondition::PrimaryKey(key),
        }
    }

    /// Check if two predicates might access the same data
    pub fn conflicts_with(&self, other: &Predicate) -> bool {
        // Different tables never conflict
        if self.table != other.table {
            return false;
        }

        // Check if conditions overlap
        self.condition.overlaps(&other.condition)
    }
}

impl PredicateCondition {
    /// Check if two conditions might overlap
    pub fn overlaps(&self, other: &PredicateCondition) -> bool {
        match (self, other) {
            // All overlaps with everything
            (PredicateCondition::All, _) | (_, PredicateCondition::All) => true,

            // Primary keys only overlap if equal
            (PredicateCondition::PrimaryKey(k1), PredicateCondition::PrimaryKey(k2)) => k1 == k2,

            // Ranges overlap if they're on the same column and ranges intersect
            (
                PredicateCondition::Range {
                    column: c1,
                    start: s1,
                    end: e1,
                },
                PredicateCondition::Range {
                    column: c2,
                    start: s2,
                    end: e2,
                },
            ) if c1 == c2 => Self::ranges_overlap(s1, e1, s2, e2),

            // PK vs Range: check if PK falls in range (assuming PK column is "id")
            (
                PredicateCondition::PrimaryKey(key),
                PredicateCondition::Range { column, start, end },
            )
            | (
                PredicateCondition::Range { column, start, end },
                PredicateCondition::PrimaryKey(key),
            ) if column == "id" => Self::value_in_range(key, start, end),

            // Equality checks overlap if same column and value
            (
                PredicateCondition::Equals {
                    column: c1,
                    value: v1,
                },
                PredicateCondition::Equals {
                    column: c2,
                    value: v2,
                },
            ) => c1 == c2 && v1 == v2,

            // Equality vs Range: check if value falls in range
            (
                PredicateCondition::Equals { column: c1, value },
                PredicateCondition::Range {
                    column: c2,
                    start,
                    end,
                },
            )
            | (
                PredicateCondition::Range {
                    column: c2,
                    start,
                    end,
                },
                PredicateCondition::Equals { column: c1, value },
            ) if c1 == c2 => Self::value_in_range(value, start, end),

            // Conjunction: overlaps if any part overlaps
            (PredicateCondition::And(preds), other) | (other, PredicateCondition::And(preds)) => {
                preds.iter().any(|p| p.overlaps(other))
            }

            // Disjunction: overlaps if any part overlaps
            (PredicateCondition::Or(preds), other) | (other, PredicateCondition::Or(preds)) => {
                preds.iter().any(|p| p.overlaps(other))
            }

            // Expression: conservatively assume overlap
            (PredicateCondition::Expression(_), _) | (_, PredicateCondition::Expression(_)) => true,

            // Default: no overlap
            _ => false,
        }
    }

    /// Check if two ranges overlap
    fn ranges_overlap(
        s1: &Bound<Value>,
        e1: &Bound<Value>,
        s2: &Bound<Value>,
        e2: &Bound<Value>,
    ) -> bool {
        // Check if start of one range is before end of other
        let start1_before_end2 = match (s1, e2) {
            (Bound::Unbounded, _) => true,
            (_, Bound::Unbounded) => true,
            (Bound::Included(s1), Bound::Included(e2)) => s1 <= e2,
            (Bound::Included(s1), Bound::Excluded(e2)) => s1 < e2,
            (Bound::Excluded(s1), Bound::Included(e2)) => s1 < e2,
            (Bound::Excluded(s1), Bound::Excluded(e2)) => s1 < e2,
        };

        let start2_before_end1 = match (s2, e1) {
            (Bound::Unbounded, _) => true,
            (_, Bound::Unbounded) => true,
            (Bound::Included(s2), Bound::Included(e1)) => s2 <= e1,
            (Bound::Included(s2), Bound::Excluded(e1)) => s2 < e1,
            (Bound::Excluded(s2), Bound::Included(e1)) => s2 < e1,
            (Bound::Excluded(s2), Bound::Excluded(e1)) => s2 < e1,
        };

        start1_before_end2 && start2_before_end1
    }

    /// Check if a value falls within a range
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
}

/// Predicates for a complete query
#[derive(Debug, Clone, Default)]
pub struct QueryPredicates {
    /// Predicates for data we'll read
    pub reads: Vec<Predicate>,
    /// Predicates for data we'll modify
    pub writes: Vec<Predicate>,
    /// Predicates for new data we'll insert
    pub inserts: Vec<Predicate>,
}

impl QueryPredicates {
    pub fn new() -> Self {
        Self {
            reads: Vec::new(),
            writes: Vec::new(),
            inserts: Vec::new(),
        }
    }

    /// Merge predicates from another query into this one
    pub fn merge(&mut self, other: QueryPredicates) {
        self.reads.extend(other.reads);
        self.writes.extend(other.writes);
        self.inserts.extend(other.inserts);
    }

    /// Release read predicates at PREPARE time
    pub fn release_reads(&mut self) {
        self.reads.clear();
    }

    /// Check if this is a read-only query
    pub fn is_read_only(&self) -> bool {
        self.writes.is_empty() && self.inserts.is_empty()
    }

    /// Find conflicts with another set of predicates
    pub fn conflicts_with(&self, other: &QueryPredicates) -> Option<ConflictInfo> {
        // Read-Write conflicts: we read what they write
        for our_read in &self.reads {
            for their_write in &other.writes {
                if our_read.conflicts_with(their_write) {
                    return Some(ConflictInfo::ReadWrite {
                        our_predicate: our_read.clone(),
                        their_predicate: their_write.clone(),
                    });
                }
            }
            // Also check against their inserts
            for their_insert in &other.inserts {
                if our_read.conflicts_with(their_insert) {
                    return Some(ConflictInfo::ReadWrite {
                        our_predicate: our_read.clone(),
                        their_predicate: their_insert.clone(),
                    });
                }
            }
        }

        // Write-Write conflicts: we both write to same data
        for our_write in &self.writes {
            for their_write in &other.writes {
                if our_write.conflicts_with(their_write) {
                    return Some(ConflictInfo::WriteWrite {
                        our_predicate: our_write.clone(),
                        their_predicate: their_write.clone(),
                    });
                }
            }
        }

        // Write-Read conflicts: they read what we write
        for our_write in &self.writes {
            for their_read in &other.reads {
                if our_write.conflicts_with(their_read) {
                    return Some(ConflictInfo::WriteRead {
                        our_predicate: our_write.clone(),
                        their_predicate: their_read.clone(),
                    });
                }
            }
        }

        // Insert-Read conflicts: they read what we're inserting (phantom prevention)
        for our_insert in &self.inserts {
            for their_read in &other.reads {
                if our_insert.conflicts_with(their_read) {
                    return Some(ConflictInfo::WriteRead {
                        our_predicate: our_insert.clone(),
                        their_predicate: their_read.clone(),
                    });
                }
            }
        }

        // Insert-Insert conflicts: inserting same primary key
        for our_insert in &self.inserts {
            for their_insert in &other.inserts {
                if our_insert.conflicts_with(their_insert) {
                    return Some(ConflictInfo::InsertInsert {
                        our_predicate: our_insert.clone(),
                        their_predicate: their_insert.clone(),
                    });
                }
            }
        }

        None
    }
}

/// Information about a predicate conflict
#[derive(Debug, Clone)]
pub enum ConflictInfo {
    /// We're trying to read what they're writing
    ReadWrite {
        our_predicate: Predicate,
        their_predicate: Predicate,
    },
    /// We're both trying to write to the same data
    WriteWrite {
        our_predicate: Predicate,
        their_predicate: Predicate,
    },
    /// They're reading what we're writing
    WriteRead {
        our_predicate: Predicate,
        their_predicate: Predicate,
    },
    /// We're both inserting (possibly same PK)
    InsertInsert {
        our_predicate: Predicate,
        their_predicate: Predicate,
    },
}

impl ConflictInfo {
    /// Get a human-readable description of the conflict
    pub fn description(&self) -> String {
        match self {
            ConflictInfo::ReadWrite {
                our_predicate,
                their_predicate,
            } => {
                format!(
                    "Read-Write conflict: reading from {} while other transaction is writing to {}",
                    our_predicate.table, their_predicate.table
                )
            }
            ConflictInfo::WriteWrite {
                our_predicate,
                their_predicate: _,
            } => {
                format!(
                    "Write-Write conflict: both writing to table {}",
                    our_predicate.table
                )
            }
            ConflictInfo::WriteRead {
                our_predicate,
                their_predicate,
            } => {
                format!(
                    "Write-Read conflict: writing to {} while other transaction is reading from {}",
                    our_predicate.table, their_predicate.table
                )
            }
            ConflictInfo::InsertInsert { our_predicate, .. } => {
                format!(
                    "Insert-Insert conflict: both inserting into table {}",
                    our_predicate.table
                )
            }
        }
    }
}
