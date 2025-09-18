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

    /// LIKE pattern matching on a column
    Like { column: String, pattern: String },

    /// IS NULL check on a column
    IsNull { column: String },

    /// IS NOT NULL check on a column
    IsNotNull { column: String },

    /// Disjunction (OR) of multiple conditions (used for IN lists)
    Or(Vec<PredicateCondition>),
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

            // LIKE patterns: conservatively check if patterns might match overlapping rows
            (
                PredicateCondition::Like {
                    column: c1,
                    pattern: p1,
                },
                PredicateCondition::Like {
                    column: c2,
                    pattern: p2,
                },
            ) if c1 == c2 => Self::like_patterns_might_overlap(p1, p2),

            // LIKE vs Equals: check if value might match pattern
            (
                PredicateCondition::Like {
                    column: c1,
                    pattern,
                },
                PredicateCondition::Equals { column: c2, value },
            )
            | (
                PredicateCondition::Equals { column: c2, value },
                PredicateCondition::Like {
                    column: c1,
                    pattern,
                },
            ) if c1 == c2 => {
                // Check if the value actually matches the LIKE pattern
                Self::value_matches_like_pattern(value, pattern)
            }

            // IS NULL and IS NOT NULL are disjoint
            (
                PredicateCondition::IsNull { column: c1 },
                PredicateCondition::IsNotNull { column: c2 },
            )
            | (
                PredicateCondition::IsNotNull { column: c2 },
                PredicateCondition::IsNull { column: c1 },
            ) if c1 == c2 => false,

            // IS NULL overlaps with IS NULL on same column
            (
                PredicateCondition::IsNull { column: c1 },
                PredicateCondition::IsNull { column: c2 },
            ) => c1 == c2,

            // IS NOT NULL overlaps with IS NOT NULL on same column
            (
                PredicateCondition::IsNotNull { column: c1 },
                PredicateCondition::IsNotNull { column: c2 },
            ) => c1 == c2,

            // IS NULL doesn't overlap with non-null values
            (
                PredicateCondition::IsNull { column: c1 },
                PredicateCondition::Equals { column: c2, .. },
            )
            | (
                PredicateCondition::Equals { column: c2, .. },
                PredicateCondition::IsNull { column: c1 },
            ) if c1 == c2 => false,

            // IS NOT NULL might overlap with non-null values
            (
                PredicateCondition::IsNotNull { column: c1 },
                PredicateCondition::Equals { column: c2, .. },
            )
            | (
                PredicateCondition::Equals { column: c2, .. },
                PredicateCondition::IsNotNull { column: c1 },
            ) if c1 == c2 => true,

            // Disjunction (OR): overlaps if any part overlaps
            (PredicateCondition::Or(preds), other) | (other, PredicateCondition::Or(preds)) => {
                preds.iter().any(|p| p.overlaps(other))
            }

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

    /// Check if two LIKE patterns might match overlapping sets of values
    fn like_patterns_might_overlap(p1: &str, p2: &str) -> bool {
        // For suffix patterns like '%@gmail.com' and '%@yahoo.com'
        if p1.starts_with('%') && p2.starts_with('%') {
            let suffix1 = &p1[1..];
            let suffix2 = &p2[1..];
            // If suffixes are completely different, they don't overlap
            if suffix1 != suffix2 && !suffix1.ends_with(suffix2) && !suffix2.ends_with(suffix1) {
                return false;
            }
        }

        // For prefix patterns like '/home/%' and '/var/%'
        if p1.ends_with('%') && p2.ends_with('%') {
            let prefix1 = &p1[..p1.len() - 1];
            let prefix2 = &p2[..p2.len() - 1];
            // If prefixes are completely different, they don't overlap
            if prefix1 != prefix2 && !prefix1.starts_with(prefix2) && !prefix2.starts_with(prefix1)
            {
                return false;
            }
        }

        // Conservative: if we can't determine, assume they might overlap
        true
    }

    /// Check if a value matches a SQL LIKE pattern
    fn value_matches_like_pattern(value: &Value, pattern: &str) -> bool {
        // Only string values can match LIKE patterns
        let value_str = match value {
            Value::Str(s) => s,
            _ => return false,
        };

        // Handle prefix patterns (e.g., 'prefix%')
        if pattern.ends_with('%') && !pattern[..pattern.len() - 1].contains('%') {
            let prefix = &pattern[..pattern.len() - 1];
            return value_str.starts_with(prefix);
        }

        // Handle suffix patterns (e.g., '%suffix')
        if pattern.starts_with('%') && !pattern[1..].contains('%') {
            let suffix = &pattern[1..];
            return value_str.ends_with(suffix);
        }

        // Handle infix patterns (e.g., '%infix%')
        if pattern.starts_with('%') && pattern.ends_with('%') && pattern.len() > 2 {
            let infix = &pattern[1..pattern.len() - 1];
            if !infix.contains('%') {
                return value_str.contains(infix);
            }
        }

        // Handle exact match (no wildcards)
        if !pattern.contains('%') && !pattern.contains('_') {
            return value_str == pattern;
        }

        // For complex patterns with multiple % or _ wildcards,
        // conservatively assume they might match
        true
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

    /// Find conflicts with another set of predicates
    pub fn conflicts_with(&self, other: &QueryPredicates) -> Option<ConflictInfo> {
        // Read-Write conflicts: we read what they write
        for our_read in &self.reads {
            for their_write in &other.writes {
                if our_read.conflicts_with(their_write) {
                    return Some(ConflictInfo::ReadWrite);
                }
            }
            // Also check against their inserts
            for their_insert in &other.inserts {
                if our_read.conflicts_with(their_insert) {
                    return Some(ConflictInfo::ReadWrite);
                }
            }
        }

        // Write-Write conflicts: we both write to same data
        for our_write in &self.writes {
            for their_write in &other.writes {
                if our_write.conflicts_with(their_write) {
                    return Some(ConflictInfo::WriteWrite);
                }
            }
        }

        // Write-Read conflicts: they read what we write
        for our_write in &self.writes {
            for their_read in &other.reads {
                if our_write.conflicts_with(their_read) {
                    return Some(ConflictInfo::WriteRead);
                }
            }
        }

        // Insert-Read conflicts: they read what we're inserting (phantom prevention)
        for our_insert in &self.inserts {
            for their_read in &other.reads {
                if our_insert.conflicts_with(their_read) {
                    return Some(ConflictInfo::WriteRead);
                }
            }
        }

        // Insert-Insert conflicts: inserting same primary key
        for our_insert in &self.inserts {
            for their_insert in &other.inserts {
                if our_insert.conflicts_with(their_insert) {
                    return Some(ConflictInfo::InsertInsert);
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
    ReadWrite,
    /// We're both trying to write to the same data
    WriteWrite,
    /// They're reading what we're writing
    WriteRead,
    /// We're both inserting (possibly same PK)
    InsertInsert,
}
