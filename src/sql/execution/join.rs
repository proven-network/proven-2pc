//! Join execution for SQL queries

use crate::error::Result;
use crate::sql::types::value::{Row, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Hash joiner for equijoin operations
pub struct HashJoiner {
    /// The column index in the left table  
    left_column: usize,
    /// The column index in the right table
    right_column: usize,
    /// Hash table built from the left side
    hash_table: HashMap<Value, Vec<Arc<Row>>>,
}

impl HashJoiner {
    /// Create a new hash joiner
    pub fn new(left_column: usize, right_column: usize) -> Self {
        Self {
            left_column,
            right_column,
            hash_table: HashMap::new(),
        }
    }

    /// Build phase: add rows from the left side to the hash table
    pub fn build(&mut self, row: Arc<Row>) -> Result<()> {
        let key = row[self.left_column].clone();
        self.hash_table
            .entry(key)
            .or_insert_with(Vec::new)
            .push(row);
        Ok(())
    }

    /// Probe phase: find matching rows for a right-side row
    pub fn probe(&self, right_row: &Row) -> Vec<Arc<Row>> {
        let key = &right_row[self.right_column];
        self.hash_table
            .get(key)
            .map(|rows| {
                rows.iter()
                    .map(|left_row| {
                        let mut joined = (**left_row).clone();
                        joined.extend_from_slice(right_row);
                        Arc::new(joined)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Probe for outer join: returns joined rows or right row with NULLs
    pub fn probe_outer(&self, right_row: &Row, left_columns: usize) -> Vec<Arc<Row>> {
        let matches = self.probe(right_row);
        if !matches.is_empty() {
            matches
        } else {
            // No match - return right row with NULLs for left columns
            let mut joined = vec![Value::Null; left_columns];
            joined.extend_from_slice(right_row);
            vec![Arc::new(joined)]
        }
    }
}

/// Nested loop joiner for general join predicates
pub struct NestedLoopJoiner {
    /// All rows from the left side
    left_rows: Vec<Arc<Row>>,
}

impl NestedLoopJoiner {
    /// Create a new nested loop joiner
    pub fn new() -> Self {
        Self {
            left_rows: Vec::new(),
        }
    }

    /// Add a row from the left side
    pub fn add_left(&mut self, row: Arc<Row>) {
        self.left_rows.push(row);
    }

    /// Join a right row with all left rows using a predicate
    pub fn join_with<F>(&self, right_row: &Row, predicate: F) -> Vec<Arc<Row>>
    where
        F: Fn(&Row, &Row) -> bool,
    {
        self.left_rows
            .iter()
            .filter(|left_row| predicate(left_row, right_row))
            .map(|left_row| {
                let mut joined = (**left_row).clone();
                joined.extend_from_slice(right_row);
                Arc::new(joined)
            })
            .collect()
    }

    /// Join for outer join with a predicate
    pub fn join_outer_with<F>(
        &self,
        right_row: &Row,
        predicate: F,
        left_columns: usize,
    ) -> Vec<Arc<Row>>
    where
        F: Fn(&Row, &Row) -> bool,
    {
        let matches = self.join_with(right_row, predicate);
        if !matches.is_empty() {
            matches
        } else {
            // No match - return right row with NULLs for left columns
            let mut joined = vec![Value::Null; left_columns];
            joined.extend_from_slice(right_row);
            vec![Arc::new(joined)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_join() {
        let mut joiner = HashJoiner::new(0, 0);

        // Build phase - add left rows
        joiner
            .build(Arc::new(vec![
                Value::Integer(1),
                Value::String("Alice".into()),
            ]))
            .unwrap();
        joiner
            .build(Arc::new(vec![
                Value::Integer(2),
                Value::String("Bob".into()),
            ]))
            .unwrap();

        // Probe phase - find matches
        let right_row = vec![Value::Integer(1), Value::String("Engineer".into())];
        let results = joiner.probe(&right_row);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(1));
        assert_eq!(results[0][1], Value::String("Alice".into()));
        assert_eq!(results[0][2], Value::Integer(1));
        assert_eq!(results[0][3], Value::String("Engineer".into()));
    }

    #[test]
    fn test_nested_loop_join() {
        let mut joiner = NestedLoopJoiner::new();

        // Add left rows
        joiner.add_left(Arc::new(vec![
            Value::Integer(1),
            Value::String("Alice".into()),
        ]));
        joiner.add_left(Arc::new(vec![
            Value::Integer(2),
            Value::String("Bob".into()),
        ]));

        // Join with right row using predicate
        let right_row = vec![Value::Integer(1), Value::String("Engineer".into())];
        let results = joiner.join_with(&right_row, |left, right| {
            left[0] == right[0] // Join on first column
        });

        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], Value::Integer(1));
        assert_eq!(results[0][1], Value::String("Alice".into()));
        assert_eq!(results[0][2], Value::Integer(1));
        assert_eq!(results[0][3], Value::String("Engineer".into()));
    }
}
