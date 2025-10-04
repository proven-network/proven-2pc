//! MVCC-aware join implementations
//!
//! This module provides join operations that work with MVCC transactions
//! and visibility rules. Unlike toydb's pure iterator-based joins, these
//! joins integrate with transaction contexts and respect MVCC isolation.

use super::expression;
use crate::error::{Error, Result};
use crate::storage::Storage;
use crate::types::context::ExecutionContext;
use crate::types::expression::Expression;
use crate::types::query::{JoinType, RowRef, Rows};
use crate::types::value::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// NestedLoopJoiner implements MVCC-aware nested loop joins.
///
/// For every row in the left source, iterate over the right source and join
/// them. Rows are filtered on the join predicate, if given. This version
/// respects MVCC transaction visibility and isolation rules.
pub struct NestedLoopJoiner {
    /// The left row iterator
    left_rows: Vec<RowRef>,
    /// Current left row index
    left_index: usize,
    /// The right row iterator (cached for reuse)
    right_rows: Vec<RowRef>,
    /// Current right row index
    right_index: usize,
    /// The number of columns in the left source
    left_columns: usize,
    /// The number of columns in the right source
    right_columns: usize,
    /// True if a right match has been seen for the current left row
    right_matched: bool,
    /// Set of right row indices that have been matched (for RIGHT/FULL joins)
    matched_right_indices: HashSet<usize>,
    /// Whether we're in the phase of emitting unmatched right rows
    emitting_unmatched_right: bool,
    /// The join predicate
    predicate: Expression,
    /// The join type
    join_type: JoinType,
    /// Transaction context for expression evaluation
    context: ExecutionContext,
}

impl NestedLoopJoiner {
    /// Creates a new MVCC-aware nested loop joiner.
    pub fn new(
        left: Rows<'_>,
        right: Rows<'_>,
        left_columns: usize,
        right_columns: usize,
        predicate: Expression,
        join_type: JoinType,
        context: ExecutionContext,
    ) -> Result<Self> {
        // Collect all rows from both sources
        let mut left_rows = Vec::new();
        for row in left {
            left_rows.push(row?);
        }

        let mut right_rows = Vec::new();
        for row in right {
            right_rows.push(row?);
        }

        Ok(Self {
            left_rows,
            left_index: 0,
            right_rows,
            right_index: 0,
            left_columns,
            right_columns,
            right_matched: false,
            matched_right_indices: HashSet::new(),
            emitting_unmatched_right: false,
            predicate,
            join_type,
            context,
        })
    }

    /// Get the next joined row, respecting MVCC visibility
    pub fn next_row(&mut self) -> Result<Option<RowRef>> {
        // Phase 1: Process all left rows
        if !self.emitting_unmatched_right {
            while self.left_index < self.left_rows.len() {
                let left = &self.left_rows[self.left_index];

                // If there is a match in the remaining right rows, return it
                while self.right_index < self.right_rows.len() {
                    let right = &self.right_rows[self.right_index];
                    let right_idx = self.right_index;
                    self.right_index += 1;

                    // Create joined row
                    let mut row = left.to_vec();
                    row.extend(right.iter().cloned());
                    let joined_row = Arc::new(row);

                    // Check join predicate
                    match self.evaluate_predicate(&joined_row)? {
                        Value::Bool(true) => {
                            self.right_matched = true;
                            self.matched_right_indices.insert(right_idx);
                            return Ok(Some(joined_row));
                        }
                        Value::Bool(false) | Value::Null => continue,
                        v => {
                            return Err(Error::InvalidValue(format!(
                                "join predicate returned {}, expected boolean",
                                v
                            )));
                        }
                    }
                }

                // Handle LEFT/FULL outer joins when no right match found
                if !self.right_matched && matches!(self.join_type, JoinType::Left | JoinType::Full)
                {
                    self.right_matched = true;
                    let mut row = left.to_vec();
                    row.extend(std::iter::repeat_n(Value::Null, self.right_columns));
                    return Ok(Some(Arc::new(row)));
                }

                // Move to next left row and reset right iteration
                self.left_index += 1;
                self.right_index = 0;
                self.right_matched = false;
            }

            // Done with left rows, now handle unmatched right rows for RIGHT/FULL joins
            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                self.emitting_unmatched_right = true;
                self.right_index = 0;
            }
        }

        // Phase 2: Emit unmatched right rows for RIGHT/FULL joins
        if self.emitting_unmatched_right {
            while self.right_index < self.right_rows.len() {
                let right_idx = self.right_index;
                self.right_index += 1;

                // Skip if this right row was already matched
                if self.matched_right_indices.contains(&right_idx) {
                    continue;
                }

                // Create row with NULL left columns and the unmatched right row
                let right = &self.right_rows[right_idx];
                let mut row = vec![Value::Null; self.left_columns];
                row.extend(right.iter().cloned());
                return Ok(Some(Arc::new(row)));
            }
        }

        Ok(None)
    }

    /// Evaluate the join predicate for MVCC context
    fn evaluate_predicate(&self, row: &RowRef) -> Result<Value> {
        // TODO: Thread parameters through join operations
        expression::evaluate(&self.predicate, Some(row), &self.context, None)
    }
}

impl Iterator for NestedLoopJoiner {
    type Item = Result<RowRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// HashJoiner implements MVCC-aware hash joins.
///
/// This builds a hash table of rows from the right source keyed on the join
/// column value, then iterates over the left source and looks up matching
/// rows in the hash table. Respects MVCC isolation and visibility.
pub struct HashJoiner {
    /// The left row iterator
    left_rows: Vec<RowRef>,
    /// Current left row index
    left_index: usize,
    /// The left column to join on
    left_column: usize,
    /// The right hash map to join on
    right_hash: HashMap<Value, Vec<RowRef>>,
    /// All right rows (for RIGHT/FULL join unmatched emission)
    right_rows: Vec<RowRef>,
    /// Current matches for the current left row
    current_matches: Vec<RowRef>,
    /// Current match index
    match_index: usize,
    /// The number of columns in the left source
    left_columns: usize,
    /// The number of columns in the right source
    right_columns: usize,
    /// Set of right rows that have been matched
    matched_right_rows: HashSet<usize>,
    /// Whether we're emitting unmatched right rows
    emitting_unmatched_right: bool,
    /// Current index for unmatched right emission
    unmatched_right_index: usize,
    /// The join type
    join_type: JoinType,
}

impl HashJoiner {
    /// Creates a new MVCC-aware hash joiner.
    pub fn new(
        left: Rows<'_>,
        left_column: usize,
        right: Rows<'_>,
        right_column: usize,
        left_columns: usize,
        right_columns: usize,
        join_type: JoinType,
    ) -> Result<Self> {
        // Collect left rows
        let mut left_rows = Vec::new();
        for row in left {
            left_rows.push(row?);
        }

        // Build hash table from right source and collect all right rows
        let mut right_hash: HashMap<Value, Vec<RowRef>> = HashMap::new();
        let mut right_rows = Vec::new();

        for row in right {
            let row = row?;
            let key_value = row
                .get(right_column)
                .ok_or_else(|| {
                    Error::InvalidValue(format!("Right column {} not found", right_column))
                })?
                .clone();

            // Store the row with its index for tracking
            right_rows.push(row.clone());

            // Skip undefined values (they never match)
            if !matches!(key_value, Value::Null) {
                right_hash.entry(key_value).or_default().push(row);
            }
        }

        Ok(Self {
            left_rows,
            left_index: 0,
            left_column,
            right_hash,
            right_rows,
            current_matches: Vec::new(),
            match_index: 0,
            left_columns,
            right_columns,
            matched_right_rows: HashSet::new(),
            emitting_unmatched_right: false,
            unmatched_right_index: 0,
            join_type,
        })
    }

    /// Get the next joined row
    pub fn next_row(&mut self) -> Result<Option<RowRef>> {
        // Phase 1: Process left rows and their matches
        if !self.emitting_unmatched_right {
            // First, return any pending matches from the current left row
            if self.match_index < self.current_matches.len() {
                let left = &self.left_rows[self.left_index - 1]; // Use previous left row
                let right = &self.current_matches[self.match_index];
                self.match_index += 1;

                // Track which right row was matched
                for (idx, r) in self.right_rows.iter().enumerate() {
                    if Arc::ptr_eq(r, right) {
                        self.matched_right_rows.insert(idx);
                        break;
                    }
                }

                let mut row = left.to_vec();
                row.extend(right.iter().cloned());
                return Ok(Some(Arc::new(row)));
            }

            // Move to the next left row
            while self.left_index < self.left_rows.len() {
                let left = &self.left_rows[self.left_index];

                // Get the join key from left row
                let join_key = left
                    .get(self.left_column)
                    .ok_or_else(|| {
                        Error::InvalidValue(format!("Left column {} not found", self.left_column))
                    })?
                    .clone();

                // Move to next left row immediately
                self.left_index += 1;

                // Find matching right rows
                if let Some(matches) = self.right_hash.get(&join_key).cloned() {
                    // Found matches - set them up for iteration
                    self.current_matches = matches;
                    self.match_index = 0;

                    // Return first match
                    if !self.current_matches.is_empty() {
                        let right = &self.current_matches[0];
                        self.match_index = 1;

                        // Track which right row was matched
                        for (idx, r) in self.right_rows.iter().enumerate() {
                            if Arc::ptr_eq(r, right) {
                                self.matched_right_rows.insert(idx);
                                break;
                            }
                        }

                        let mut row = left.to_vec();
                        row.extend(right.iter().cloned());
                        return Ok(Some(Arc::new(row)));
                    }
                }

                // Handle LEFT/FULL outer joins when no match found
                if matches!(self.join_type, JoinType::Left | JoinType::Full) {
                    self.current_matches.clear();
                    self.match_index = 0;

                    let mut row = left.to_vec();
                    row.extend(std::iter::repeat_n(Value::Null, self.right_columns));
                    return Ok(Some(Arc::new(row)));
                }

                // No match and not an outer join, continue to next left row
                self.current_matches.clear();
                self.match_index = 0;
            }

            // Done with left rows, now handle unmatched right rows for RIGHT/FULL joins
            if matches!(self.join_type, JoinType::Right | JoinType::Full) {
                self.emitting_unmatched_right = true;
                self.unmatched_right_index = 0;
            }
        }

        // Phase 2: Emit unmatched right rows for RIGHT/FULL joins
        if self.emitting_unmatched_right {
            while self.unmatched_right_index < self.right_rows.len() {
                let idx = self.unmatched_right_index;
                self.unmatched_right_index += 1;

                // Skip if this right row was already matched
                if self.matched_right_rows.contains(&idx) {
                    continue;
                }

                // Create row with NULL left columns and the unmatched right row
                let right = &self.right_rows[idx];
                let mut row = vec![Value::Null; self.left_columns];
                row.extend(right.iter().cloned());
                return Ok(Some(Arc::new(row)));
            }
        }

        Ok(None)
    }
}

impl Iterator for HashJoiner {
    type Item = Result<RowRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Execute a nested loop join with MVCC awareness
pub fn execute_nested_loop_join<'a>(
    left: Rows<'a>,
    right: Rows<'a>,
    left_columns: usize,
    right_columns: usize,
    predicate: Expression,
    join_type: JoinType,
    _storage: &Storage,
) -> Result<Rows<'a>> {
    // For now, create a dummy context - this should come from the transaction context
    use proven_hlc::{HlcTimestamp, NodeId};
    let context = ExecutionContext::new(HlcTimestamp::new(0, 0, NodeId::new(1)), 0);
    let joiner = NestedLoopJoiner::new(
        left,
        right,
        left_columns,
        right_columns,
        predicate,
        join_type,
        context,
    )?;

    Ok(Box::new(joiner))
}

/// Execute a hash join with MVCC awareness
pub fn execute_hash_join<'a>(
    left: Rows<'a>,
    left_column: usize,
    right: Rows<'a>,
    right_column: usize,
    left_columns: usize,
    right_columns: usize,
    join_type: JoinType,
) -> Result<Rows<'a>> {
    let joiner = HashJoiner::new(
        left,
        left_column,
        right,
        right_column,
        left_columns,
        right_columns,
        join_type,
    )?;

    Ok(Box::new(joiner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::context::ExecutionContext;
    use crate::types::expression::Expression;
    use crate::types::value::Value;
    use proven_hlc::{HlcTimestamp, NodeId};

    fn create_test_context() -> ExecutionContext {
        ExecutionContext::new(HlcTimestamp::new(100, 0, NodeId::new(1)), 0)
    }

    #[test]
    fn test_nested_loop_inner_join() {
        let context = create_test_context();

        // Create test data: left table with id, name
        let left_rows: Vec<RowRef> = vec![
            Arc::new(vec![Value::integer(1), Value::string("Alice".to_string())]),
            Arc::new(vec![Value::integer(2), Value::string("Bob".to_string())]),
        ];

        // Right table with id, age
        let right_rows: Vec<RowRef> = vec![
            Arc::new(vec![Value::integer(1), Value::integer(25)]),
            Arc::new(vec![Value::integer(3), Value::integer(30)]),
        ];

        // Join predicate: left.id = right.id (columns 0 and 2)
        let predicate = Expression::Equal(
            Box::new(Expression::Column(0)), // left.id
            Box::new(Expression::Column(2)), // right.id (after join)
        );

        let joiner = NestedLoopJoiner::new(
            Box::new(left_rows.into_iter().map(Ok)),
            Box::new(right_rows.into_iter().map(Ok)),
            2, // left has 2 columns
            2, // right has 2 columns
            predicate,
            JoinType::Inner,
            context,
        )
        .unwrap();

        // Should get one match: (1, Alice, 1, 25)
        let result: Vec<_> = joiner.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            Arc::new(vec![
                Value::integer(1),
                Value::string("Alice".to_string()),
                Value::integer(1),
                Value::integer(25)
            ])
        );
    }

    #[test]
    fn test_hash_join_inner() {
        // Create test data: left table with id, name
        let left_rows: Vec<RowRef> = vec![
            Arc::new(vec![Value::integer(1), Value::string("Alice".to_string())]),
            Arc::new(vec![Value::integer(2), Value::string("Bob".to_string())]),
        ];

        // Right table with id, age
        let right_rows: Vec<RowRef> = vec![
            Arc::new(vec![Value::integer(1), Value::integer(25)]),
            Arc::new(vec![Value::integer(2), Value::integer(30)]),
        ];

        let joiner = HashJoiner::new(
            Box::new(left_rows.into_iter().map(Ok)),
            0, // left join column (id)
            Box::new(right_rows.into_iter().map(Ok)),
            0, // right join column (id)
            2, // left has 2 columns
            2, // right has 2 columns
            JoinType::Inner,
        )
        .unwrap();

        // Should get two matches
        let result: Vec<_> = joiner.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result.len(), 2);

        // First match: (1, Alice, 1, 25)
        assert_eq!(
            result[0],
            Arc::new(vec![
                Value::integer(1),
                Value::string("Alice".to_string()),
                Value::integer(1),
                Value::integer(25)
            ])
        );

        // Second match: (2, Bob, 2, 30)
        assert_eq!(
            result[1],
            Arc::new(vec![
                Value::integer(2),
                Value::string("Bob".to_string()),
                Value::integer(2),
                Value::integer(30)
            ])
        );
    }

    #[test]
    fn test_left_outer_join() {
        // Create test data: left table with id, name
        let left_rows: Vec<RowRef> = vec![
            Arc::new(vec![Value::integer(1), Value::string("Alice".to_string())]),
            Arc::new(vec![Value::integer(2), Value::string("Bob".to_string())]),
        ];

        // Right table with id, age (only one matching row)
        let right_rows: Vec<RowRef> = vec![Arc::new(vec![Value::integer(1), Value::integer(25)])];

        let joiner = HashJoiner::new(
            Box::new(left_rows.into_iter().map(Ok)),
            0, // left join column (id)
            Box::new(right_rows.into_iter().map(Ok)),
            0, // right join column (id)
            2, // left has 2 columns
            2, // right has 2 columns
            JoinType::Left,
        )
        .unwrap();

        let result: Vec<_> = joiner.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(result.len(), 2);

        // First match: (1, Alice, 1, 25)
        assert_eq!(
            result[0],
            Arc::new(vec![
                Value::integer(1),
                Value::string("Alice".to_string()),
                Value::integer(1),
                Value::integer(25)
            ])
        );

        // Second row with NULLs: (2, Bob, NULL, NULL)
        assert_eq!(
            result[1],
            Arc::new(vec![
                Value::integer(2),
                Value::string("Bob".to_string()),
                Value::Null,
                Value::Null
            ])
        );
    }
}
