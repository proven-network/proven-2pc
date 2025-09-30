//! Core types used throughout the storage module

use crate::types::value::Value;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Use primitive types directly instead of wrappers
pub type RowId = u64;

pub type FjallIterator<'a> =
    Box<dyn Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), fjall::Error>> + 'a>;

/// A row of data in storage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub id: u64, // Use u64 directly
    pub values: Vec<Value>,
    pub deleted: bool,
}

impl Row {
    pub fn new(id: u64, values: Vec<Value>) -> Self {
        Self {
            id,
            values,
            deleted: false,
        }
    }
}

/// Write operation for transaction tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteOp {
    // Data operations only - index operations are tracked separately in IndexVersionStore
    Insert {
        table: String,
        row_id: u64,
        row: Arc<Row>,
    },
    Update {
        table: String,
        row_id: u64,
        old_row: Arc<Row>,
        new_row: Arc<Row>,
    },
    Delete {
        table: String,
        row_id: u64,
        row: Arc<Row>,
    },
}

impl WriteOp {
    /// Get the table name for data operations
    pub fn table_name(&self) -> Option<&str> {
        match self {
            WriteOp::Insert { table, .. }
            | WriteOp::Update { table, .. }
            | WriteOp::Delete { table, .. } => Some(table),
        }
    }

    /// Get the row_id affected by this operation
    pub fn row_id(&self) -> u64 {
        match self {
            WriteOp::Insert { row_id, .. }
            | WriteOp::Update { row_id, .. }
            | WriteOp::Delete { row_id, .. } => *row_id,
        }
    }
}
