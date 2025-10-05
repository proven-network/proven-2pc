//! Index management and operations

use serde::{Deserialize, Serialize};

/// Index type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
    Unique,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
}
