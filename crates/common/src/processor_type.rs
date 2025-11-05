//! Processor type definitions

use serde::{Deserialize, Serialize};

/// Type of processor to run
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessorType {
    /// Key-value storage processor
    Kv,
    /// SQL database processor
    Sql,
    /// Queue processor
    Queue,
    /// Resource (balance/inventory) processor
    Resource,
}
