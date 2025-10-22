//! Interval type for time durations

use serde::{Deserialize, Serialize};

/// Interval type for time durations
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}
