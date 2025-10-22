//! Point type for geometric data

use serde::{Deserialize, Serialize};

/// Point type for geometric data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }
}

impl Eq for Point {}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.x.partial_cmp(&other.x) {
            Some(std::cmp::Ordering::Equal) | None => self
                .y
                .partial_cmp(&other.y)
                .unwrap_or(std::cmp::Ordering::Equal),
            Some(ord) => ord,
        }
    }
}

impl std::hash::Hash for Point {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.x.to_bits().hash(state);
        self.y.to_bits().hash(state);
    }
}
