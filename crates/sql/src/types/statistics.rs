//! Query optimization statistics
//!
//! Statistics are collected from storage and used by the planner
//! to make better decisions about index usage and join ordering.
//! They are separate from schemas to allow independent updates.

use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Statistics for a table index
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct IndexStatistics {
    /// Name of the index
    pub name: String,
    /// Columns in the index
    pub columns: Vec<String>,
    /// Number of distinct values per column
    pub cardinality: Vec<usize>,
    /// Total number of entries
    pub total_entries: usize,
    /// Selectivity (1/cardinality) for quick access
    pub selectivity: Vec<f64>,
}

impl IndexStatistics {
    /// Create new index statistics
    pub fn new(name: String, columns: Vec<String>) -> Self {
        let col_count = columns.len();
        Self {
            name,
            columns,
            cardinality: vec![0; col_count],
            total_entries: 0,
            selectivity: vec![1.0; col_count],
        }
    }

    /// Calculate selectivity from cardinality
    pub fn calculate_selectivity(&mut self) {
        self.selectivity = self
            .cardinality
            .iter()
            .map(|&c| if c > 0 { 1.0 / c as f64 } else { 1.0 })
            .collect();
    }

    /// Get average selectivity across all columns
    pub fn avg_selectivity(&self) -> f64 {
        if self.selectivity.is_empty() {
            1.0
        } else {
            self.selectivity.iter().sum::<f64>() / self.selectivity.len() as f64
        }
    }
}

/// Column-level statistics
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ColumnStatistics {
    /// Column name
    pub name: String,
    /// Number of distinct values
    pub distinct_count: usize,
    /// Number of null values
    pub null_count: usize,
    /// Minimum value (if applicable)
    pub min_value: Option<Value>,
    /// Maximum value (if applicable)
    pub max_value: Option<Value>,
    /// Most common values and their frequencies (top-k)
    pub most_common_values: Vec<(Value, usize)>,
    /// Histogram buckets for value distribution (equal-width for simplicity)
    pub histogram: Option<Histogram>,
}

/// Histogram for value distribution
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct Histogram {
    /// Number of buckets
    pub num_buckets: usize,
    /// Bucket boundaries (for numeric types)
    pub boundaries: Vec<Value>,
    /// Count of values in each bucket
    pub frequencies: Vec<usize>,
}

impl Histogram {
    /// Estimate selectivity for a range query
    pub fn estimate_range_selectivity(&self, start: &Value, end: &Value) -> f64 {
        // Simple estimation based on bucket overlap
        let total: usize = self.frequencies.iter().sum();
        if total == 0 {
            return 1.0;
        }

        let mut selected = 0;
        for (i, freq) in self.frequencies.iter().enumerate() {
            if i < self.boundaries.len() - 1 {
                let bucket_start = &self.boundaries[i];
                let bucket_end = &self.boundaries[i + 1];

                // Check if bucket overlaps with query range
                if bucket_end >= start && bucket_start <= end {
                    // Simple: assume uniform distribution within bucket
                    selected += freq;
                }
            }
        }

        selected as f64 / total as f64
    }
}

/// Statistics for a table
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TableStatistics {
    /// Estimated row count
    pub row_count: usize,
    /// Index statistics by index name
    pub indexes: HashMap<String, IndexStatistics>,
    /// Column statistics by column name
    pub columns: HashMap<String, ColumnStatistics>,
    /// Correlation between columns (for join selectivity)
    pub correlations: HashMap<(String, String), f64>,
}

impl TableStatistics {
    /// Create new table statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or update index statistics
    pub fn update_index(&mut self, name: String, stats: IndexStatistics) {
        self.indexes.insert(name, stats);
    }

    /// Add or update column statistics
    pub fn update_column(&mut self, name: String, stats: ColumnStatistics) {
        self.columns.insert(name, stats);
    }

    /// Get column statistics
    pub fn get_column(&self, name: &str) -> Option<&ColumnStatistics> {
        self.columns.get(name)
    }

    /// Estimate selectivity for an equality predicate
    pub fn estimate_equality_selectivity(&self, column: &str, value: &Value) -> f64 {
        if let Some(col_stats) = self.columns.get(column) {
            // Check if it's a most common value
            for (common_val, freq) in &col_stats.most_common_values {
                if common_val == value {
                    return *freq as f64 / self.row_count.max(1) as f64;
                }
            }

            // Otherwise use 1/distinct_count
            if col_stats.distinct_count > 0 {
                return 1.0 / col_stats.distinct_count as f64;
            }
        }

        // Default selectivity
        0.1
    }

    /// Estimate selectivity for a range predicate
    pub fn estimate_range_selectivity(&self, column: &str, start: &Value, end: &Value) -> f64 {
        if let Some(col_stats) = self.columns.get(column) {
            if let Some(ref histogram) = col_stats.histogram {
                return histogram.estimate_range_selectivity(start, end);
            }

            // Fallback: use min/max if available
            if let (Some(min), Some(max)) = (&col_stats.min_value, &col_stats.max_value) {
                // Simple linear estimation
                if min < end && max > start {
                    return 0.3; // Conservative estimate
                }
                return 0.0;
            }
        }

        // Default range selectivity
        0.25
    }

    /// Estimate join selectivity between two columns
    pub fn estimate_join_selectivity(
        &self,
        col1: &str,
        col2: &str,
        other_table: &TableStatistics,
    ) -> f64 {
        // Check for correlation information
        if let Some(&correlation) = self.correlations.get(&(col1.to_string(), col2.to_string())) {
            // Use correlation to adjust selectivity
            return (1.0 - correlation.abs()) * 0.1 + 0.01;
        }

        // Use distinct counts
        if let (Some(stats1), Some(stats2)) = (self.get_column(col1), other_table.get_column(col2))
        {
            let max_distinct = stats1.distinct_count.max(stats2.distinct_count);
            if max_distinct > 0 {
                return 1.0 / max_distinct as f64;
            }
        }

        // Default join selectivity
        0.1
    }
}

/// Complete database statistics
#[derive(Clone, Debug, Default)]
pub struct DatabaseStatistics {
    /// Statistics per table
    pub tables: HashMap<String, TableStatistics>,
}

impl DatabaseStatistics {
    /// Create new database statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get statistics for a specific table
    pub fn get_table(&self, table_name: &str) -> Option<&TableStatistics> {
        self.tables.get(table_name)
    }

    /// Update statistics for a table
    pub fn update_table(&mut self, table_name: String, stats: TableStatistics) {
        self.tables.insert(table_name, stats);
    }

    /// Merge statistics from storage
    pub fn merge_from(&mut self, other: DatabaseStatistics) {
        for (table_name, table_stats) in other.tables {
            self.tables.insert(table_name, table_stats);
        }
    }
}
