//! Prepared statement cache for SQL queries
//!
//! This module implements an LRU cache for semantically analyzed SQL statements,
//! storing validated and type-checked queries to avoid re-analysis.

use crate::semantic::AnalyzedStatement;
use std::collections::{HashMap, VecDeque};

/// Maximum number of prepared statements to cache
const DEFAULT_CACHE_SIZE: usize = 100;

/// Location where a parameter appears in the statement
#[derive(Debug, Clone)]
pub enum ParameterLocation {
    /// Parameter in an expression at this path
    Expression(Vec<usize>),
    /// Parameter in a VALUES clause
    Values { row: usize, col: usize },
}

/// A prepared statement with semantic analysis results
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The analyzed statement with type information
    pub analyzed: AnalyzedStatement,
    /// Number of parameters expected
    pub param_count: usize,
    /// Locations of parameters in the statement
    pub param_locations: Vec<ParameterLocation>,
}

/// LRU cache for prepared statements
pub struct PreparedCache {
    /// Map from SQL string to cache entry
    cache: HashMap<String, PreparedStatement>,
    /// LRU queue tracking access order (most recent at back)
    lru: VecDeque<String>,
    /// Maximum cache size
    max_size: usize,
}

impl PreparedCache {
    /// Create a new prepared statement cache
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CACHE_SIZE)
    }

    /// Create a cache with specified capacity
    pub fn with_capacity(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size.min(DEFAULT_CACHE_SIZE)),
            lru: VecDeque::with_capacity(max_size.min(DEFAULT_CACHE_SIZE)),
            max_size,
        }
    }

    /// Get a prepared statement from the cache
    pub fn get(&mut self, sql: &str) -> Option<&PreparedStatement> {
        if self.cache.contains_key(sql) {
            // Move to end of LRU queue
            self.update_lru(sql);
            self.cache.get(sql)
        } else {
            None
        }
    }

    /// Insert a prepared statement into the cache
    pub fn insert(&mut self, sql: String, prepared: PreparedStatement) {
        // Check if we need to evict
        if self.cache.len() >= self.max_size && !self.cache.contains_key(&sql) {
            // Evict least recently used
            if let Some(evicted) = self.lru.pop_front() {
                self.cache.remove(&evicted);
            }
        }

        // Insert or update
        if self.cache.contains_key(&sql) {
            self.update_lru(&sql);
        } else {
            self.lru.push_back(sql.clone());
        }
        self.cache.insert(sql, prepared);
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru.clear();
    }

    /// Remove a specific entry from the cache
    pub fn remove(&mut self, sql: &str) {
        if self.cache.remove(sql).is_some()
            && let Some(pos) = self.lru.iter().position(|x| x == sql)
        {
            self.lru.remove(pos);
        }
    }

    /// Get the current cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Update LRU order for an accessed key
    fn update_lru(&mut self, sql: &str) {
        // Remove from current position
        if let Some(pos) = self.lru.iter().position(|s| s == sql) {
            self.lru.remove(pos);
        }
        // Add to end (most recently used)
        self.lru.push_back(sql.to_string());
    }

    /// Clear cache entries that reference a specific table (for schema changes)
    pub fn invalidate_table(&mut self, table_name: &str) {
        let to_remove: Vec<String> = self
            .cache
            .iter()
            .filter(|(_, stmt)| {
                stmt.analyzed
                    .metadata
                    .table_access
                    .iter()
                    .any(|t| t.table == *table_name)
            })
            .map(|(sql, _)| sql.clone())
            .collect();

        for sql in to_remove {
            self.remove(&sql);
        }
    }
}

impl Default for PreparedCache {
    fn default() -> Self {
        Self::new()
    }
}
