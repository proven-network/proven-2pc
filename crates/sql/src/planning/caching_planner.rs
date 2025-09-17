//! Caching planner for query plans
//!
//! This module provides a caching wrapper around the query planner that
//! maintains an LRU cache of query plans to avoid redundant planning.

use super::plan::Plan;
use super::planner::Planner;
use crate::error::Result;
use crate::semantic::statement::AnalyzedStatement;
use crate::storage::mvcc::IndexMetadata;
use crate::types::schema::Table;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default capacity for the plan cache
const DEFAULT_CACHE_CAPACITY: usize = 500;

/// A caching wrapper around the query planner
pub struct CachingPlanner {
    /// The underlying planner
    planner: Planner,

    /// LRU cache for query plans
    /// Key is the Arc pointer address of the analyzed statement
    cache: LruCache<usize, Arc<Plan>>,
}

impl CachingPlanner {
    /// Create a new caching planner with default capacity
    pub fn new(
        schemas: HashMap<String, Table>,
        indexes: HashMap<String, Vec<IndexMetadata>>,
    ) -> Self {
        Self::with_capacity(schemas, indexes, DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new caching planner with specified capacity
    pub fn with_capacity(
        schemas: HashMap<String, Table>,
        indexes: HashMap<String, Vec<IndexMetadata>>,
        capacity: usize,
    ) -> Self {
        Self {
            planner: Planner::new(schemas, indexes),
            cache: LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()),
            ),
        }
    }

    /// Plan a statement with caching
    /// Takes an Arc<AnalyzedStatement> from the CachingSemanticAnalyzer
    pub fn plan(&mut self, analyzed: Arc<AnalyzedStatement>) -> Result<Arc<Plan>> {
        // Use the Arc pointer as cache key
        let cache_key = Arc::as_ptr(&analyzed) as usize;

        // Check cache
        if let Some(plan) = self.cache.get(&cache_key) {
            return Ok(plan.clone());
        }

        // Plan the statement
        // We need to clone the inner analyzed statement for the planner
        let plan = self.planner.plan((*analyzed).clone())?;
        let arc_plan = Arc::new(plan);

        // Cache the result
        self.cache.put(cache_key, arc_plan.clone());

        Ok(arc_plan)
    }

    /// Update schemas and invalidate cache
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.planner.update_schemas(schemas);
        self.cache.clear();
    }

    /// Update indexes and invalidate cache
    pub fn update_indexes(&mut self, indexes: HashMap<String, Vec<IndexMetadata>>) {
        self.planner.update_indexes(indexes);
        self.cache.clear();
    }

    /// Extract predicates (delegates to underlying planner)
    pub fn extract_predicates(&self, plan: &Plan) -> crate::planning::predicate::QueryPredicates {
        self.planner.extract_predicates(plan)
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            capacity: self.cache.cap().get(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries in cache
    pub size: usize,
    /// Maximum capacity
    pub capacity: usize,
}
