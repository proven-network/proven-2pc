//! Caching planner for query plans
//!
//! This module provides a caching wrapper around the query planner that
//! maintains an LRU cache of query plans to avoid redundant planning.

use super::planner::Planner;
use crate::error::Result;
use crate::semantic::statement::AnalyzedStatement;
use crate::types::index::IndexMetadata;
use crate::types::plan::Plan;
use crate::types::schema::Table;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

/// Default capacity for the plan cache
const DEFAULT_CACHE_CAPACITY: usize = 500;

/// A caching wrapper around the query planner
pub struct CachingPlanner {
    /// The underlying planner
    planner: Planner,

    /// LRU cache for query plans (wrapped in Mutex for interior mutability)
    /// Key is the Arc pointer address of the analyzed statement
    cache: Mutex<LruCache<usize, Arc<Plan>>>,
}

impl CachingPlanner {
    /// Create a new caching planner with default capacity
    pub fn new(schemas: HashMap<String, Table>, indexes: HashMap<String, IndexMetadata>) -> Self {
        Self::with_capacity(schemas, indexes, DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new caching planner with specified capacity
    pub fn with_capacity(
        schemas: HashMap<String, Table>,
        indexes: HashMap<String, IndexMetadata>,
        capacity: usize,
    ) -> Self {
        Self {
            planner: Planner::new(schemas, indexes),
            cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()),
            )),
        }
    }

    /// Plan a statement with caching
    /// Takes an Arc<AnalyzedStatement> from the CachingSemanticAnalyzer
    pub fn plan(&self, analyzed: Arc<AnalyzedStatement>) -> Result<Arc<Plan>> {
        // Use the Arc pointer as cache key
        let cache_key = Arc::as_ptr(&analyzed) as usize;

        // Check cache
        {
            let mut cache = self.cache.lock().unwrap();
            if let Some(plan) = cache.get(&cache_key) {
                return Ok(plan.clone());
            }
        }

        // Plan the statement
        // We need to clone the inner analyzed statement for the planner
        let plan = self.planner.plan((*analyzed).clone())?;
        let arc_plan = Arc::new(plan);

        // Cache the result
        {
            let mut cache = self.cache.lock().unwrap();
            cache.put(cache_key, arc_plan.clone());
        }

        Ok(arc_plan)
    }

    /// Update schemas and invalidate cache
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.planner.update_schemas(schemas);
        self.cache.lock().unwrap().clear();
    }

    /// Update indexes and invalidate cache
    pub fn update_indexes(&mut self, indexes: HashMap<String, IndexMetadata>) {
        self.planner.update_indexes(indexes);
        self.cache.lock().unwrap().clear();
    }
}
