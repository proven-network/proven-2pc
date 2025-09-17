//! Caching semantic analyzer for analyzed statements
//!
//! This module provides a caching wrapper around the semantic analyzer that
//! maintains an LRU cache of analyzed statements to avoid redundant analysis.

use super::analyzer::SemanticAnalyzer;
use super::statement::AnalyzedStatement;
use crate::error::Result;
use crate::parsing::ast::Statement;
use crate::types::schema::Table;
use lru::LruCache;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default capacity for the semantic cache
const DEFAULT_CACHE_CAPACITY: usize = 500;

/// A caching wrapper around the semantic analyzer
pub struct CachingSemanticAnalyzer {
    /// The underlying semantic analyzer
    analyzer: SemanticAnalyzer,

    /// LRU cache for analyzed statements
    /// Key is the Arc pointer address of the parsed statement
    cache: LruCache<usize, Arc<AnalyzedStatement>>,
}

impl CachingSemanticAnalyzer {
    /// Create a new caching semantic analyzer with default capacity
    pub fn new(schemas: HashMap<String, Table>) -> Self {
        Self::with_capacity(schemas, DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new caching semantic analyzer with specified capacity
    pub fn with_capacity(schemas: HashMap<String, Table>, capacity: usize) -> Self {
        Self {
            analyzer: SemanticAnalyzer::new(schemas),
            cache: LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()),
            ),
        }
    }

    /// Analyze a statement with caching
    /// Takes an Arc<Statement> from the CachingParser
    pub fn analyze(&mut self, statement: Arc<Statement>) -> Result<Arc<AnalyzedStatement>> {
        // Use the Arc pointer as cache key
        let cache_key = Arc::as_ptr(&statement) as usize;

        // Check cache
        if let Some(analyzed) = self.cache.get(&cache_key) {
            return Ok(analyzed.clone());
        }

        // Analyze the statement
        // We need to clone the inner statement for the analyzer
        let analyzed = self.analyzer.analyze((*statement).clone())?;
        let arc_analyzed = Arc::new(analyzed);

        // Cache the result
        self.cache.put(cache_key, arc_analyzed.clone());

        Ok(arc_analyzed)
    }

    /// Update schemas and invalidate cache
    pub fn update_schemas(&mut self, schemas: HashMap<String, Table>) {
        self.analyzer.update_schemas(schemas);
        self.cache.clear();
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
