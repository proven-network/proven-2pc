//! Caching semantic analyzer for analyzed statements
//!
//! This module provides a caching wrapper around the semantic analyzer that
//! maintains an LRU cache of analyzed statements to avoid redundant analysis.
//! The cache key includes both the statement and parameter types to enable
//! single-pass validation with complete type information.

use super::analyzer::SemanticAnalyzer;
use super::statement::AnalyzedStatement;
use crate::error::Result;
use crate::parsing::ast::Statement;
use crate::types::data_type::DataType;
use crate::types::schema::Table;
use lru::LruCache;
use std::collections::HashMap;
use std::hash::Hash;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default capacity for the semantic cache
const DEFAULT_CACHE_CAPACITY: usize = 500;

/// Cache key that combines statement and parameter types
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CacheKey {
    /// Pointer address of the Arc<Statement> for fast comparison
    statement_ptr: usize,
    /// Parameter types in order
    param_types: Vec<DataType>,
}

/// A caching wrapper around the semantic analyzer
pub struct CachingSemanticAnalyzer {
    /// The underlying semantic analyzer
    analyzer: SemanticAnalyzer,

    /// LRU cache for analyzed statements
    /// Key includes both statement pointer and parameter types
    cache: LruCache<CacheKey, Arc<AnalyzedStatement>>,
}

impl CachingSemanticAnalyzer {
    /// Create a new caching semantic analyzer with default capacity
    pub fn new(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
    ) -> Self {
        Self::with_capacity(schemas, index_metadata, DEFAULT_CACHE_CAPACITY)
    }

    /// Create a new caching semantic analyzer with specified capacity
    pub fn with_capacity(
        schemas: HashMap<String, Table>,
        index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
        capacity: usize,
    ) -> Self {
        Self {
            analyzer: SemanticAnalyzer::new(schemas, index_metadata),
            cache: LruCache::new(
                NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(100).unwrap()),
            ),
        }
    }

    /// Analyze a statement with caching, using parameter types for complete validation
    /// Takes an Arc<Statement> from the CachingParser and parameter types
    pub fn analyze(
        &mut self,
        statement: Arc<Statement>,
        param_types: Vec<DataType>,
    ) -> Result<Arc<AnalyzedStatement>> {
        // Create cache key combining statement pointer and parameter types
        let cache_key = CacheKey {
            statement_ptr: Arc::as_ptr(&statement) as usize,
            param_types: param_types.clone(),
        };

        // Check cache
        if let Some(analyzed) = self.cache.get(&cache_key) {
            return Ok(analyzed.clone());
        }

        // Analyze the statement with parameter types
        // We need to clone the inner statement for the analyzer
        let analyzed = self.analyzer.analyze((*statement).clone(), param_types)?;
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

    /// Update index metadata and invalidate cache
    pub fn update_index_metadata(
        &mut self,
        index_metadata: HashMap<String, crate::types::index::IndexMetadata>,
    ) {
        self.analyzer.update_index_metadata(index_metadata);
        self.cache.clear();
    }
}
