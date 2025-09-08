//! Statistics cache management for query optimization
//!
//! Provides a clean abstraction for managing database statistics,
//! updating them periodically, and providing them to the query planner.

use crate::storage::MvccStorage;
use crate::types::statistics::DatabaseStatistics;
use std::time::{Duration, Instant};

/// Statistics cache for query optimization
pub struct StatisticsCache {
    /// Cached statistics
    stats: Option<DatabaseStatistics>,

    /// Last time statistics were updated
    last_update: Option<Instant>,

    /// Number of commits since last update
    commits_since_update: usize,

    /// Update interval (in number of commits)
    update_interval: usize,

    /// Maximum age before forcing update (regardless of commit count)
    max_age: Duration,
}

impl StatisticsCache {
    /// Create a new statistics cache
    pub fn new(update_interval: usize) -> Self {
        Self {
            stats: None,
            last_update: None,
            commits_since_update: 0,
            update_interval,
            max_age: Duration::from_secs(300), // 5 minutes default
        }
    }

    /// Get current statistics (if available)
    pub fn get(&self) -> Option<&DatabaseStatistics> {
        self.stats.as_ref()
    }

    /// Check if statistics need updating
    pub fn needs_update(&self) -> bool {
        // Force update if we've never calculated stats
        if self.stats.is_none() {
            return true;
        }

        // Check commit count threshold
        if self.commits_since_update >= self.update_interval {
            return true;
        }

        // Check age threshold
        if let Some(last_update) = self.last_update
            && last_update.elapsed() > self.max_age
        {
            return true;
        }

        false
    }

    /// Record a commit (increments counter)
    pub fn record_commit(&mut self) {
        self.commits_since_update += 1;
    }

    /// Update statistics from storage
    pub fn update(&mut self, storage: &MvccStorage) {
        self.stats = Some(storage.calculate_statistics());
        self.last_update = Some(Instant::now());
        self.commits_since_update = 0;
    }

    /// Force update on next check (e.g., after DDL)
    pub fn invalidate(&mut self) {
        // Set commits to trigger update on next check
        self.commits_since_update = self.update_interval;
    }

    /// Update if needed and no transactions are active
    pub fn maybe_update(&mut self, storage: &MvccStorage, active_transactions: usize) {
        // Only update if no transactions are waiting (to avoid blocking)
        if self.needs_update() && active_transactions == 0 {
            self.update(storage);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_cache_lifecycle() {
        let storage = MvccStorage::new();
        let mut cache = StatisticsCache::new(10);

        // Initially needs update
        assert!(cache.needs_update());
        assert!(cache.get().is_none());

        // After update, has stats
        cache.update(&storage);
        assert!(!cache.needs_update());
        assert!(cache.get().is_some());

        // After enough commits, needs update
        for _ in 0..10 {
            cache.record_commit();
        }
        assert!(cache.needs_update());

        // After invalidation, needs update
        cache.update(&storage);
        assert!(!cache.needs_update());
        cache.invalidate();
        assert!(cache.needs_update());
    }
}
