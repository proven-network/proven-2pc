//! Simplified SQL stream processor that uses the generic stream processor
//!
//! This module provides a thin wrapper around the generic StreamProcessor,
//! maintaining backward compatibility while leveraging the shared implementation.

use super::engine::SqlTransactionEngine;
use proven_engine::{Message, MockClient};
use proven_stream::StreamProcessor;
use std::sync::Arc;

/// SQL stream processor wrapper
pub struct SqlStreamProcessor {
    inner: StreamProcessor<SqlTransactionEngine>,
}

impl SqlStreamProcessor {
    /// Create a new processor with empty storage
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        let engine = SqlTransactionEngine::new();
        let inner = StreamProcessor::new(engine, client, stream_name);
        Self { inner }
    }

    /// Process a message from the stream
    pub async fn process_message(&mut self, message: Message) -> crate::error::Result<()> {
        self.inner
            .process_message(message)
            .await
            .map_err(|e| crate::error::Error::InvalidValue(format!("{:?}", e)))
    }

    // TODO: Add these methods once we have accessors on StreamProcessor
    // For now, these testing methods are not available through the wrapper

    // /// Get a reference to the storage for testing
    // #[cfg(test)]
    // pub fn storage(&self) -> &crate::storage::mvcc::MvccStorage {
    //     &self.inner.engine().storage
    // }

    // /// Get a reference to the lock manager for testing
    // #[cfg(test)]
    // pub fn lock_manager(&self) -> &crate::storage::lock::LockManager {
    //     &self.inner.engine().lock_manager
    // }

    /// Run garbage collection
    pub fn garbage_collect(&mut self) -> usize {
        // TODO: Implement once we have engine accessor
        0
    }

    /// Create a processor for testing (creates a mock engine internally)
    #[cfg(test)]
    pub fn new_for_testing() -> (Self, Arc<proven_engine::MockEngine>) {
        use proven_engine::MockEngine;
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-sql-processor".to_string(),
            engine.clone(),
        ));
        (Self::new(client, "sql-stream".to_string()), engine)
    }
}
