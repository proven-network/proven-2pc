//! KV stream processor using the generic stream processing framework
//!
//! This is a thin wrapper around the generic StreamProcessor that uses
//! the KvTransactionEngine for KV-specific operations.

use proven_engine::{Message, MockClient};
use proven_stream::StreamProcessor;
use std::sync::Arc;

use super::engine::KvTransactionEngine;

/// KV stream processor with transaction isolation
pub struct KvStreamProcessor {
    /// The underlying generic stream processor
    processor: StreamProcessor<KvTransactionEngine>,
}

impl KvStreamProcessor {
    /// Create a new KV stream processor
    pub fn new(client: Arc<MockClient>, stream_name: String) -> Self {
        let engine = KvTransactionEngine::new();
        let processor = StreamProcessor::new(engine, client, stream_name);
        Self { processor }
    }

    /// Process a message from the stream
    pub async fn process_message(&mut self, message: Message) -> Result<(), String> {
        self.processor
            .process_message(message)
            .await
            .map_err(|e| e.to_string())
    }

    /// Create a processor for testing (creates a mock engine internally)
    #[cfg(test)]
    pub fn new_for_testing() -> (Self, Arc<proven_engine::MockEngine>) {
        use proven_engine::MockEngine;
        let engine = Arc::new(MockEngine::new());
        let client = Arc::new(MockClient::new(
            "test-kv-processor".to_string(),
            engine.clone(),
        ));
        (Self::new(client, "kv-stream".to_string()), engine)
    }
}

// Re-export for compatibility
pub use crate::storage::lock::{LockManager, LockMode};
pub use crate::storage::mvcc::MvccStorage;