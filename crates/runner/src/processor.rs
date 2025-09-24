//! Processor lifecycle management

use proven_engine::MockClient;
use proven_snapshot::SnapshotStore;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Handle to a running processor
#[derive(Clone)]
pub struct ProcessorHandle {
    pub(crate) guaranteed_until: Instant,
    pub(crate) shutdown_tx: Arc<parking_lot::Mutex<Option<oneshot::Sender<()>>>>,
}

impl ProcessorHandle {
    /// Get guaranteed until time in milliseconds
    pub fn guaranteed_until_ms(&self) -> u64 {
        let remaining = self
            .guaranteed_until
            .saturating_duration_since(Instant::now());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now_ms + remaining.as_millis() as u64
    }

    /// Check if processor is shutting down
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_tx.lock().is_none()
    }

    /// Shutdown the processor
    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(());
        }
        // Don't wait for task to complete, just signal shutdown
    }
}

/// Start a processor for a stream
pub async fn start_processor(
    stream: String,
    duration: Duration,
    client: Arc<MockClient>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> Result<ProcessorHandle, crate::RunnerError> {
    // Determine processor type from stream name
    let processor_type = determine_processor_type(&stream);

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Create processor and start it (waits for replay to complete)
    match processor_type {
        ProcessorType::Kv => {
            let processor =
                create_kv_processor(stream.clone(), client.clone(), snapshot_store.clone()).await?;
            processor
                .start_with_replay(shutdown_rx)
                .await
                .map_err(|e| {
                    crate::RunnerError::Other(format!("Failed to start processor: {}", e))
                })?;
        }
        ProcessorType::Sql => {
            let processor =
                create_sql_processor(stream.clone(), client.clone(), snapshot_store.clone())
                    .await?;
            processor
                .start_with_replay(shutdown_rx)
                .await
                .map_err(|e| {
                    crate::RunnerError::Other(format!("Failed to start processor: {}", e))
                })?;
        }
        ProcessorType::Queue => {
            let processor =
                create_queue_processor(stream.clone(), client.clone(), snapshot_store.clone())
                    .await?;
            processor
                .start_with_replay(shutdown_rx)
                .await
                .map_err(|e| {
                    crate::RunnerError::Other(format!("Failed to start processor: {}", e))
                })?;
        }
        ProcessorType::Resource => {
            let processor =
                create_resource_processor(stream.clone(), client.clone(), snapshot_store.clone())
                    .await?;
            processor
                .start_with_replay(shutdown_rx)
                .await
                .map_err(|e| {
                    crate::RunnerError::Other(format!("Failed to start processor: {}", e))
                })?;
        }
    }

    // Processor has completed replay and is ready

    Ok(ProcessorHandle {
        guaranteed_until: Instant::now() + duration,
        shutdown_tx: Arc::new(parking_lot::Mutex::new(Some(shutdown_tx))),
    })
}

/// Processor types
enum ProcessorType {
    Kv,
    Sql,
    Queue,
    Resource,
}

/// Determine processor type from stream name
fn determine_processor_type(stream: &str) -> ProcessorType {
    // Simple heuristic based on stream name
    if stream.contains("kv") {
        ProcessorType::Kv
    } else if stream.contains("sql") {
        ProcessorType::Sql
    } else if stream.contains("queue") {
        ProcessorType::Queue
    } else if stream.contains("resource") {
        ProcessorType::Resource
    } else {
        // Default to KV
        ProcessorType::Kv
    }
}

/// Create a KV processor
async fn create_kv_processor(
    stream: String,
    client: Arc<MockClient>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> Result<proven_stream::StreamProcessor<proven_kv::KvTransactionEngine>, crate::RunnerError> {
    let engine = proven_kv::KvTransactionEngine::new();
    Ok(proven_stream::StreamProcessor::new(
        engine,
        client,
        stream,
        snapshot_store,
    ))
}

/// Create a SQL processor
async fn create_sql_processor(
    stream: String,
    client: Arc<MockClient>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> Result<proven_stream::StreamProcessor<proven_sql::SqlTransactionEngine>, crate::RunnerError> {
    let engine = proven_sql::SqlTransactionEngine::new();
    Ok(proven_stream::StreamProcessor::new(
        engine,
        client,
        stream,
        snapshot_store,
    ))
}

/// Create a Queue processor
async fn create_queue_processor(
    stream: String,
    client: Arc<MockClient>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> Result<proven_stream::StreamProcessor<proven_queue::QueueTransactionEngine>, crate::RunnerError>
{
    let engine = proven_queue::QueueTransactionEngine::new();
    Ok(proven_stream::StreamProcessor::new(
        engine,
        client,
        stream,
        snapshot_store,
    ))
}

/// Create a Resource processor
async fn create_resource_processor(
    stream: String,
    client: Arc<MockClient>,
    snapshot_store: Arc<dyn SnapshotStore>,
) -> Result<
    proven_stream::StreamProcessor<proven_resource::ResourceTransactionEngine>,
    crate::RunnerError,
> {
    let engine = proven_resource::ResourceTransactionEngine::new();
    Ok(proven_stream::StreamProcessor::new(
        engine,
        client,
        stream,
        snapshot_store,
    ))
}

#[cfg(test)]
impl ProcessorHandle {
    /// Test helper to create a ProcessorHandle for testing
    pub fn new_for_test(guaranteed_until: Instant) -> Self {
        let (shutdown_tx, _) = oneshot::channel();

        Self {
            guaranteed_until,
            shutdown_tx: Arc::new(parking_lot::Mutex::new(Some(shutdown_tx))),
        }
    }
}
