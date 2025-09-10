//! Processor lifecycle management

use proven_engine::MockClient;
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
) -> Result<ProcessorHandle, crate::RunnerError> {
    // Determine processor type from stream name
    let processor_type = determine_processor_type(&stream);

    // Start processor task
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let stream_clone = stream.clone();
    let stream_clone2 = stream.clone();
    let client_clone = client.clone();

    tokio::spawn(async move {
        // Create and run processor based on type
        let result = match processor_type {
            ProcessorType::Kv => {
                match create_kv_processor(stream_clone2.clone(), client_clone.clone()).await {
                    Ok(processor) => run_typed_processor(processor, shutdown_rx).await,
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                }
            }
            ProcessorType::Sql => {
                match create_sql_processor(stream_clone2.clone(), client_clone.clone()).await {
                    Ok(processor) => run_typed_processor(processor, shutdown_rx).await,
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                }
            }
            ProcessorType::Queue => {
                match create_queue_processor(stream_clone2.clone(), client_clone.clone()).await {
                    Ok(processor) => run_typed_processor(processor, shutdown_rx).await,
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                }
            }
            ProcessorType::Resource => {
                match create_resource_processor(stream_clone2.clone(), client_clone.clone()).await {
                    Ok(processor) => run_typed_processor(processor, shutdown_rx).await,
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                }
            }
        };

        if let Err(ref e) = result {
            tracing::error!("Processor for {} failed: {}", stream_clone, e);
        }
        result
    });

    // Wait a bit for the processor to reach Live phase
    // In production, this would use a proper ready signal
    // For now, a small delay works since phase transitions are fast
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(ProcessorHandle {
        guaranteed_until: Instant::now() + duration,
        shutdown_tx: Arc::new(parking_lot::Mutex::new(Some(shutdown_tx))),
    })
}

/// Run a typed processor until shutdown
async fn run_typed_processor<E: proven_stream::engine::TransactionEngine>(
    mut processor: proven_stream::processor::StreamProcessor<E>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Run the processor until shutdown
    tokio::select! {
        result = processor.run() => {
            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }
        _ = shutdown_rx => {
            Ok(())
        }
    }
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
) -> Result<
    proven_stream::processor::StreamProcessor<proven_kv::KvTransactionEngine>,
    crate::RunnerError,
> {
    let engine = proven_kv::KvTransactionEngine::new();
    Ok(proven_stream::processor::StreamProcessor::new(
        engine, client, stream,
    ))
}

/// Create a SQL processor
async fn create_sql_processor(
    stream: String,
    client: Arc<MockClient>,
) -> Result<
    proven_stream::processor::StreamProcessor<proven_sql::SqlTransactionEngine>,
    crate::RunnerError,
> {
    let engine = proven_sql::SqlTransactionEngine::new();
    Ok(proven_stream::processor::StreamProcessor::new(
        engine, client, stream,
    ))
}

/// Create a Queue processor
async fn create_queue_processor(
    stream: String,
    client: Arc<MockClient>,
) -> Result<
    proven_stream::processor::StreamProcessor<proven_queue::QueueTransactionEngine>,
    crate::RunnerError,
> {
    let engine = proven_queue::QueueTransactionEngine::new();
    Ok(proven_stream::processor::StreamProcessor::new(
        engine, client, stream,
    ))
}

/// Create a Resource processor
async fn create_resource_processor(
    stream: String,
    client: Arc<MockClient>,
) -> Result<
    proven_stream::processor::StreamProcessor<proven_resource::ResourceTransactionEngine>,
    crate::RunnerError,
> {
    let engine = proven_resource::ResourceTransactionEngine::new();
    Ok(proven_stream::processor::StreamProcessor::new(
        engine, client, stream,
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
