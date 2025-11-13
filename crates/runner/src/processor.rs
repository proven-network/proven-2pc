//! Processor lifecycle management

use parking_lot::Mutex;
use proven_common::ProcessorType;
use proven_engine::MockClient;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Handle to a running processor
#[derive(Clone)]
pub struct ProcessorHandle {
    pub(crate) guaranteed_until: Arc<Mutex<Instant>>,
    pub(crate) shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    pub(crate) last_activity: Arc<Mutex<Instant>>,
}

impl ProcessorHandle {
    /// Get guaranteed until time in milliseconds
    pub fn guaranteed_until_ms(&self) -> u64 {
        let guaranteed_until = *self.guaranteed_until.lock();
        let remaining = guaranteed_until.saturating_duration_since(Instant::now());
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now_ms + remaining.as_millis() as u64
    }

    /// Extend the guaranteed time by the given duration
    pub fn extend_guarantee(&self, additional_duration: Duration) {
        let mut guaranteed_until = self.guaranteed_until.lock();
        *guaranteed_until = Instant::now() + additional_duration;
    }

    /// Get the last activity time instant
    pub fn last_activity_instant(&self) -> Instant {
        *self.last_activity.lock()
    }

    /// Get the last activity time in milliseconds since epoch
    pub fn last_activity_ms(&self) -> u64 {
        let last_activity = *self.last_activity.lock();
        let elapsed_since_last = Instant::now().saturating_duration_since(last_activity);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now_ms - elapsed_since_last.as_millis() as u64
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
///
/// This function spawns the processor in the background and waits for it to complete
/// replay and transition to live processing before returning. This ensures the processor
/// is ready to handle operations when ensure_processor() returns.
pub async fn start_processor(
    stream: String,
    processor_type: ProcessorType,
    duration: Duration,
    client: Arc<MockClient>,
    base_dir: PathBuf,
) -> Result<ProcessorHandle, crate::RunnerError> {
    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Create last activity tracker
    let last_activity = Arc::new(Mutex::new(Instant::now()));

    // Create readiness channel - processor signals when it's ready to accept operations
    let (ready_tx, ready_rx) = oneshot::channel();

    // Create kernel and spawn orchestration in background
    match processor_type {
        ProcessorType::Kv => {
            let kernel = create_kv_kernel(stream.clone(), client.clone(), base_dir.clone()).await?;
            let client_clone = client.clone();
            let last_activity_clone = last_activity.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::orchestration::run_stream_processor(
                    kernel,
                    client_clone,
                    shutdown_rx,
                    last_activity_clone,
                    ready_tx,
                )
                .await
                {
                    tracing::error!("KV processor error: {:?}", e);
                }
            });
        }
        ProcessorType::Sql => {
            let kernel =
                create_sql_kernel(stream.clone(), client.clone(), base_dir.clone()).await?;
            let client_clone = client.clone();
            let last_activity_clone = last_activity.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::orchestration::run_stream_processor(
                    kernel,
                    client_clone,
                    shutdown_rx,
                    last_activity_clone,
                    ready_tx,
                )
                .await
                {
                    tracing::error!("SQL processor error: {:?}", e);
                }
            });
        }
        ProcessorType::Queue => {
            let kernel =
                create_queue_kernel(stream.clone(), client.clone(), base_dir.clone()).await?;
            let client_clone = client.clone();
            let last_activity_clone = last_activity.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::orchestration::run_stream_processor(
                    kernel,
                    client_clone,
                    shutdown_rx,
                    last_activity_clone,
                    ready_tx,
                )
                .await
                {
                    tracing::error!("Queue processor error: {:?}", e);
                }
            });
        }
        ProcessorType::Resource => {
            let kernel =
                create_resource_kernel(stream.clone(), client.clone(), base_dir.clone()).await?;
            let client_clone = client.clone();
            let last_activity_clone = last_activity.clone();
            tokio::spawn(async move {
                if let Err(e) = crate::orchestration::run_stream_processor(
                    kernel,
                    client_clone,
                    shutdown_rx,
                    last_activity_clone,
                    ready_tx,
                )
                .await
                {
                    tracing::error!("Resource processor error: {:?}", e);
                }
            });
        }
    }

    // Wait for processor to complete replay and become ready to accept operations
    // This ensures ensure_processor() doesn't return until the processor can handle requests
    ready_rx
        .await
        .map_err(|_| crate::RunnerError::Other("Processor failed during startup".to_string()))?;

    Ok(ProcessorHandle {
        guaranteed_until: Arc::new(Mutex::new(Instant::now() + duration)),
        shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        last_activity,
    })
}

/// Create a KV processing kernel
async fn create_kv_kernel(
    stream: String,
    client: Arc<MockClient>,
    base_dir: PathBuf,
) -> Result<
    proven_processor::StreamProcessingKernel<proven_kv::KvTransactionEngine>,
    crate::RunnerError,
> {
    // Create directory structure: {base_dir}/kv/{stream_name}
    let storage_path = base_dir.join("kv").join(&stream);
    std::fs::create_dir_all(&storage_path)
        .map_err(|e| crate::RunnerError::Other(format!("Failed to create KV directory: {}", e)))?;

    let config = proven_mvcc::StorageConfig::new(storage_path);
    let engine = proven_kv::KvTransactionEngine::with_config(config);
    Ok(proven_processor::StreamProcessingKernel::new(
        engine, client, stream,
    ))
}

/// Create a SQL processing kernel
async fn create_sql_kernel(
    stream: String,
    client: Arc<MockClient>,
    base_dir: PathBuf,
) -> Result<
    proven_processor::StreamProcessingKernel<proven_sql::SqlTransactionEngine>,
    crate::RunnerError,
> {
    // Create directory structure: {base_dir}/sql/{stream_name}
    let storage_path = base_dir.join("sql").join(&stream);
    std::fs::create_dir_all(&storage_path)
        .map_err(|e| crate::RunnerError::Other(format!("Failed to create SQL directory: {}", e)))?;

    let config = proven_sql::SqlStorageConfig::with_data_dir(storage_path);
    let engine = proven_sql::SqlTransactionEngine::new(config);
    Ok(proven_processor::StreamProcessingKernel::new(
        engine, client, stream,
    ))
}

/// Create a Queue processing kernel
async fn create_queue_kernel(
    stream: String,
    client: Arc<MockClient>,
    base_dir: PathBuf,
) -> Result<
    proven_processor::StreamProcessingKernel<proven_queue::QueueTransactionEngine>,
    crate::RunnerError,
> {
    // Create directory structure: {base_dir}/queue/{stream_name}
    let storage_path = base_dir.join("queue").join(&stream);
    std::fs::create_dir_all(&storage_path).map_err(|e| {
        crate::RunnerError::Other(format!("Failed to create Queue directory: {}", e))
    })?;

    let config = proven_mvcc::StorageConfig::new(storage_path);
    let engine = proven_queue::QueueTransactionEngine::with_config(config);
    Ok(proven_processor::StreamProcessingKernel::new(
        engine, client, stream,
    ))
}

/// Create a Resource processing kernel
async fn create_resource_kernel(
    stream: String,
    client: Arc<MockClient>,
    base_dir: PathBuf,
) -> Result<
    proven_processor::StreamProcessingKernel<proven_resource::ResourceTransactionEngine>,
    crate::RunnerError,
> {
    // Create directory structure: {base_dir}/resource/{stream_name}
    let storage_path = base_dir.join("resource").join(&stream);
    std::fs::create_dir_all(&storage_path).map_err(|e| {
        crate::RunnerError::Other(format!("Failed to create Resource directory: {}", e))
    })?;

    let config = proven_mvcc::StorageConfig::new(storage_path);
    let engine = proven_resource::ResourceTransactionEngine::with_config(config);
    Ok(proven_processor::StreamProcessingKernel::new(
        engine, client, stream,
    ))
}

#[cfg(test)]
impl ProcessorHandle {
    /// Test helper to create a ProcessorHandle for testing
    pub fn new_for_test(guaranteed_until: Instant) -> Self {
        let (shutdown_tx, _) = oneshot::channel();

        Self {
            guaranteed_until: Arc::new(Mutex::new(guaranteed_until)),
            shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
            last_activity: Arc::new(Mutex::new(Instant::now())),
        }
    }
}
