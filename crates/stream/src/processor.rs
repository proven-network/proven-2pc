//! Simplified stream processor using MessageRouter
//!
//! This processor focuses on phase management and stream consumption,
//! delegating all transaction logic to the MessageRouter.

use crate::engine::TransactionEngine;
use crate::error::{ProcessorError, Result};
use crate::router::MessageRouter;
use proven_common::{Timestamp, TransactionId};
use proven_engine::client::MessageStream;
use proven_engine::{Message, MockClient};
use proven_snapshot::SnapshotStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Processing phase for the stream processor
#[derive(Debug, Clone)]
pub enum ProcessorPhase {
    /// Replaying historical messages to rebuild state
    Replay,
    /// Checking for and executing recovery
    Recovery,
    /// Normal live processing
    Live,
}

/// Configuration for snapshot behavior
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// No messages for this duration triggers snapshot consideration
    pub idle_duration: Duration,
    /// Minimum commits before allowing snapshot
    pub min_commits_between_snapshots: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            idle_duration: Duration::from_secs(30),
            min_commits_between_snapshots: 100,
        }
    }
}

/// Simplified stream processor that delegates to MessageRouter
pub struct StreamProcessor<E: TransactionEngine> {
    /// Message router that handles all transaction logic
    router: MessageRouter<E>,

    /// Current processing phase
    phase: ProcessorPhase,

    /// Current stream offset
    pub current_offset: u64,

    /// Snapshot store (if configured)
    snapshot_store: Option<Arc<dyn SnapshotStore>>,

    /// Snapshot tracking
    last_snapshot_offset: u64,
    last_message_time: Option<Instant>,
    last_log_timestamp: Timestamp,

    /// Snapshot configuration
    pub snapshot_config: SnapshotConfig,

    /// Flag indicating if replay is complete
    is_ready: Arc<AtomicBool>,

    /// Stream name for identification
    stream_name: String,

    /// Client for stream operations
    client: Arc<MockClient>,
}

impl<E: TransactionEngine + Send> StreamProcessor<E> {
    /// Create a new stream processor
    pub fn new(
        engine: E,
        client: Arc<MockClient>,
        stream_name: String,
        snapshot_store: Arc<dyn SnapshotStore>,
    ) -> Self {
        // Get the engine's current log index (if it persists state)
        let engine_offset = engine.get_log_index().unwrap_or(0);

        // Try to get the last snapshot offset
        let start_offset;
        let last_snapshot_offset;

        if let Some(snapshot_offset) = snapshot_store.get(&stream_name) {
            // Verify the engine is at the right position
            if engine_offset == snapshot_offset {
                start_offset = snapshot_offset + 1;
                last_snapshot_offset = snapshot_offset;
                tracing::info!(
                    "Engine at offset {}, will replay from {} for stream {}",
                    snapshot_offset,
                    start_offset,
                    stream_name
                );
            } else {
                tracing::warn!(
                    "Engine offset ({}) doesn't match snapshot offset ({}), replaying from engine position for stream {}",
                    engine_offset,
                    snapshot_offset,
                    stream_name
                );
                start_offset = engine_offset + 1;
                last_snapshot_offset = engine_offset;
            }
        } else {
            // No snapshot, use engine's position
            start_offset = engine_offset + 1;
            last_snapshot_offset = engine_offset;
            if engine_offset > 0 {
                tracing::info!(
                    "No snapshot found, using engine offset {}, will replay from {} for stream {}",
                    engine_offset,
                    start_offset,
                    stream_name
                );
            }
        }

        // Create the router with the engine
        let router = MessageRouter::new(engine, client.clone(), stream_name.clone());

        Self {
            router,
            phase: ProcessorPhase::Replay,
            current_offset: start_offset,
            snapshot_store: Some(snapshot_store),
            last_snapshot_offset,
            last_message_time: None,
            last_log_timestamp: Timestamp::from_micros(0),
            snapshot_config: SnapshotConfig::default(),
            is_ready: Arc::new(AtomicBool::new(false)),
            stream_name,
            client,
        }
    }

    /// Get the current processing phase
    pub fn phase(&self) -> &ProcessorPhase {
        &self.phase
    }

    /// Check if the processor is ready
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Relaxed)
    }

    /// Process a message based on the current phase
    async fn process_message(
        &mut self,
        message: Message,
        msg_timestamp: Timestamp,
        msg_offset: u64,
    ) -> Result<()> {
        self.current_offset = msg_offset;
        self.last_log_timestamp = msg_timestamp;
        self.last_message_time = Some(Instant::now());

        match self.phase {
            ProcessorPhase::Replay => {
                // During replay, just track state without side effects
                self.router
                    .track_transaction_state(&message, TransactionId::new())?;
                Ok(())
            }
            ProcessorPhase::Recovery => {
                unreachable!("Should not process messages during recovery phase")
            }
            ProcessorPhase::Live => {
                // In live mode, route the message with log index
                self.router
                    .route_message(message, msg_timestamp, msg_offset)
            }
        }
    }

    /// Check if we should take a snapshot
    fn should_snapshot(&self) -> bool {
        // No active transactions
        if self.router.context().has_active_transactions() {
            return false;
        }

        // Check idle time
        if let Some(last_msg_time) = self.last_message_time
            && last_msg_time.elapsed() < self.snapshot_config.idle_duration
        {
            return false;
        }

        // Check minimum commit count
        if self.router.context().commits_since_snapshot
            < self.snapshot_config.min_commits_between_snapshots
        {
            return false;
        }

        true
    }

    /// Try to take a snapshot if conditions are met
    async fn try_snapshot(&mut self) -> Result<()> {
        if !self.should_snapshot() {
            return Ok(());
        }

        if let Some(store) = &self.snapshot_store {
            // Just save the current offset - engine has already persisted its state
            store.update(&self.stream_name, self.current_offset);

            self.last_snapshot_offset = self.current_offset;
            self.router.context_mut().commits_since_snapshot = 0;

            tracing::info!(
                "Updated snapshot to offset {} for stream {}",
                self.current_offset,
                self.stream_name
            );
        }

        Ok(())
    }

    /// Start the processor with replay
    pub async fn start_with_replay(mut self, shutdown_rx: oneshot::Receiver<()>) -> Result<()>
    where
        E: Send + 'static,
    {
        let stream_name = self.stream_name.clone();

        // Perform replay phase
        self.perform_replay().await?;

        // Mark as ready
        self.is_ready.store(true, Ordering::Relaxed);

        // Create live stream subscription
        let start_offset = if self.current_offset > 0 {
            Some(self.current_offset + 1)
        } else {
            Some(0)
        };

        let live_stream = self
            .client
            .stream_messages(stream_name.clone(), start_offset)
            .await
            .map_err(|e| ProcessorError::EngineError(e.to_string()))?;

        tracing::info!(
            "Stream processor for {} is ready, starting live processing",
            stream_name
        );

        // Spawn live processing task
        let stream_name_task = stream_name.clone();
        let handle = tokio::spawn(async move {
            tracing::debug!("Live processing started for stream: {}", stream_name_task);
            match self.run_live_processing(shutdown_rx, live_stream).await {
                Ok(()) => {
                    tracing::debug!("Live processing for {} ended normally", stream_name_task);
                }
                Err(e) => {
                    tracing::error!("Live processing for {} failed: {:?}", stream_name_task, e);
                }
            }
        });

        // Monitor for panics
        let stream_name_monitor = stream_name.clone();
        tokio::spawn(async move {
            match handle.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    tracing::error!("Panic in processor for {}: {:?}", stream_name_monitor, e);
                }
                Err(e) => {
                    tracing::warn!("Task join error for {}: {:?}", stream_name_monitor, e);
                }
            }
        });

        Ok(())
    }

    /// Process a readonly message from pubsub (no timestamp/offset)
    async fn process_readonly_message(&mut self, message: Message) -> Result<()> {
        // Only valid in Live phase
        if !matches!(self.phase, ProcessorPhase::Live) {
            tracing::warn!(
                "Received readonly message during {:?} phase for stream {}, ignoring",
                self.phase,
                self.stream_name
            );
            return Ok(());
        }

        // Route through readonly execution path
        // The message contains the read_timestamp header set by the coordinator
        self.router.route_readonly_message(message)
    }

    /// Perform the replay phase
    async fn perform_replay(&mut self) -> Result<()> {
        use proven_engine::DeadlineStreamItem;

        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio_stream::StreamExt;

        let mut last_offset = 0u64;

        if matches!(self.phase, ProcessorPhase::Replay) {
            // Get current time
            let physical = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let current_time = Timestamp::from_micros(physical);

            let client = self.client.clone();
            let stream_name = self.stream_name.clone();

            // Stream messages until current time
            let replay_stream = client
                .stream_messages_until_deadline(&stream_name, Some(0), current_time)
                .map_err(|e| ProcessorError::EngineError(e.to_string()))?;

            tokio::pin!(replay_stream);

            while let Some(item) = replay_stream.next().await {
                match item {
                    DeadlineStreamItem::Message(message, timestamp, offset) => {
                        last_offset = offset;
                        if let Err(e) = self.process_message(message, timestamp, offset).await {
                            tracing::error!("Error during replay at offset {}: {:?}", offset, e);
                        }
                    }
                    DeadlineStreamItem::DeadlineReached => {
                        // Transition to recovery then live
                        self.phase = ProcessorPhase::Recovery;
                        self.router.run_recovery_check(current_time).await?;
                        self.phase = ProcessorPhase::Live;
                        break;
                    }
                }
            }
        }

        self.current_offset = last_offset;
        Ok(())
    }

    /// Run live processing
    async fn run_live_processing(
        mut self,
        mut shutdown_rx: oneshot::Receiver<()>,
        mut live_stream: MessageStream,
    ) -> Result<()> {
        // Subscribe to readonly messages via pubsub
        let mut readonly_stream = match self
            .client
            .subscribe(&format!("stream.{}.readonly", self.stream_name), None)
            .await
        {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!("Failed to subscribe to readonly messages: {}", e);
                return Err(ProcessorError::EngineError(e.to_string()));
            }
        };

        // Timeout interval for periodic maintenance
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Prioritize ordered stream (biased select)
                biased;

                // Check for shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Stream processor for {} shutting down", self.stream_name);
                    break;
                }

                // Ordered stream messages (ReadWrite/AdHoc)
                result = live_stream.recv() => {
                    match result {
                        Some((message, timestamp, offset)) => {
                            if let Err(e) = self.process_message(message, timestamp, offset).await {
                                tracing::error!("Error processing message at offset {}: {:?}", offset, e);
                            }
                        }
                        None => {
                            // Stream ended
                            break;
                        }
                    }
                }

                // Unordered readonly messages via pubsub
                Some(message) = readonly_stream.recv() => {
                    if let Err(e) = self.process_readonly_message(message).await {
                        tracing::error!("Error processing readonly message: {:?}", e);
                    }
                }

                // Periodic maintenance (recovery, snapshots)
                _ = interval.tick() => {
                    // Run recovery check for expired transactions
                    // Use wall-clock time instead of last_log_timestamp so recovery
                    // can detect expired transactions even when processor is blocked
                    if let Err(e) = self
                        .router
                        .run_recovery_check(Timestamp::now())
                        .await
                    {
                        tracing::warn!("Failed to run recovery check: {:?}", e);
                    }

                    // Check for snapshot
                    if let Err(e) = self.try_snapshot().await {
                        tracing::warn!("Failed to create snapshot: {:?}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
