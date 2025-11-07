//! Stream processor orchestration - manages I/O and lifecycle for stream processing kernels
//!
//! This module owns the event loop, stream subscriptions, and phase transitions.
//! It delegates actual message processing to the StreamProcessingKernel.

use parking_lot::Mutex;
use proven_engine::MockClient;
use proven_stream::{ResponseMode, StreamProcessingKernel, TransactionEngine};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Run a stream processor with full lifecycle management
///
/// This function:
/// 1. Recovers transaction state from persisted metadata
/// 2. Performs replay to catch up with the stream
/// 3. Transitions to live processing
/// 4. Handles graceful shutdown with message draining
///
/// The `last_activity` parameter tracks when the processor last received a message.
/// The `ready_tx` signals when the processor has completed replay and is ready to accept operations.
pub async fn run_stream_processor<E: TransactionEngine>(
    mut kernel: StreamProcessingKernel<E>,
    client: Arc<MockClient>,
    shutdown_rx: oneshot::Receiver<()>,
    last_activity: Arc<Mutex<Instant>>,
    ready_tx: oneshot::Sender<()>,
) -> Result<(), crate::RunnerError> {
    // Step 1: Recover persisted transaction state from metadata
    kernel
        .recover_transaction_state()
        .map_err(|e| crate::RunnerError::Other(format!("Recovery failed: {}", e)))?;

    // Step 2: Perform replay phase to catch up with stream
    perform_replay(&mut kernel, &client, &last_activity).await?;

    // Step 3: Signal that processor is ready to accept operations
    let _ = ready_tx.send(());

    // Step 4: Transition to live processing
    kernel.set_response_mode(ResponseMode::Send);
    run_live_processing(kernel, client, shutdown_rx, last_activity).await
}

/// Perform replay from engine's current position to stream head
async fn perform_replay<E: TransactionEngine>(
    kernel: &mut StreamProcessingKernel<E>,
    client: &Arc<MockClient>,
    last_activity: &Arc<Mutex<Instant>>,
) -> Result<(), crate::RunnerError> {
    let stream_name = kernel.stream_name().to_string();
    let start_offset = kernel.current_offset() + 1;

    tracing::info!(
        "[{}] Starting replay from offset {}",
        stream_name,
        start_offset
    );

    // Ensure we're in replay phase
    kernel.set_response_mode(ResponseMode::Suppress);

    // Use stream_messages_until_deadline to replay up to current time
    // This gives us precise "caught up" detection without heuristics
    use tokio_stream::StreamExt;

    let replay_deadline = proven_common::Timestamp::now();
    let replay_stream = client
        .stream_messages_until_deadline(&stream_name, Some(start_offset), replay_deadline)
        .map_err(|e| crate::RunnerError::Engine(e.to_string()))?;

    tokio::pin!(replay_stream);

    let mut count = 0;
    let start_time = std::time::Instant::now();

    // Process messages until we reach the deadline (caught up)
    while let Some(result) = replay_stream.next().await {
        match result {
            proven_engine::DeadlineStreamItem::Message(message, timestamp, offset) => {
                // Update last activity time
                *last_activity.lock() = Instant::now();

                if let Err(e) = kernel.process_ordered(message, timestamp, offset).await {
                    tracing::error!(
                        "[{}] Error during replay at offset {}: {:?}",
                        stream_name,
                        offset,
                        e
                    );
                }
                count += 1;

                // Log progress every 10k messages
                if count % 10_000 == 0 {
                    tracing::info!(
                        "[{}] Replay progress: {} messages, at offset {}",
                        stream_name,
                        count,
                        offset
                    );
                }
            }
            proven_engine::DeadlineStreamItem::DeadlineReached => {
                // Caught up to the deadline - replay complete
                tracing::debug!(
                    "[{}] Reached replay deadline, caught up at offset {}",
                    stream_name,
                    kernel.current_offset()
                );
                break;
            }
        }
    }

    let elapsed = start_time.elapsed();
    tracing::info!(
        "[{}] Replay complete. Processed {} messages in {:.2}s, now at offset {}",
        stream_name,
        count,
        elapsed.as_secs_f64(),
        kernel.current_offset()
    );

    Ok(())
}

/// Run live processing with event loop and graceful shutdown
async fn run_live_processing<E: TransactionEngine>(
    mut kernel: StreamProcessingKernel<E>,
    client: Arc<MockClient>,
    mut shutdown_rx: oneshot::Receiver<()>,
    last_activity: Arc<Mutex<Instant>>,
) -> Result<(), crate::RunnerError> {
    let stream_name = kernel.stream_name().to_string();

    tracing::info!(
        "[{}] Starting live processing from offset {}",
        stream_name,
        kernel.current_offset()
    );

    // Subscribe to ordered stream (start from next offset after engine's position)
    let start_offset = if kernel.current_offset() > 0 {
        Some(kernel.current_offset() + 1)
    } else {
        Some(0)
    };

    let mut ordered_stream = client
        .stream_messages(stream_name.clone(), start_offset)
        .await
        .map_err(|e| crate::RunnerError::Engine(e.to_string()))?;

    // Subscribe to readonly pubsub (ReadOnly + AdHoc reads)
    let mut readonly_stream = client
        .subscribe(&format!("stream.{}.readonly", stream_name), None)
        .await
        .map_err(|e| crate::RunnerError::Engine(e.to_string()))?;

    // Idle detection for noop injection (check every 100ms)
    let mut idle_check = tokio::time::interval(Duration::from_millis(100));
    idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // Prioritize shutdown and ordered stream
            biased;

            // Shutdown signal
            _ = &mut shutdown_rx => {
                tracing::info!("[{}] Shutdown signal received, draining messages...", stream_name);

                // Drain any pending messages with timeout
                let drain_timeout = tokio::time::sleep(Duration::from_secs(5));
                tokio::pin!(drain_timeout);

                let mut drained = 0;
                loop {
                    tokio::select! {
                        _ = &mut drain_timeout => {
                            if drained > 0 {
                                tracing::info!("[{}] Drained {} messages before timeout", stream_name, drained);
                            }
                            tracing::warn!("[{}] Drain timeout reached, forcing shutdown", stream_name);
                            break;
                        }
                        result = ordered_stream.recv() => {
                            match result {
                                Some((message, timestamp, offset)) => {
                                    if let Err(e) = kernel.process_ordered(message, timestamp, offset).await {
                                        tracing::error!(
                                            "[{}] Error processing message during drain at offset {}: {:?}",
                                            stream_name, offset, e
                                        );
                                    }
                                    drained += 1;
                                }
                                None => {
                                    if drained > 0 {
                                        tracing::info!("[{}] Successfully drained {} messages", stream_name, drained);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

                tracing::info!("[{}] Clean shutdown complete", stream_name);
                break;
            }

            // Ordered stream messages (ReadWrite + AdHoc writes)
            result = ordered_stream.recv() => {
                match result {
                    Some((message, timestamp, offset)) => {
                        // Update last activity time
                        *last_activity.lock() = Instant::now();

                        if let Err(e) = kernel.process_ordered(message, timestamp, offset).await {
                            tracing::error!(
                                "[{}] Error processing ordered message at offset {}: {:?}",
                                stream_name, offset, e
                            );
                        }
                    }
                    None => {
                        tracing::warn!("[{}] Ordered stream ended", stream_name);
                        break;
                    }
                }
            }

            // Unordered pubsub messages (ReadOnly + AdHoc reads)
            Some(message) = readonly_stream.recv() => {
                // Update last activity time
                *last_activity.lock() = Instant::now();

                // Process in parallel - spawn concurrent task without blocking
                if let Err(e) = kernel.process_read_only(message) {
                    tracing::error!(
                        "[{}] Error spawning readonly task: {:?}",
                        stream_name, e
                    );
                }
            }

            // Idle detection for noop injection
            _ = idle_check.tick() => {
                let now = proven_common::Timestamp::now();

                // Check if there are transactions that need processing (expired or GC-able)
                if kernel.needs_processing(now) {
                    // Stream is idle with expired transactions - inject a noop
                    tracing::debug!(
                        "[{}] Injecting noop message to trigger recovery/GC",
                        stream_name
                    );

                    use proven_protocol::OrderedMessage;
                    let noop_msg = OrderedMessage::<E::Operation>::Noop;
                    if let Err(e) = client.publish_to_stream(
                        stream_name.clone(),
                        vec![noop_msg.into_message()],
                    ).await {
                        tracing::warn!(
                            "[{}] Failed to inject noop: {:?}",
                            stream_name, e
                        );
                    }
                }
            }
        }
    }

    tracing::info!("[{}] Live processing stopped", stream_name);
    Ok(())
}
