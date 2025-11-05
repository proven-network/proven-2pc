//! Main runner implementation for managing stream processors

use crate::ProcessorType;
use crate::cluster_view::ClusterView;
use crate::error::{Result, RunnerError};
use crate::heartbeat;
use crate::messages::{ProcessorAck, ProcessorExtension, ProcessorRequest};
use crate::processor::{self, ProcessorHandle};
use parking_lot::Mutex;
use proven_engine::MockClient;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Information about a running processor
#[derive(Debug, Clone)]
pub struct ProcessorInfo {
    pub node_id: String,
    pub stream: String,
    pub guaranteed_until_ms: u64,
}

/// Type for pending processor requests
type PendingRequests = Arc<Mutex<HashMap<Uuid, oneshot::Sender<ProcessorInfo>>>>;

/// Main runner that manages processors and cluster coordination
pub struct Runner {
    node_id: String,
    client: Arc<MockClient>,
    base_dir: PathBuf,

    // Core components
    pub(crate) processors: Arc<Mutex<HashMap<String, ProcessorHandle>>>,
    pub(crate) cluster_view: Arc<RwLock<ClusterView>>,

    // Pending processor requests waiting for ACKs
    pending_requests: PendingRequests,

    // Background tasks
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Runner {
    /// Create a new runner with base directory for storage
    pub fn new(
        node_id: impl Into<String>,
        client: Arc<MockClient>,
        base_dir: impl Into<PathBuf>,
    ) -> Self {
        let base_dir = base_dir.into();

        // Create base directory if it doesn't exist
        if let Err(e) = std::fs::create_dir_all(&base_dir) {
            tracing::warn!("Failed to create base directory {:?}: {}", base_dir, e);
        }

        Self {
            node_id: node_id.into(),
            client,
            base_dir,
            processors: Arc::new(Mutex::new(HashMap::new())),
            cluster_view: Arc::new(RwLock::new(ClusterView::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Start the runner (call once on node startup)
    pub async fn start(&self) -> Result<()> {
        // Register this node in its consensus groups
        // This ensures the node is recognized as a member of group(s)
        let _ = self.client.node_groups().await;

        // Start heartbeat task
        let heartbeat_task = heartbeat::start(
            self.node_id.clone(),
            self.client.clone(),
            self.processors.clone(),
        );
        self.tasks.lock().push(heartbeat_task);

        // Start listening for processor requests
        let request_listener = self.start_request_listener();
        self.tasks.lock().push(request_listener);

        // Start listening for heartbeats from other nodes
        let heartbeat_listener = self.start_heartbeat_listener();
        self.tasks.lock().push(heartbeat_listener);

        // Start listening for processor ACKs
        let ack_listener = self.start_ack_listener();
        self.tasks.lock().push(ack_listener);

        Ok(())
    }

    /// API for coordinators - ensure a processor is running somewhere
    pub async fn ensure_processor(
        &self,
        stream: &str,
        processor_type: ProcessorType,
        min_duration: Duration,
    ) -> Result<ProcessorInfo> {
        // First check if we're running this processor locally
        if let Some(handle) = self.processors.lock().get(stream) {
            let guaranteed_until_ms = handle.guaranteed_until_ms();
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // If processor is running locally and not expired, use it
            // We don't require the full min_duration remaining if it's already running
            if guaranteed_until_ms > now_ms {
                return Ok(ProcessorInfo {
                    node_id: self.node_id.clone(),
                    stream: stream.to_string(),
                    guaranteed_until_ms,
                });
            } else {
                println!(
                    "⏰ Processor for {} has expired (guaranteed_until: {} < now: {})",
                    stream, guaranteed_until_ms, now_ms
                );
            }
        }

        // Check if already running somewhere else
        if let Some(info) = self.cluster_view.read().await.find_processor(stream) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // If processor is running somewhere and not expired, use it
            // We don't require the full min_duration remaining if it's already running
            if info.guaranteed_until_ms > now_ms {
                return Ok(info);
            }

            // Processor exists but is expired - try to extend it
            if let Ok(extended_info) = self
                .extend_processor(stream, processor_type, min_duration)
                .await
            {
                return Ok(extended_info);
            }
        }

        // Request one to start
        self.request_processor(stream, processor_type, min_duration)
            .await
    }

    /// Extend a processor's guaranteed time
    pub async fn extend_processor(
        &self,
        stream: &str,
        processor_type: ProcessorType,
        additional_duration: Duration,
    ) -> Result<ProcessorInfo> {
        // Check if we're running this processor locally
        let is_local = self.processors.lock().contains_key(stream);

        if is_local {
            // Extend our own processor
            let new_guaranteed_until_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + additional_duration.as_millis() as u64;

            // Update the handle's guaranteed time (would need to add this method)
            // For now, just return the updated info
            let processor_info = ProcessorInfo {
                node_id: self.node_id.clone(),
                stream: stream.to_string(),
                guaranteed_until_ms: new_guaranteed_until_ms,
            };

            // Broadcast the extension
            let extension_msg = ProcessorExtension {
                node_id: self.node_id.clone(),
                stream: stream.to_string(),
                new_guaranteed_until_ms,
            };

            let message = proven_engine::Message::new(
                serde_json::to_vec(&extension_msg)
                    .map_err(|e| RunnerError::Other(e.to_string()))?,
                HashMap::new(),
            );

            self.client
                .publish("runner.processor.extension", vec![message])
                .await
                .map_err(|e| RunnerError::Engine(e.to_string()))?;

            // Update cluster view
            self.cluster_view
                .write()
                .await
                .add_processor(processor_info.clone());

            Ok(processor_info)
        } else {
            // Request extension from the node running it
            // For simplicity, just request a new processor
            self.request_processor(stream, processor_type, additional_duration)
                .await
        }
    }

    /// Request a processor to start on a specific node
    async fn request_processor(
        &self,
        stream: &str,
        processor_type: ProcessorType,
        min_duration: Duration,
    ) -> Result<ProcessorInfo> {
        // Get stream info to find its group
        let stream_info = self
            .client
            .get_stream_info(stream)
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?
            .ok_or_else(|| RunnerError::Other(format!("Stream '{}' not found", stream)))?;

        // Get the group ID
        let proven_engine::StreamPlacement::Group(group_id) = stream_info.placement else {
            return Err(RunnerError::Other("Stream is not in a group".to_string()));
        };

        // Get members of the group
        let group_info = self
            .client
            .get_group_info(group_id)
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?
            .ok_or_else(|| RunnerError::Other(format!("Group {:?} not found", group_id)))?;

        if group_info.members.is_empty() {
            return Err(RunnerError::Other("No members in group".to_string()));
        }

        // Pick a random member to run the processor
        use rand::seq::SliceRandom;
        let target_node = {
            let mut rng = rand::thread_rng();
            group_info
                .members
                .choose(&mut rng)
                .ok_or_else(|| RunnerError::Other("Failed to pick random member".to_string()))?
                .clone()
        };

        let request_id = Uuid::new_v4();

        // Set up response channel
        let (tx, rx) = oneshot::channel();

        // Store the pending request
        self.pending_requests.lock().insert(request_id, tx);

        // Send request with specific target node
        let request = ProcessorRequest {
            request_id,
            stream: stream.to_string(),
            processor_type,
            min_duration_ms: min_duration.as_millis() as u64,
            requester: self.node_id.clone(),
            target_node: Some(target_node),
        };

        let message = proven_engine::Message::new(
            serde_json::to_vec(&request).map_err(|e| RunnerError::Other(e.to_string()))?,
            HashMap::new(),
        );

        self.client
            .publish("runner.processor.request", vec![message])
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?;

        // Wait for response with timeout
        tokio::select! {
            result = rx => {
                // Clean up the pending request
                self.pending_requests.lock().remove(&request_id);
                result.map_err(|_| RunnerError::RequestTimeout)
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                // Clean up the pending request on timeout
                self.pending_requests.lock().remove(&request_id);
                // TODO: Could retry with a different node
                Err(RunnerError::RequestTimeout)
            }
        }
    }

    /// Start listening for processor requests
    fn start_request_listener(&self) -> JoinHandle<()> {
        let client = self.client.clone();
        let node_id = self.node_id.clone();
        let processors = self.processors.clone();
        let base_dir = self.base_dir.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::listen_for_requests(client, node_id, processors, base_dir).await {
                tracing::error!("Request listener failed: {}", e);
            }
        })
    }

    /// Start listening for heartbeats
    fn start_heartbeat_listener(&self) -> JoinHandle<()> {
        let client = self.client.clone();
        let cluster_view = self.cluster_view.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::listen_for_heartbeats(client, cluster_view).await {
                tracing::error!("Heartbeat listener failed: {}", e);
            }
        })
    }

    /// Start listening for processor ACKs
    fn start_ack_listener(&self) -> JoinHandle<()> {
        let client = self.client.clone();
        let pending_requests = self.pending_requests.clone();
        let cluster_view = self.cluster_view.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::listen_for_acks(client, pending_requests, cluster_view).await {
                tracing::error!("ACK listener failed: {}", e);
            }
        })
    }

    /// Listen for processor requests
    async fn listen_for_requests(
        client: Arc<MockClient>,
        node_id: String,
        processors: Arc<Mutex<HashMap<String, ProcessorHandle>>>,
        base_dir: PathBuf,
    ) -> Result<()> {
        // Subscribe to processor requests
        let mut subscription = client
            .subscribe("runner.processor.request", None)
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?;

        while let Some(message) = subscription.recv().await {
            // Parse request
            let request: ProcessorRequest = serde_json::from_slice(&message.body)
                .map_err(|e| RunnerError::Other(e.to_string()))?;

            // Check if this request is targeted at us
            let should_handle = match &request.target_node {
                Some(target) => target == &node_id,
                None => {
                    // Legacy behavior for backward compatibility (shouldn't happen)
                    false
                }
            };

            if should_handle {
                // Start processor
                match processor::start_processor(
                    request.stream.clone(),
                    request.processor_type,
                    Duration::from_millis(request.min_duration_ms),
                    client.clone(),
                    base_dir.clone(),
                )
                .await
                {
                    Ok(handle) => {
                        // Store handle
                        processors
                            .lock()
                            .insert(request.stream.clone(), handle.clone());

                        // Send ACK
                        let ack = ProcessorAck {
                            request_id: request.request_id,
                            node_id: node_id.clone(),
                            stream: request.stream.clone(),
                            guaranteed_until_ms: handle.guaranteed_until_ms(),
                        };

                        let ack_message = proven_engine::Message::new(
                            serde_json::to_vec(&ack).unwrap(),
                            HashMap::new(),
                        );

                        let _ = client
                            .publish("runner.processor.ack", vec![ack_message])
                            .await;
                    }
                    Err(e) => {
                        tracing::warn!("Failed to start processor for {}: {}", request.stream, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Listen for heartbeats from other nodes
    async fn listen_for_heartbeats(
        client: Arc<MockClient>,
        cluster_view: Arc<RwLock<ClusterView>>,
    ) -> Result<()> {
        let mut subscription = client
            .subscribe("runner.heartbeat", None)
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?;

        while let Some(message) = subscription.recv().await {
            if let Ok(heartbeat) =
                serde_json::from_slice::<crate::messages::Heartbeat>(&message.body)
            {
                cluster_view.write().await.update_from_heartbeat(heartbeat);
            }
        }

        Ok(())
    }

    /// Listen for processor ACKs and complete pending requests
    async fn listen_for_acks(
        client: Arc<MockClient>,
        pending_requests: PendingRequests,
        cluster_view: Arc<RwLock<ClusterView>>,
    ) -> Result<()> {
        let mut subscription = client
            .subscribe("runner.processor.ack", None)
            .await
            .map_err(|e| RunnerError::Engine(e.to_string()))?;

        while let Some(message) = subscription.recv().await {
            if let Ok(ack) = serde_json::from_slice::<ProcessorAck>(&message.body) {
                // Update cluster view with the new processor
                let processor_info = ProcessorInfo {
                    node_id: ack.node_id.clone(),
                    stream: ack.stream.clone(),
                    guaranteed_until_ms: ack.guaranteed_until_ms,
                };

                // Complete the pending request if we have it
                if let Some(tx) = pending_requests.lock().remove(&ack.request_id) {
                    let _ = tx.send(processor_info.clone());
                }

                // Update cluster view (add processor to the node that acknowledged)
                // This ensures all nodes have an up-to-date view of running processors
                cluster_view.write().await.add_processor(processor_info);
            }
        }

        Ok(())
    }

    /// Shutdown the runner
    pub async fn shutdown(&self) {
        // Cancel all tasks
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }

        // Shutdown all processors
        let handles: Vec<_> = self.processors.lock().drain().map(|(_, h)| h).collect();
        for handle in handles {
            handle.shutdown().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_engine::{MockClient, MockEngine};
    use std::time::Duration;

    /// Create a test engine with a stream
    async fn create_test_engine_with_stream(stream_name: &str) -> Arc<MockEngine> {
        let engine = Arc::new(MockEngine::new());
        engine.create_stream(stream_name.to_string()).unwrap();
        engine
    }

    #[tokio::test]
    async fn test_processor_extension() {
        let engine = create_test_engine_with_stream("ext-stream").await;
        let client = Arc::new(MockClient::new("node1".to_string(), engine.clone()));
        let temp_dir = tempfile::tempdir().unwrap();
        let runner = Runner::new("node1", client.clone(), temp_dir.path());

        runner.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Manually add a processor to simulate it running
        runner.processors.lock().insert(
            "ext-stream".to_string(),
            crate::processor::ProcessorHandle::new_for_test(
                std::time::Instant::now() + Duration::from_secs(30),
            ),
        );

        // Extend the processor
        let result = runner
            .extend_processor("ext-stream", ProcessorType::Kv, Duration::from_secs(60))
            .await;

        assert!(result.is_ok());
        let info = result.unwrap();
        assert_eq!(info.node_id, "node1");
        assert_eq!(info.stream, "ext-stream");

        // Check that guaranteed time was extended
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(info.guaranteed_until_ms > now_ms + 50000); // At least 50 seconds in future

        runner.shutdown().await;
    }
}
