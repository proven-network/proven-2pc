//! Heartbeat sending logic

use crate::messages::{Heartbeat, ProcessorState, ProcessorStatus};
use crate::processor::ProcessorHandle;
use dashmap::DashMap;
use proven_engine::MockClient;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Start the heartbeat task
pub fn start(
    node_id: String,
    client: Arc<MockClient>,
    processors: Arc<DashMap<String, ProcessorHandle>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;

            // Build heartbeat
            let heartbeat = build_heartbeat(&node_id, &client, &processors).await;

            // Send it
            if let Err(e) = send_heartbeat(&client, heartbeat).await {
                tracing::warn!("Failed to send heartbeat: {}", e);
            }
        }
    })
}

/// Build a heartbeat message
async fn build_heartbeat(
    node_id: &str,
    client: &Arc<MockClient>,
    processors: &Arc<DashMap<String, ProcessorHandle>>,
) -> Heartbeat {
    let processor_statuses: Vec<ProcessorStatus> = processors
        .iter()
        .map(|entry| {
            let stream = entry.key();
            let handle = entry.value();
            ProcessorStatus {
                stream: stream.clone(),
                guaranteed_until_ms: handle.guaranteed_until_ms(),
                last_activity_ms: handle.last_activity_ms(),
                state: if handle.is_shutting_down() {
                    ProcessorState::ShuttingDown
                } else {
                    ProcessorState::Active
                },
            }
        })
        .collect();

    // Get raft groups this node is part of
    let raft_groups = match client.node_groups().await {
        Ok(groups) => groups.iter().map(|g| format!("group-{}", g.0)).collect(),
        Err(e) => {
            tracing::debug!("Failed to get node groups: {}", e);
            Vec::new()
        }
    };

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    Heartbeat {
        node_id: node_id.to_string(),
        processors: processor_statuses,
        raft_groups,
        timestamp_ms,
    }
}

/// Send a heartbeat
async fn send_heartbeat(
    client: &Arc<MockClient>,
    heartbeat: Heartbeat,
) -> Result<(), Box<dyn std::error::Error>> {
    let message = proven_engine::Message::new(serde_json::to_vec(&heartbeat)?, HashMap::new());

    client.publish("runner.heartbeat", vec![message]).await?;

    Ok(())
}
