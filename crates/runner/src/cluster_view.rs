//! Cluster-wide view of running processors

use crate::messages::*;
use crate::runner::ProcessorInfo;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cluster-wide view of processors
pub struct ClusterView {
    /// Which processors are running where (stream -> list of locations)
    processors: HashMap<String, Vec<ProcessorLocation>>,

    /// When we last heard from each node (for cleanup)
    last_heartbeat: HashMap<String, Instant>,
}

/// Location of a processor
#[derive(Debug, Clone)]
struct ProcessorLocation {
    node_id: String,
    guaranteed_until_ms: u64,
}

impl ClusterView {
    /// Create a new empty cluster view
    pub fn new() -> Self {
        Self {
            processors: HashMap::new(),
            last_heartbeat: HashMap::new(),
        }
    }

    /// Update the view from a heartbeat
    pub fn update_from_heartbeat(&mut self, heartbeat: Heartbeat) {
        let node_id = heartbeat.node_id.clone();

        // Update last heartbeat time
        self.last_heartbeat.insert(node_id.clone(), Instant::now());

        // Remove old processor entries for this node
        for locations in self.processors.values_mut() {
            locations.retain(|loc| loc.node_id != node_id);
        }

        // Add current processors from heartbeat
        for processor in heartbeat.processors {
            let location = ProcessorLocation {
                node_id: node_id.clone(),
                guaranteed_until_ms: processor.guaranteed_until_ms,
            };

            self.processors
                .entry(processor.stream)
                .or_default()
                .push(location);
        }

        // Clean up stale nodes periodically
        self.cleanup_stale_nodes();
    }

    /// Find a processor for a stream
    pub fn find_processor(&self, stream: &str) -> Option<ProcessorInfo> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.processors.get(stream).and_then(|locations| {
            // Find a location with valid guarantee
            locations
                .iter()
                .find(|loc| loc.guaranteed_until_ms > now_ms)
                .map(|loc| ProcessorInfo {
                    node_id: loc.node_id.clone(),
                    stream: stream.to_string(),
                    guaranteed_until_ms: loc.guaranteed_until_ms,
                })
        })
    }

    /// Add a processor to the cluster view (used when ACKs are received)
    pub fn add_processor(&mut self, processor_info: ProcessorInfo) {
        let location = ProcessorLocation {
            node_id: processor_info.node_id,
            guaranteed_until_ms: processor_info.guaranteed_until_ms,
        };

        self.processors
            .entry(processor_info.stream)
            .or_default()
            .push(location);
    }

    /// Clean up information about nodes we haven't heard from
    fn cleanup_stale_nodes(&mut self) {
        let stale_threshold = Duration::from_secs(30);
        let stale_nodes: Vec<String> = self
            .last_heartbeat
            .iter()
            .filter(|(_, last)| last.elapsed() > stale_threshold)
            .map(|(node_id, _)| node_id.clone())
            .collect();

        for node_id in stale_nodes {
            self.last_heartbeat.remove(&node_id);

            // Remove processors from stale nodes
            for locations in self.processors.values_mut() {
                locations.retain(|loc| loc.node_id != node_id);
            }
        }
    }
}

impl Default for ClusterView {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::ProcessorInfo;

    #[test]
    fn test_cluster_view_updates() {
        let mut cluster_view = ClusterView::new();

        // Add a processor
        let processor_info = ProcessorInfo {
            node_id: "node2".to_string(),
            stream: "view-stream".to_string(),
            guaranteed_until_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 60000,
        };

        cluster_view.add_processor(processor_info.clone());

        // Find the processor
        let found = cluster_view.find_processor("view-stream");
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.node_id, "node2");
        assert_eq!(found.stream, "view-stream");
    }
}
