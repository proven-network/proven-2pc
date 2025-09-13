//! Core mock engine implementation
//!
//! This module provides the central mock engine that manages streams,
//! pub/sub, and coordination between components.

use crate::{
    Message, MockEngineError, Result,
    stream::{DeadlineStreamItem, StreamManager},
};
use parking_lot::Mutex;
use proven_hlc::HlcTimestamp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::Stream as TokioStream;

/// Type alias for request handler channels
type RequestHandler = mpsc::UnboundedSender<(Message, oneshot::Sender<Message>)>;

/// Consensus group identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConsensusGroupId(pub u64);

/// Stream placement information
#[derive(Debug, Clone)]
pub enum StreamPlacement {
    Global,
    Group(ConsensusGroupId),
}

/// Stream information including metadata and placement
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub stream_name: String,
    pub placement: StreamPlacement,
}

/// Group information including members
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub group_id: ConsensusGroupId,
    pub members: Vec<String>,
    pub leader: Option<String>,
}

/// Mock engine that simulates the production consensus engine
pub struct MockEngine {
    /// Stream manager for ordered message streams
    streams: Arc<StreamManager>,

    /// Pub/sub subscriptions
    subscriptions: Arc<Mutex<HashMap<String, Vec<mpsc::UnboundedSender<Message>>>>>,

    /// Request/reply handlers
    request_handlers: Arc<Mutex<HashMap<String, RequestHandler>>>,

    /// Stream metadata (placement information)
    stream_info: Arc<Mutex<HashMap<String, StreamInfo>>>,

    /// Group membership - which nodes are in which groups
    group_membership: Arc<Mutex<HashMap<ConsensusGroupId, HashSet<String>>>>,

    /// Node to groups mapping - which groups each node is part of
    node_groups: Arc<Mutex<HashMap<String, HashSet<ConsensusGroupId>>>>,
}

impl MockEngine {
    /// Create a new mock engine
    pub fn new() -> Self {
        // Initialize with a default group that all nodes belong to
        let default_group = ConsensusGroupId(1);
        let mut group_membership = HashMap::new();
        group_membership.insert(default_group, HashSet::new());

        Self {
            streams: Arc::new(StreamManager::new()),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            request_handlers: Arc::new(Mutex::new(HashMap::new())),
            stream_info: Arc::new(Mutex::new(HashMap::new())),
            group_membership: Arc::new(Mutex::new(group_membership)),
            node_groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the stream manager
    pub fn streams(&self) -> &Arc<StreamManager> {
        &self.streams
    }

    /// Create a new stream
    pub fn create_stream(&self, name: String) -> Result<()> {
        // Create the stream in the stream manager
        self.streams.create_stream(name.clone())?;

        // Store stream info - all streams go to the default group in mock
        let mut stream_info = self.stream_info.lock();
        stream_info.insert(
            name.clone(),
            StreamInfo {
                stream_name: name,
                placement: StreamPlacement::Group(ConsensusGroupId(1)),
            },
        );

        Ok(())
    }

    /// Publish messages to a stream
    pub fn publish_to_stream(&self, stream_name: &str, messages: Vec<Message>) -> Result<u64> {
        self.streams.append_to_stream(stream_name, messages)
    }

    /// Stream messages from a stream
    pub fn stream_messages(
        &self,
        stream_name: &str,
        start_sequence: Option<u64>,
    ) -> Result<mpsc::UnboundedReceiver<(Message, HlcTimestamp, u64)>> {
        self.streams.create_consumer(stream_name, start_sequence)
    }

    /// Publish messages to a subject (pub/sub)
    pub fn publish(&self, subject: &str, messages: Vec<Message>) -> Result<()> {
        let subs = self.subscriptions.lock();

        // Find all matching subscriptions
        let mut sent = false;
        for (pattern, subscribers) in subs.iter() {
            if subject_matches(subject, pattern) {
                for sub in subscribers {
                    for msg in &messages {
                        let _ = sub.send(msg.clone());
                        sent = true;
                    }
                }
            }
        }

        // Also check request handlers
        let handlers = self.request_handlers.lock();
        if let Some(handler) = handlers.get(subject) {
            // For request handlers, we expect a single message and need a reply
            if messages.len() == 1 {
                let (reply_tx, _reply_rx) = oneshot::channel();
                let _ = handler.send((messages[0].clone(), reply_tx));
                sent = true;
            }
        }

        if !sent {
            // It's OK if no one is listening - that's valid in pub/sub
            // But we'll return success
        }

        Ok(())
    }

    /// Subscribe to a subject pattern
    pub fn subscribe(&self, subject_pattern: &str) -> mpsc::UnboundedReceiver<Message> {
        let (tx, rx) = mpsc::unbounded_channel();

        let mut subs = self.subscriptions.lock();
        subs.entry(subject_pattern.to_string())
            .or_default()
            .push(tx);

        rx
    }

    /// Register a request handler for a subject
    pub fn register_handler(
        &self,
        subject: &str,
    ) -> mpsc::UnboundedReceiver<(Message, oneshot::Sender<Message>)> {
        let (tx, rx) = mpsc::unbounded_channel();

        let mut handlers = self.request_handlers.lock();
        handlers.insert(subject.to_string(), tx);

        rx
    }

    /// Send a request and wait for reply
    pub async fn request(
        &self,
        subject: &str,
        message: Message,
        timeout_ms: u64,
    ) -> Result<Message> {
        // Check if there's a handler for this subject
        let reply_rx = {
            let handlers = self.request_handlers.lock();
            if let Some(handler) = handlers.get(subject) {
                let (reply_tx, reply_rx) = oneshot::channel();

                // Send the request to the handler
                if handler.send((message, reply_tx)).is_err() {
                    return Err(MockEngineError::ChannelClosed);
                }
                Some(reply_rx)
            } else {
                None
            }
        };

        if let Some(reply_rx) = reply_rx {
            // Wait for reply with timeout
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), reply_rx).await
            {
                Ok(Ok(reply)) => Ok(reply),
                Ok(Err(_)) => Err(MockEngineError::ChannelClosed),
                Err(_) => Err(MockEngineError::Timeout),
            }
        } else {
            // No handler registered
            Err(MockEngineError::NoSubscribers(subject.to_string()))
        }
    }

    /// Create a stream that reads messages until deadline
    pub fn create_deadline_stream(
        &self,
        stream_name: &str,
        start_offset: u64,
        deadline: HlcTimestamp,
    ) -> Result<impl TokioStream<Item = DeadlineStreamItem>> {
        self.streams
            .create_deadline_stream(stream_name, start_offset, deadline)
    }

    /// Clean up closed subscriptions
    pub fn cleanup(&self) {
        let mut subs = self.subscriptions.lock();
        for subscribers in subs.values_mut() {
            subscribers.retain(|s| !s.is_closed());
        }

        let mut handlers = self.request_handlers.lock();
        handlers.retain(|_, h| !h.is_closed());
    }

    /// Get information about a stream
    pub fn get_stream_info(&self, stream_name: &str) -> Result<Option<StreamInfo>> {
        let stream_info = self.stream_info.lock();
        Ok(stream_info.get(stream_name).cloned())
    }

    /// Get all consensus groups this node is a member of
    pub fn node_groups(&self, node_id: &str) -> Result<Vec<ConsensusGroupId>> {
        // Ensure node is registered in the default group
        let mut node_groups = self.node_groups.lock();
        let groups = node_groups.entry(node_id.to_string()).or_insert_with(|| {
            // Add node to default group
            let default_group = ConsensusGroupId(1);
            let mut group_membership = self.group_membership.lock();
            group_membership
                .entry(default_group)
                .or_default()
                .insert(node_id.to_string());

            let mut groups = HashSet::new();
            groups.insert(default_group);
            groups
        });

        Ok(groups.iter().copied().collect())
    }

    /// Get information about a specific consensus group
    pub fn get_group_info(&self, group_id: ConsensusGroupId) -> Result<Option<GroupInfo>> {
        let group_membership = self.group_membership.lock();

        if let Some(members) = group_membership.get(&group_id) {
            let members_vec: Vec<String> = members.iter().cloned().collect();
            // Pick first member as leader for simplicity
            let leader = members_vec.first().cloned();

            Ok(Some(GroupInfo {
                group_id,
                members: members_vec,
                leader,
            }))
        } else {
            Ok(None)
        }
    }
}

impl Default for MockEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if a subject matches a pattern
fn subject_matches(subject: &str, pattern: &str) -> bool {
    // Simple pattern matching:
    // - Exact match
    // - Wildcard * matches one token
    // - Wildcard > matches one or more tokens

    let subject_parts: Vec<&str> = subject.split('.').collect();
    let pattern_parts: Vec<&str> = pattern.split('.').collect();

    let mut s_idx = 0;
    let mut p_idx = 0;

    while s_idx < subject_parts.len() && p_idx < pattern_parts.len() {
        let pattern_part = pattern_parts[p_idx];

        if pattern_part == ">" {
            // > matches the rest
            return true;
        } else if pattern_part == "*" {
            // * matches exactly one token
            s_idx += 1;
            p_idx += 1;
        } else if pattern_part == subject_parts[s_idx] {
            // Exact match
            s_idx += 1;
            p_idx += 1;
        } else {
            // No match
            return false;
        }
    }

    // Check if we consumed both entirely
    s_idx == subject_parts.len() && p_idx == pattern_parts.len()
}
