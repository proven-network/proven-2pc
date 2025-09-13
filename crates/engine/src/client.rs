//! Mock client that follows the production engine client API
//!
//! This module provides a client interface matching the production engine client,
//! allowing seamless testing of SQL and KV stream processors.

use crate::{
    Message, Result,
    engine::{ConsensusGroupId, GroupInfo, MockEngine, StreamInfo},
    stream::DeadlineStreamItem,
};
use proven_hlc::HlcTimestamp;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::Stream;

/// Mock client for interacting with the mock engine
#[derive(Clone)]
pub struct MockClient {
    /// Node ID
    node_id: String,

    /// Reference to the mock engine
    engine: Arc<MockEngine>,
}

impl MockClient {
    /// Create a new mock client
    pub fn new(node_id: String, engine: Arc<MockEngine>) -> Self {
        Self { node_id, engine }
    }

    /// Get the node ID of this client
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Create a new stream
    pub async fn create_group_stream(&self, name: String) -> Result<()> {
        self.engine.create_stream(name)
    }

    /// Write messages to a stream (goes through consensus)
    pub async fn publish_to_stream<M>(&self, stream_name: String, messages: Vec<M>) -> Result<u64>
    where
        M: Into<Message>,
    {
        let messages: Vec<Message> = messages.into_iter().map(Into::into).collect();
        self.engine.publish_to_stream(&stream_name, messages)
    }

    /// Stream messages from a stream
    pub async fn stream_messages(
        &self,
        stream_name: String,
        start_sequence: Option<u64>,
    ) -> Result<MessageStream> {
        let receiver = self.engine.stream_messages(&stream_name, start_sequence)?;
        Ok(MessageStream { receiver })
    }

    /// Stream messages from a stream until deadline is reached
    pub fn stream_messages_until_deadline(
        &self,
        stream_name: &str,
        start_sequence: Option<u64>,
        deadline: HlcTimestamp,
    ) -> Result<impl Stream<Item = DeadlineStreamItem>> {
        let start_offset = start_sequence.unwrap_or(1);
        self.engine
            .create_deadline_stream(stream_name, start_offset, deadline)
    }

    /// Publish messages to a subject
    pub async fn publish<M>(&self, subject: &str, messages: Vec<M>) -> Result<()>
    where
        M: Into<Message>,
    {
        let messages: Vec<Message> = messages.into_iter().map(Into::into).collect();
        self.engine.publish(subject, messages)
    }

    /// Subscribe to a subject pattern
    pub async fn subscribe(
        &self,
        subject_pattern: &str,
        _queue_group: Option<String>, // Ignored in mock for simplicity
    ) -> Result<PubSubMessageStream> {
        let receiver = self.engine.subscribe(subject_pattern);
        Ok(PubSubMessageStream { receiver })
    }

    /// Send a request and wait for a reply
    pub async fn request(
        &self,
        subject: &str,
        message: impl Into<Message>,
        timeout_ms: u64,
    ) -> Result<Message> {
        let message = message.into();
        self.engine.request(subject, message, timeout_ms).await
    }

    /// Check if there are any subscribers for a subject
    pub async fn has_responders(&self, subject: &str) -> Result<bool> {
        // In the mock, we'll just check if the stream exists for stream subjects
        // or if there are active subscriptions
        if subject.starts_with("stream.") {
            let stream_name = subject.strip_prefix("stream.").unwrap();
            Ok(self.engine.streams().stream_exists(stream_name))
        } else {
            // For simplicity, assume pub/sub subjects always have potential responders
            Ok(true)
        }
    }

    /// Get information about a stream including its group placement
    pub async fn get_stream_info(&self, stream_name: &str) -> Result<Option<StreamInfo>> {
        self.engine.get_stream_info(stream_name)
    }

    /// Get all consensus groups this node is a member of
    pub async fn node_groups(&self) -> Result<Vec<ConsensusGroupId>> {
        self.engine.node_groups(&self.node_id)
    }

    /// Get information about a specific consensus group
    pub async fn get_group_info(&self, group_id: ConsensusGroupId) -> Result<Option<GroupInfo>> {
        self.engine.get_group_info(group_id)
    }
}

/// Stream of messages from a subscription or stream consumer
pub struct MessageStream {
    receiver: mpsc::UnboundedReceiver<(Message, HlcTimestamp, u64)>,
}

impl MessageStream {
    /// Receive the next message with timestamp and log index
    pub async fn recv(&mut self) -> Option<(Message, HlcTimestamp, u64)> {
        self.receiver.recv().await
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<(Message, HlcTimestamp, u64)> {
        self.receiver.try_recv().ok()
    }
}

// Implement Stream trait for async iteration
impl futures::Stream for MessageStream {
    type Item = (Message, HlcTimestamp, u64);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

// Add futures dependency for Stream trait
use futures;

/// Stream of messages from pub/sub subscriptions (without timestamps/sequence)
pub struct PubSubMessageStream {
    receiver: mpsc::UnboundedReceiver<Message>,
}

impl PubSubMessageStream {
    /// Receive the next message
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<Message> {
        self.receiver.try_recv().ok()
    }
}

// Implement Stream trait for async iteration
impl futures::Stream for PubSubMessageStream {
    type Item = Message;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
