//! Stream management for the mock engine
//!
//! This module handles ordered message streams with persistent storage
//! and consumer tracking.

use crate::{Message, MockEngineError, Result};
use parking_lot::Mutex;
use proven_hlc::{HlcTimestamp, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::Stream as TokioStream;

/// A persistent stream that stores messages
pub struct Stream {
    /// Stream name
    #[allow(dead_code)]
    name: String,

    /// All messages in the stream with their timestamps and sequences
    messages: Vec<(Message, HlcTimestamp, u64)>,

    /// Next sequence number
    next_sequence: u64,

    /// Active consumers
    consumers: Vec<StreamConsumer>,

    /// Last assigned timestamp (for monotonicity)
    last_timestamp: Option<HlcTimestamp>,

    /// Node ID for this stream
    node_id: NodeId,
}

impl Stream {
    /// Create a new stream
    pub fn new(name: String) -> Self {
        // Create a unique node ID based on stream name
        let node_id = NodeId::new(
            name.bytes()
                .fold(1u64, |acc: u64, b| acc.wrapping_add(b as u64)),
        );

        Self {
            name: name.clone(),
            messages: Vec::new(),
            next_sequence: 1,
            consumers: Vec::new(),
            last_timestamp: None,
            node_id,
        }
    }

    /// Append messages to the stream
    pub fn append(&mut self, messages: Vec<Message>) -> u64 {
        let start_sequence = self.next_sequence;

        // Track timestamps and sequences for each message
        let mut message_metadata = Vec::new();

        for msg in &messages {
            // Get current physical time in microseconds
            let physical = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            let now = HlcTimestamp::new(physical, 0, self.node_id);

            // Ensure monotonic increase
            let timestamp = match self.last_timestamp {
                Some(last) if last >= now => {
                    // Need to increment to maintain monotonicity
                    HlcTimestamp::new(last.physical, last.logical + 1, last.node_id)
                }
                _ => now,
            };

            message_metadata.push((msg.clone(), timestamp, self.next_sequence));
            self.next_sequence += 1;
            self.last_timestamp = Some(timestamp);
        }

        // Send to all active consumers
        for consumer in &self.consumers {
            for (msg, timestamp, sequence) in &message_metadata {
                // Send tuple to each consumer (ignore if channel is closed)
                let _ = consumer
                    .sender
                    .try_send((msg.clone(), *timestamp, *sequence));
            }
        }

        // Store messages with metadata
        self.messages.extend(message_metadata);

        start_sequence
    }

    /// Create a new consumer starting from a specific sequence
    pub fn create_consumer(
        &mut self,
        start_sequence: Option<u64>,
    ) -> mpsc::Receiver<(Message, HlcTimestamp, u64)> {
        let (tx, rx) = mpsc::channel(1000);

        // Send historical messages
        let start = start_sequence.unwrap_or(1) as usize;
        if start > 0 && start <= self.messages.len() {
            for (msg, timestamp, sequence) in &self.messages[start - 1..] {
                let _ = tx.try_send((msg.clone(), *timestamp, *sequence));
            }
        }

        // Add to active consumers
        self.consumers.push(StreamConsumer {
            sender: tx,
            current_sequence: start_sequence.unwrap_or(self.next_sequence),
        });

        rx
    }

    /// Clean up closed consumers
    pub fn cleanup_consumers(&mut self) {
        self.consumers.retain(|c| !c.sender.is_closed());
    }

    /// Get messages from a specific offset (for deadline reads)
    pub fn get_messages_from(&self, start_offset: u64) -> Vec<(Message, HlcTimestamp, u64)> {
        let start_idx = if start_offset > 0 {
            (start_offset - 1) as usize
        } else {
            0
        };
        self.messages[start_idx..].to_vec()
    }

    /// Check if current time is past deadline
    pub fn is_past_deadline(&self, deadline: HlcTimestamp) -> bool {
        let physical = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let current_time = HlcTimestamp::new(physical, 0, self.node_id);
        current_time > deadline
    }
}

/// Items yielded by deadline-aware stream reader
#[derive(Debug, Clone)]
pub enum DeadlineStreamItem {
    /// A message from the stream with timestamp and log index
    Message(Message, HlcTimestamp, u64),
    /// Deadline has been reached
    DeadlineReached,
}

/// An active stream consumer
struct StreamConsumer {
    sender: mpsc::Sender<(Message, HlcTimestamp, u64)>,
    #[allow(dead_code)]
    current_sequence: u64,
}

/// Stream manager that handles multiple streams
pub struct StreamManager {
    streams: Arc<Mutex<HashMap<String, Stream>>>,
}

impl StreamManager {
    /// Create a new stream manager
    pub fn new() -> Self {
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new stream
    pub fn create_stream(&self, name: String) -> Result<()> {
        let mut streams = self.streams.lock();
        if streams.contains_key(&name) {
            return Err(MockEngineError::InvalidOperation(format!(
                "Stream '{}' already exists",
                name
            )));
        }
        streams.insert(name.clone(), Stream::new(name));
        Ok(())
    }

    /// Append messages to a stream
    pub fn append_to_stream(&self, name: &str, messages: Vec<Message>) -> Result<u64> {
        let mut streams = self.streams.lock();
        let stream = streams
            .get_mut(name)
            .ok_or_else(|| MockEngineError::StreamNotFound(name.to_string()))?;

        let sequence = stream.append(messages);
        stream.cleanup_consumers();
        Ok(sequence)
    }

    /// Create a consumer for a stream
    pub fn create_consumer(
        &self,
        name: &str,
        start_sequence: Option<u64>,
    ) -> Result<mpsc::Receiver<(Message, HlcTimestamp, u64)>> {
        let mut streams = self.streams.lock();
        let stream = streams
            .get_mut(name)
            .ok_or_else(|| MockEngineError::StreamNotFound(name.to_string()))?;

        Ok(stream.create_consumer(start_sequence))
    }

    /// Check if a stream exists
    pub fn stream_exists(&self, name: &str) -> bool {
        self.streams.lock().contains_key(name)
    }

    /// Create a stream that reads messages until deadline
    pub fn create_deadline_stream(
        &self,
        stream_name: &str,
        start_offset: u64,
        deadline: HlcTimestamp,
    ) -> Result<impl TokioStream<Item = DeadlineStreamItem>> {
        // Extract necessary data while holding the lock
        let (messages, is_past_deadline) = {
            let streams = self.streams.lock();
            let stream = streams
                .get(stream_name)
                .ok_or_else(|| MockEngineError::StreamNotFound(stream_name.to_string()))?;

            (
                stream.get_messages_from(start_offset),
                stream.is_past_deadline(deadline),
            )
        };

        // Create the stream without holding the lock
        Ok(async_stream::stream! {
            for (msg, ts, sequence) in messages {
                // Check if we've passed deadline
                if ts > deadline {
                    yield DeadlineStreamItem::DeadlineReached;
                    return;
                }

                yield DeadlineStreamItem::Message(msg, ts, sequence);
            }

            // Check if current time is past deadline
            if is_past_deadline {
                yield DeadlineStreamItem::DeadlineReached;
            }
        })
    }
}
