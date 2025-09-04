//! Stream management for the mock engine
//!
//! This module handles ordered message streams with persistent storage
//! and consumer tracking.

use crate::{Message, MockEngineError, Result};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// A persistent stream that stores messages
pub struct Stream {
    /// Stream name
    #[allow(dead_code)]
    name: String,

    /// All messages in the stream
    messages: Vec<Message>,

    /// Next sequence number
    next_sequence: u64,

    /// Active consumers
    consumers: Vec<StreamConsumer>,
}

impl Stream {
    /// Create a new stream
    pub fn new(name: String) -> Self {
        Self {
            name,
            messages: Vec::new(),
            next_sequence: 1,
            consumers: Vec::new(),
        }
    }

    /// Append messages to the stream
    pub fn append(&mut self, mut messages: Vec<Message>) -> u64 {
        let start_sequence = self.next_sequence;

        // Assign sequences and timestamps
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        for msg in &mut messages {
            msg.sequence = Some(self.next_sequence);
            msg.timestamp = Some(timestamp);
            self.next_sequence += 1;
        }

        // Send to all active consumers
        for consumer in &self.consumers {
            for msg in &messages {
                // Send a clone to each consumer (ignore if channel is closed)
                let _ = consumer.sender.try_send(msg.clone());
            }
        }

        // Store messages
        self.messages.extend(messages);

        start_sequence
    }

    /// Create a new consumer starting from a specific sequence
    pub fn create_consumer(&mut self, start_sequence: Option<u64>) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel(1000);

        // Send historical messages
        let start = start_sequence.unwrap_or(1) as usize;
        if start > 0 && start <= self.messages.len() {
            for msg in &self.messages[start - 1..] {
                let _ = tx.try_send(msg.clone());
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
}

/// An active stream consumer
struct StreamConsumer {
    sender: mpsc::Sender<Message>,
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
    ) -> Result<mpsc::Receiver<Message>> {
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
}
