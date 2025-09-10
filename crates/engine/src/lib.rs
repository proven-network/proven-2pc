//! In-memory mock engine for testing distributed transactions
//!
//! This module provides an in-memory implementation of the production engine client API,
//! allowing testing of distributed transactions across SQL and KV systems.

use thiserror::Error;

pub mod client;
pub mod engine;
pub mod message;
pub mod stream;

pub use client::MockClient;
pub use engine::{ConsensusGroupId, GroupInfo, MockEngine, StreamInfo, StreamPlacement};
pub use message::Message;
pub use stream::DeadlineStreamItem;

/// Mock engine errors
#[derive(Debug, Error)]
pub enum MockEngineError {
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    #[error("No subscribers for subject: {0}")]
    NoSubscribers(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

pub type Result<T> = std::result::Result<T, MockEngineError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_stream_operations() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Create a stream
        client
            .create_group_stream("test-stream".to_string())
            .await
            .unwrap();

        // Publish messages
        let messages = vec![
            Message::with_body(b"message1".to_vec()),
            Message::with_body(b"message2".to_vec()),
        ];

        let sequence = client
            .publish_to_stream("test-stream".to_string(), messages)
            .await
            .unwrap();
        assert_eq!(sequence, 1);

        // Stream messages
        let mut stream = client
            .stream_messages("test-stream".to_string(), Some(1))
            .await
            .unwrap();

        let (msg1, _, seq1) = stream.recv().await.unwrap();
        assert_eq!(msg1.body, b"message1");
        assert_eq!(seq1, 1);

        let (msg2, _, seq2) = stream.recv().await.unwrap();
        assert_eq!(msg2.body, b"message2");
        assert_eq!(seq2, 2);
    }

    #[tokio::test]
    async fn test_pub_sub() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Subscribe to a subject
        let mut sub = client.subscribe("test.subject", None).await.unwrap();

        // Publish a message
        let message = Message::with_body(b"hello".to_vec());
        client.publish("test.subject", vec![message]).await.unwrap();

        // Receive the message
        let received = sub.recv().await.unwrap();
        assert_eq!(received.body, b"hello");
    }

    #[tokio::test]
    async fn test_request_reply() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Set up a responder
        tokio::spawn({
            let engine = engine.clone();
            async move {
                let mut handler = engine.register_handler("echo");
                while let Some((msg, reply_tx)) = handler.recv().await {
                    // Echo the message back
                    let reply = Message::with_body(msg.body.clone());
                    let _ = reply_tx.send(reply);
                }
            }
        });

        // Small delay to ensure handler is registered
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Send a request
        let request = Message::with_body(b"ping".to_vec());
        let reply = client.request("echo", request, 1000).await.unwrap();

        assert_eq!(reply.body, b"ping");
    }

    #[tokio::test]
    async fn test_wildcard_subscriptions() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Subscribe to wildcard pattern
        let mut sub = client.subscribe("test.*", None).await.unwrap();

        // Publish to matching subjects
        let msg1 = Message::with_body(b"foo".to_vec());
        client.publish("test.foo", vec![msg1]).await.unwrap();

        let msg2 = Message::with_body(b"bar".to_vec());
        client.publish("test.bar", vec![msg2]).await.unwrap();

        // Should not match
        let msg3 = Message::with_body(b"baz".to_vec());
        client.publish("other.baz", vec![msg3]).await.unwrap();

        // Receive matching messages
        let received1 = sub.recv().await.unwrap();
        assert_eq!(received1.body, b"foo");

        let received2 = sub.recv().await.unwrap();
        assert_eq!(received2.body, b"bar");

        // Should not receive the non-matching message
        assert!(sub.try_recv().is_none());
    }

    #[tokio::test]
    async fn test_deadline_read() {
        use proven_hlc::{HlcTimestamp, NodeId};
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::pin;
        use tokio_stream::StreamExt;

        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Create a stream
        client
            .create_group_stream("deadline-stream".to_string())
            .await
            .unwrap();

        // Publish some messages
        let msg1 = Message::with_body(b"message1".to_vec());
        let msg2 = Message::with_body(b"message2".to_vec());
        let msg3 = Message::with_body(b"message3".to_vec());

        client
            .publish_to_stream("deadline-stream".to_string(), vec![msg1, msg2, msg3])
            .await
            .unwrap();

        // Set a deadline in the future (so we don't pass it)
        let future_deadline = HlcTimestamp::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64
                + 1_000_000, // 1 second in future
            0,
            NodeId::new(1),
        );

        // Read all messages until deadline
        let stream = client
            .stream_messages_until_deadline("deadline-stream", Some(1), future_deadline)
            .unwrap();
        pin!(stream);

        let mut messages = Vec::new();
        let mut deadline_reached = false;

        while let Some(item) = stream.next().await {
            match item {
                DeadlineStreamItem::Message(msg, _, _) => messages.push(msg),
                DeadlineStreamItem::DeadlineReached => {
                    deadline_reached = true;
                    break;
                }
            }
        }

        // Should have all 3 messages, no deadline yet
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].body, b"message1");
        assert_eq!(messages[1].body, b"message2");
        assert_eq!(messages[2].body, b"message3");
        assert!(
            !deadline_reached,
            "Should not reach deadline - it's in the future"
        );

        // Now test with a past deadline
        let past_deadline = HlcTimestamp::new(1000, 0, NodeId::new(1));

        let stream = client
            .stream_messages_until_deadline("deadline-stream", Some(1), past_deadline)
            .unwrap();
        pin!(stream);

        let mut messages = Vec::new();
        let mut deadline_reached = false;

        while let Some(item) = stream.next().await {
            match item {
                DeadlineStreamItem::Message(msg, _, _) => messages.push(msg),
                DeadlineStreamItem::DeadlineReached => {
                    deadline_reached = true;
                    break;
                }
            }
        }

        assert!(deadline_reached, "Should reach deadline - it's in the past");
        assert_eq!(messages.len(), 0); // No messages before timestamp 1000
    }

    #[tokio::test]
    async fn test_timestamp_monotonicity() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Create a stream
        client
            .create_group_stream("monotonic-stream".to_string())
            .await
            .unwrap();

        // Publish multiple messages and verify timestamps are monotonic
        for i in 0..10 {
            let msg = Message::with_body(format!("message-{}", i).into_bytes());
            client
                .publish_to_stream("monotonic-stream".to_string(), vec![msg])
                .await
                .unwrap();
        }

        // Read all messages and verify monotonicity
        let mut stream = client
            .stream_messages("monotonic-stream".to_string(), Some(1))
            .await
            .unwrap();

        let mut last_timestamp = None;
        for _ in 0..10 {
            let (_msg, timestamp, _) = stream.recv().await.unwrap();

            if let Some(last) = last_timestamp {
                assert!(timestamp > last, "Timestamps must be strictly monotonic");
            }
            last_timestamp = Some(timestamp);
        }
    }

    #[tokio::test]
    async fn test_stream_with_headers() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Create a stream
        client
            .create_group_stream("txn-stream".to_string())
            .await
            .unwrap();

        // Create message with headers
        let mut headers = HashMap::new();
        headers.insert("txn_id".to_string(), "txn123".to_string());
        headers.insert("coordinator_id".to_string(), "coord456".to_string());

        let message = Message::new(b"operation".to_vec(), headers);

        // Publish and receive
        client
            .publish_to_stream("txn-stream".to_string(), vec![message])
            .await
            .unwrap();

        let mut stream = client
            .stream_messages("txn-stream".to_string(), Some(1))
            .await
            .unwrap();

        let (received, _, _) = stream.recv().await.unwrap();
        assert_eq!(received.get_header("txn_id"), Some("txn123"));
        assert_eq!(received.get_header("coordinator_id"), Some("coord456"));
    }
}
