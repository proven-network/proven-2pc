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
pub use engine::MockEngine;
pub use message::Message;

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
            .create_stream("test-stream".to_string())
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

        let msg1 = stream.recv().await.unwrap();
        assert_eq!(msg1.body, b"message1");
        assert_eq!(msg1.sequence, Some(1));

        let msg2 = stream.recv().await.unwrap();
        assert_eq!(msg2.body, b"message2");
        assert_eq!(msg2.sequence, Some(2));
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
    async fn test_stream_with_headers() {
        let engine = Arc::new(MockEngine::new());
        let client = MockClient::new("test-node".to_string(), engine.clone());

        // Create a stream
        client
            .create_stream("txn-stream".to_string())
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

        let received = stream.recv().await.unwrap();
        assert_eq!(received.get_header("txn_id"), Some("txn123"));
        assert_eq!(received.get_header("coordinator_id"), Some("coord456"));
    }
}
