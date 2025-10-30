//! Execution strategies for different transaction modes

pub mod adhoc;
pub mod read_only;

use crate::engine::TransactionMode;
use proven_common::Timestamp;
use proven_engine::Message;

/// Extract transaction mode from a message
pub fn get_transaction_mode(message: &Message) -> TransactionMode {
    match message.get_header("txn_mode") {
        Some("read_only") => TransactionMode::ReadOnly,
        Some("adhoc") => TransactionMode::AdHoc,
        _ => TransactionMode::ReadWrite,
    }
}

/// Extract read timestamp for read-only transactions
pub fn get_read_timestamp(message: &Message, fallback: Timestamp) -> Timestamp {
    message
        .get_header("read_timestamp")
        .and_then(|s| Timestamp::parse(s).ok())
        .unwrap_or(fallback)
}

/// Extract operation timestamp for ad-hoc transactions
pub fn get_operation_timestamp(message: &Message, fallback: Timestamp) -> Timestamp {
    message
        .get_header("operation_timestamp")
        .and_then(|s| Timestamp::parse(s).ok())
        .unwrap_or(fallback)
}
