//! Protocol definitions for coordinator-stream communication
//!
//! This crate defines typed message wrappers around the generic `Message` type
//! from proven-engine. It provides type safety for the distributed transaction
//! protocol without modifying the core engine types.

pub mod messages;
pub mod responses;

pub use messages::{
    CoordinatorMessage, OperationMessage, TransactionControlMessage, TransactionPhase,
};
pub use responses::{ParticipantResponse, ResponseBuilder, ResponseStatus};
