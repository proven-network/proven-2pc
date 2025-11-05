//! Cluster runtime for managing stream processors and node coordination

mod cluster_view;
mod error;
mod heartbeat;
mod messages;
mod orchestration;
mod processor;
mod runner;

pub use error::{Result, RunnerError};
pub use messages::{Heartbeat, ProcessorAck, ProcessorRequest, ProcessorState, ProcessorStatus};
pub use runner::{ProcessorInfo, Runner};

// Re-export ProcessorType from common for convenience
pub use proven_common::ProcessorType;
