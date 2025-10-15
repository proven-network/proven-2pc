//! Storage layer for resource data

pub mod entity;
pub mod lock_persistence;
mod reservation;

pub use entity::{ResourceDelta, ResourceEntity, ResourceKey, ResourceValue};
pub use lock_persistence::{
    TransactionReservations, decode_transaction_reservations, encode_transaction_reservations,
};
pub use reservation::{BalanceReservation, ReservationManager, ReservationType};

// Re-export ResourceMetadata (moved from old mvcc.rs)
use serde::{Deserialize, Serialize};

/// Resource metadata
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u32,
    pub initialized: bool,
}
