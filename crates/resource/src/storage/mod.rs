//! Storage layer for resource data

mod mvcc;
mod reservation;

pub use mvcc::{CompactedResourceData, ResourceMetadata, ResourceStorage};
pub use reservation::{BalanceReservation, ReservationManager, ReservationType};
