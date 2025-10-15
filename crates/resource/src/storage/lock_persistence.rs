//! Reservation persistence for crash recovery
//!
//! Similar to KV's lock persistence, we persist reservations so they can be
//! restored after a crash and properly released on commit/abort.
//!
//! IMPORTANT: Persisted reservations are ONLY read during recovery (startup scan).
//! During normal operation, all reservation operations use the in-memory ReservationManager.

use super::reservation::ReservationType;
use proven_hlc::HlcTimestamp;
use serde::{Deserialize, Serialize};

/// Persisted reservation information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedReservation {
    pub account: String,
    pub reservation_type: ReservationType,
}

/// Reservation state for a transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionReservations {
    pub txn_id: HlcTimestamp,
    pub reservations: Vec<PersistedReservation>,
}

impl TransactionReservations {
    pub fn new(txn_id: HlcTimestamp) -> Self {
        Self {
            txn_id,
            reservations: Vec::new(),
        }
    }

    pub fn add_reservation(&mut self, account: String, reservation_type: ReservationType) {
        self.reservations.push(PersistedReservation {
            account,
            reservation_type,
        });
    }
}

/// Encode transaction reservations for persistence (using bincode for efficiency)
pub fn encode_transaction_reservations(
    reservations: &TransactionReservations,
) -> Result<Vec<u8>, String> {
    bincode::serialize(reservations).map_err(|e| e.to_string())
}

/// Decode transaction reservations from persistence
pub fn decode_transaction_reservations(bytes: &[u8]) -> Result<TransactionReservations, String> {
    bincode::deserialize(bytes).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Amount;
    use proven_hlc::NodeId;

    fn make_timestamp(n: u64) -> HlcTimestamp {
        HlcTimestamp::new(n, 0, NodeId::new(0))
    }

    #[test]
    fn test_encode_decode_reservations() {
        let mut tx_res = TransactionReservations::new(make_timestamp(100));
        tx_res.add_reservation(
            "alice".to_string(),
            ReservationType::Debit(Amount::from_integer(50, 0)),
        );
        tx_res.add_reservation(
            "bob".to_string(),
            ReservationType::Credit(Amount::from_integer(50, 0)),
        );
        tx_res.add_reservation("".to_string(), ReservationType::MetadataUpdate);

        let encoded = encode_transaction_reservations(&tx_res).unwrap();
        let decoded = decode_transaction_reservations(&encoded).unwrap();

        assert_eq!(tx_res.txn_id, decoded.txn_id);
        assert_eq!(tx_res.reservations.len(), decoded.reservations.len());
    }

    #[test]
    fn test_encode_decode_all_reservation_types() {
        let types = vec![
            ReservationType::Debit(Amount::from_integer(100, 0)),
            ReservationType::Credit(Amount::from_integer(50, 0)),
            ReservationType::MetadataUpdate,
        ];

        for res_type in types {
            let mut tx_res = TransactionReservations::new(make_timestamp(100));
            tx_res.add_reservation("test".to_string(), res_type.clone());

            let encoded = encode_transaction_reservations(&tx_res).unwrap();
            let decoded = decode_transaction_reservations(&encoded).unwrap();

            assert_eq!(decoded.reservations.len(), 1);
            assert_eq!(decoded.reservations[0].reservation_type, res_type);
        }
    }
}
