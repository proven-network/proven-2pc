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

/// Encode transaction reservations for persistence
pub fn encode_transaction_reservations(
    reservations: &TransactionReservations,
) -> Result<Vec<u8>, String> {
    use std::io::Write;
    let mut buf = Vec::new();

    // Encode HlcTimestamp (20 bytes)
    buf.write_all(&reservations.txn_id.to_lexicographic_bytes())
        .map_err(|e| e.to_string())?;

    // Encode number of reservations
    buf.write_all(&(reservations.reservations.len() as u32).to_be_bytes())
        .map_err(|e| e.to_string())?;

    // Encode each reservation
    for reservation in &reservations.reservations {
        // Encode account
        let account_bytes = reservation.account.as_bytes();
        buf.write_all(&(account_bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        buf.write_all(account_bytes).map_err(|e| e.to_string())?;

        // Encode reservation type (1 byte tag + data)
        match &reservation.reservation_type {
            ReservationType::Debit(amount) => {
                buf.push(1); // Tag for Debit
                encode_amount_to_buf(*amount, &mut buf)?;
            }
            ReservationType::Credit(amount) => {
                buf.push(2); // Tag for Credit
                encode_amount_to_buf(*amount, &mut buf)?;
            }
            ReservationType::MetadataUpdate => {
                buf.push(3); // Tag for MetadataUpdate
            }
        }
    }

    Ok(buf)
}

/// Decode transaction reservations from persistence
pub fn decode_transaction_reservations(bytes: &[u8]) -> Result<TransactionReservations, String> {
    use std::io::{Cursor, Read};

    let mut cursor = Cursor::new(bytes);

    // Decode HlcTimestamp (20 bytes)
    let mut ts_bytes = [0u8; 20];
    cursor
        .read_exact(&mut ts_bytes)
        .map_err(|e| e.to_string())?;
    let txn_id = HlcTimestamp::from_lexicographic_bytes(&ts_bytes).map_err(|e| e.to_string())?;

    // Decode number of reservations
    let mut len_bytes = [0u8; 4];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| e.to_string())?;
    let num_reservations = u32::from_be_bytes(len_bytes) as usize;

    let mut reservations = Vec::with_capacity(num_reservations);

    // Decode each reservation
    for _ in 0..num_reservations {
        // Decode account
        cursor
            .read_exact(&mut len_bytes)
            .map_err(|e| e.to_string())?;
        let account_len = u32::from_be_bytes(len_bytes) as usize;

        let mut account_bytes = vec![0u8; account_len];
        cursor
            .read_exact(&mut account_bytes)
            .map_err(|e| e.to_string())?;
        let account =
            String::from_utf8(account_bytes).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        // Decode reservation type
        let mut type_byte = [0u8; 1];
        cursor
            .read_exact(&mut type_byte)
            .map_err(|e| e.to_string())?;

        let reservation_type = match type_byte[0] {
            1 => {
                let amount = decode_amount_from_cursor(&mut cursor)?;
                ReservationType::Debit(amount)
            }
            2 => {
                let amount = decode_amount_from_cursor(&mut cursor)?;
                ReservationType::Credit(amount)
            }
            3 => ReservationType::MetadataUpdate,
            _ => return Err(format!("Unknown reservation type: {}", type_byte[0])),
        };

        reservations.push(PersistedReservation {
            account,
            reservation_type,
        });
    }

    Ok(TransactionReservations {
        txn_id,
        reservations,
    })
}

// Helper functions for encoding/decoding Amount
use crate::types::Amount;
use rust_decimal::Decimal;
use std::str::FromStr;

fn encode_amount_to_buf(amount: Amount, buf: &mut Vec<u8>) -> Result<(), String> {
    use std::io::Write;
    // Encode Decimal as string (preserves precision)
    let decimal_str = amount.0.to_string();
    let bytes = decimal_str.as_bytes();
    buf.write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|e| e.to_string())?;
    buf.write_all(bytes).map_err(|e| e.to_string())?;
    Ok(())
}

fn decode_amount_from_cursor(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Amount, String> {
    use std::io::Read;
    let mut len_bytes = [0u8; 4];
    cursor
        .read_exact(&mut len_bytes)
        .map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut decimal_bytes = vec![0u8; len];
    cursor
        .read_exact(&mut decimal_bytes)
        .map_err(|e| e.to_string())?;

    let decimal_str =
        String::from_utf8(decimal_bytes).map_err(|e| format!("Invalid UTF-8: {}", e))?;

    let decimal = Decimal::from_str(&decimal_str).map_err(|e| format!("Invalid decimal: {}", e))?;

    Ok(Amount::new(decimal))
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
