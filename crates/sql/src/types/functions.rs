//! Deterministic SQL functions that use transaction context
//!
//! All SQL functions must be deterministic for consensus - they must produce
//! the same output given the same inputs and transaction context.

use crate::error::{Error, Result};
use crate::stream::transaction::TransactionContext;
use crate::types::value::Value;

/// Evaluate a SQL function call
pub fn evaluate_function(
    name: &str,
    args: &[Value],
    context: &TransactionContext,
) -> Result<Value> {
    match name.to_uppercase().as_str() {
        // Time functions - use transaction timestamp for determinism
        "NOW" | "CURRENT_TIMESTAMP" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Convert HLC timestamp to SQL timestamp (microseconds since epoch)
            // The physical component is already in microseconds
            Ok(Value::Timestamp(context.timestamp().physical))
        }

        "CURRENT_TIME" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Extract time of day from timestamp
            // This is simplified - in a real system we'd handle timezones
            let micros = context.timestamp().physical;
            let time_of_day = micros % (24 * 60 * 60 * 1_000_000);
            Ok(Value::Timestamp(time_of_day))
        }

        "CURRENT_DATE" => {
            if !args.is_empty() {
                return Err(Error::ExecutionError(format!(
                    "{} takes no arguments",
                    name
                )));
            }
            // Extract date from timestamp (truncate to day boundary)
            let micros = context.timestamp().physical;
            let day_micros = 24 * 60 * 60 * 1_000_000;
            let date = (micros / day_micros) * day_micros;
            Ok(Value::Timestamp(date))
        }

        // UUID generation - deterministic based on transaction ID
        "GEN_UUID" | "UUID" => {
            if args.len() > 1 {
                return Err(Error::ExecutionError(format!(
                    "{} takes at most 1 argument",
                    name
                )));
            }

            // Use provided sequence or default to 0
            let sequence = match args.first() {
                Some(Value::Integer(i)) => *i as u64,
                Some(_) => {
                    return Err(Error::ExecutionError(
                        "UUID sequence must be an integer".into(),
                    ));
                }
                None => 0,
            };

            Ok(Value::Uuid(context.deterministic_uuid(sequence)))
        }

        // Math functions (already deterministic)
        "ABS" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError("ABS takes exactly 1 argument".into()));
            }
            match &args[0] {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::Decimal(d) => Ok(Value::Decimal(d.abs())),
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "ROUND" => {
            if args.is_empty() || args.len() > 2 {
                return Err(Error::ExecutionError("ROUND takes 1 or 2 arguments".into()));
            }

            let precision = if args.len() == 2 {
                match &args[1] {
                    Value::Integer(i) => *i as i32,
                    _ => {
                        return Err(Error::ExecutionError(
                            "ROUND precision must be an integer".into(),
                        ));
                    }
                }
            } else {
                0
            };

            match &args[0] {
                Value::Decimal(d) => {
                    let rounded = d.round_dp(precision as u32);
                    Ok(Value::Decimal(rounded))
                }
                Value::Integer(i) => Ok(Value::Integer(*i)), // Already rounded
                _ => Err(Error::TypeMismatch {
                    expected: "numeric".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        // String functions (deterministic)
        "UPPER" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "UPPER takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_uppercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "LOWER" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "LOWER takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_lowercase())),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        "LENGTH" => {
            if args.len() != 1 {
                return Err(Error::ExecutionError(
                    "LENGTH takes exactly 1 argument".into(),
                ));
            }
            match &args[0] {
                Value::String(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Blob(b) => Ok(Value::Integer(b.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => Err(Error::TypeMismatch {
                    expected: "string or blob".into(),
                    found: args[0].data_type().to_string(),
                }),
            }
        }

        _ => Err(Error::ExecutionError(format!("Unknown function: {}", name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_deterministic_now() {
        let timestamp = HlcTimestamp::new(1_000_000_000, 0, NodeId::new(1));
        let context = TransactionContext::new(timestamp);

        // NOW() should always return the same value within a transaction
        let now1 = evaluate_function("NOW", &[], &context).unwrap();
        let now2 = evaluate_function("CURRENT_TIMESTAMP", &[], &context).unwrap();

        assert_eq!(now1, now2);
        assert_eq!(now1, Value::Timestamp(1_000_000_000));
    }

    #[test]
    fn test_deterministic_uuid() {
        let timestamp = HlcTimestamp::new(1_000_000_000, 0, NodeId::new(1));
        let context = TransactionContext::new(timestamp);

        // UUIDs with same sequence should be identical
        let uuid1 = evaluate_function("UUID", &[], &context).unwrap();
        let uuid2 = evaluate_function("UUID", &[], &context).unwrap();
        assert_eq!(uuid1, uuid2);

        // Different sequences should produce different UUIDs
        let uuid3 = evaluate_function("UUID", &[Value::Integer(1)], &context).unwrap();
        assert_ne!(uuid1, uuid3);
    }
}
