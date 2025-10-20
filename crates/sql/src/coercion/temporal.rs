//! Temporal type coercions (Date, Time, Timestamp, Interval)

use crate::error::{Error, Result};
use crate::types::Value;

/// Parse string to Date
pub fn parse_string_to_date(s: &str) -> Result<Value> {
    use chrono::NaiveDate;
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(Value::Date)
        .map_err(|_| Error::TypeMismatch {
            expected: "DATE".into(),
            found: format!("Invalid date string: '{}'", s),
        })
}

/// Parse string to Time
pub fn parse_string_to_time(s: &str) -> Result<Value> {
    use chrono::NaiveTime;
    // Try multiple time formats
    NaiveTime::parse_from_str(s, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
        .map(Value::Time)
        .map_err(|_| Error::TypeMismatch {
            expected: "TIME".into(),
            found: format!("Invalid time string: '{}'", s),
        })
}

/// Parse string to Timestamp
pub fn parse_string_to_timestamp(s: &str) -> Result<Value> {
    use chrono::{NaiveDate, NaiveDateTime};
    // Try multiple timestamp formats
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| {
            // Try date-only format (add 00:00:00 time)
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        })
        .map(Value::Timestamp)
        .map_err(|_| Error::TypeMismatch {
            expected: "TIMESTAMP".into(),
            found: format!("Invalid timestamp string: '{}'", s),
        })
}

/// Parse string to Interval
/// Supports formats like "1 day", "2 hours", "3 months", "4 years", etc.
pub fn parse_string_to_interval(s: &str) -> Result<Value> {
    use proven_value::Interval;

    let s = s.trim();

    // Split into number and unit
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return Err(Error::InvalidValue(format!(
            "Invalid interval format: '{}'. Expected format: '<number> <unit>'",
            s
        )));
    }

    let value: i64 = parts[0]
        .parse()
        .map_err(|_| Error::InvalidValue(format!("Invalid interval value: '{}'", parts[0])))?;

    let unit = parts[1].to_lowercase();

    let interval = match unit.as_str() {
        "microsecond" | "microseconds" | "us" => Interval {
            months: 0,
            days: 0,
            microseconds: value,
        },
        "millisecond" | "milliseconds" | "ms" => Interval {
            months: 0,
            days: 0,
            microseconds: value * 1000,
        },
        "second" | "seconds" | "s" | "sec" | "secs" => Interval {
            months: 0,
            days: 0,
            microseconds: value * 1_000_000,
        },
        "minute" | "minutes" | "min" | "mins" => Interval {
            months: 0,
            days: 0,
            microseconds: value * 60_000_000,
        },
        "hour" | "hours" | "h" | "hr" | "hrs" => Interval {
            months: 0,
            days: 0,
            microseconds: value * 3_600_000_000,
        },
        "day" | "days" | "d" => Interval {
            months: 0,
            days: value as i32,
            microseconds: 0,
        },
        "week" | "weeks" | "w" => Interval {
            months: 0,
            days: (value * 7) as i32,
            microseconds: 0,
        },
        "month" | "months" | "mon" | "mons" => Interval {
            months: value as i32,
            days: 0,
            microseconds: 0,
        },
        "year" | "years" | "y" | "yr" | "yrs" => Interval {
            months: (value * 12) as i32,
            days: 0,
            microseconds: 0,
        },
        _ => {
            return Err(Error::InvalidValue(format!(
                "Unknown interval unit: '{}'. Supported units: microsecond, millisecond, second, minute, hour, day, week, month, year",
                unit
            )));
        }
    };

    Ok(Value::Interval(interval))
}
