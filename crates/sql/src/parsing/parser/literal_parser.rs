//! Literal parser module
//!
//! Handles parsing of SQL literal values including dates, times, intervals,
//! numeric values, strings, and boolean values.

use super::super::{Keyword, Token};
use super::token_helper::TokenHelper;
use crate::error::{Error, Result};
use crate::parsing::ast::{Expression, Literal};

/// Parser trait for literal values
pub trait LiteralParser: TokenHelper {
    /// Parse a literal value from a token
    fn parse_literal(&mut self, token: Token) -> Result<Option<Expression>> {
        Ok(match token {
            // Numeric literals
            Token::Number(n) if n.chars().all(|c| c.is_ascii_digit()) => {
                // Try to parse as i128 first, then as u128 if that fails
                match n.parse::<i128>() {
                    Ok(val) => Some(Literal::Integer(val).into()),
                    Err(_) => {
                        // Try parsing as u128 for large unsigned values
                        match n.parse::<u128>() {
                            Ok(val) => {
                                // We need to handle u128 values that are > i128::MAX
                                if val <= i128::MAX as u128 {
                                    Some(Literal::Integer(val as i128).into())
                                } else {
                                    // For values > i128::MAX, use Float to preserve the value
                                    Some(Literal::Float(val as f64).into())
                                }
                            }
                            Err(e) => {
                                return Err(Error::ParseError(format!("invalid integer: {}", e)));
                            }
                        }
                    }
                }
            }
            Token::Number(n) => Some(
                Literal::Float(
                    n.parse()
                        .map_err(|e| Error::ParseError(format!("invalid float: {}", e)))?,
                )
                .into(),
            ),

            // String literals
            Token::String(s) => Some(Literal::String(s).into()),
            Token::HexString(h) => {
                // Convert hex string to bytes
                match hex::decode(&h) {
                    Ok(bytes) => Some(Literal::Bytea(bytes).into()),
                    Err(_) => return Err(Error::ParseError(format!("Invalid hex string: {}", h))),
                }
            }

            // Boolean literals
            Token::Keyword(Keyword::True) => Some(Literal::Boolean(true).into()),
            Token::Keyword(Keyword::False) => Some(Literal::Boolean(false).into()),

            // Special literals
            Token::Keyword(Keyword::Infinity) => Some(Literal::Float(f64::INFINITY).into()),
            Token::Keyword(Keyword::NaN) => Some(Literal::Float(f64::NAN).into()),
            Token::Keyword(Keyword::Null) => Some(Literal::Null.into()),

            _ => None,
        })
    }

    /// Parse a DATE literal (DATE 'YYYY-MM-DD')
    fn parse_date_literal(&mut self) -> Result<Expression> {
        match self.next()? {
            Token::String(date_str) => {
                use chrono::NaiveDate;
                match NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                    Ok(date) => Ok(Literal::Date(date).into()),
                    Err(_) => Err(Error::ParseError(format!(
                        "Invalid date format: '{}'. Expected 'YYYY-MM-DD'",
                        date_str
                    ))),
                }
            }
            _ => Err(Error::ParseError("Expected string after DATE".into())),
        }
    }

    /// Parse a TIME literal (TIME 'HH:MM:SS')
    fn parse_time_literal(&mut self) -> Result<Expression> {
        match self.next()? {
            Token::String(time_str) => {
                use chrono::NaiveTime;
                match NaiveTime::parse_from_str(&time_str, "%H:%M:%S") {
                    Ok(time) => Ok(Literal::Time(time).into()),
                    Err(_) => {
                        // Try with fractional seconds
                        match NaiveTime::parse_from_str(&time_str, "%H:%M:%S%.f") {
                            Ok(time) => Ok(Literal::Time(time).into()),
                            Err(_) => Err(Error::ParseError(format!(
                                "Invalid time format: '{}'. Expected 'HH:MM:SS[.fraction]'",
                                time_str
                            ))),
                        }
                    }
                }
            }
            _ => Err(Error::ParseError("Expected string after TIME".into())),
        }
    }

    /// Parse a TIMESTAMP literal (TIMESTAMP 'YYYY-MM-DD HH:MM:SS')
    fn parse_timestamp_literal(&mut self) -> Result<Expression> {
        match self.next()? {
            Token::String(timestamp_str) => {
                use chrono::NaiveDateTime;
                match NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S") {
                    Ok(timestamp) => Ok(Literal::Timestamp(timestamp).into()),
                    Err(_) => {
                        // Try with fractional seconds
                        match NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S%.f")
                        {
                            Ok(timestamp) => Ok(Literal::Timestamp(timestamp).into()),
                            Err(_) => Err(Error::ParseError(format!(
                                "Invalid timestamp format: '{}'. Expected 'YYYY-MM-DD HH:MM:SS[.fraction]'",
                                timestamp_str
                            ))),
                        }
                    }
                }
            }
            _ => Err(Error::ParseError("Expected string after TIMESTAMP".into())),
        }
    }

    /// Parse an INTERVAL literal
    fn parse_interval_literal(&mut self) -> Result<Expression> {
        match self.next()? {
            Token::String(interval_str) => {
                // Check for complex interval formats with TO keyword
                if self.peek()?
                    .is_some_and(|t| matches!(t, Token::Ident(s) if s.to_uppercase().ends_with("S") || s.to_uppercase().ends_with("R") || s.to_uppercase().ends_with("H") || s.to_uppercase().ends_with("Y") || s.to_uppercase().ends_with("E")))
                {
                    let unit = self.next_ident_or_keyword()?;
                    if self
                        .next_if_map(|t| match t {
                            Token::Keyword(Keyword::To) => Some(()),
                            Token::Ident(s) if s.to_uppercase() == "TO" => Some(()),
                            _ => None,
                        })
                        .is_some()
                    {
                        // Complex format with TO keyword
                        let _end_unit = self.next_ident_or_keyword()?;
                        return self.parse_complex_interval_format(&interval_str, &unit);
                    }
                    // Simple format - process normally
                    self.parse_simple_interval(&interval_str, &unit)
                } else {
                    // Simple format without unit following
                    let unit = self.next_ident_or_keyword()?;
                    self.parse_simple_interval(&interval_str, &unit)
                }
            }
            _ => Err(Error::ParseError("Expected string after INTERVAL".into())),
        }
    }

    /// Parse a simple interval format like '5' DAY
    fn parse_simple_interval(&mut self, interval_str: &str, unit: &str) -> Result<Expression> {
        use crate::types::data_type::Interval;

        let value: i32 = interval_str.parse().map_err(|_| {
            Error::ParseError(format!(
                "Invalid interval value: '{}'. Expected a number",
                interval_str
            ))
        })?;

        let unit_upper = unit.to_uppercase();
        let interval = match unit_upper.as_str() {
            "DAY" | "DAYS" => Interval {
                months: 0,
                days: value,
                microseconds: 0,
            },
            "MONTH" | "MONTHS" => Interval {
                months: value,
                days: 0,
                microseconds: 0,
            },
            "YEAR" | "YEARS" => Interval {
                months: value * 12,
                days: 0,
                microseconds: 0,
            },
            "HOUR" | "HOURS" => Interval {
                months: 0,
                days: 0,
                microseconds: value as i64 * 3_600_000_000,
            },
            "MINUTE" | "MINUTES" => Interval {
                months: 0,
                days: 0,
                microseconds: value as i64 * 60_000_000,
            },
            "SECOND" | "SECONDS" => Interval {
                months: 0,
                days: 0,
                microseconds: value as i64 * 1_000_000,
            },
            _ => {
                return Err(Error::ParseError(format!(
                    "Invalid interval unit: '{}'. Expected DAY, MONTH, YEAR, HOUR, MINUTE, or SECOND",
                    unit
                )));
            }
        };
        Ok(Literal::Interval(interval).into())
    }

    /// Parse complex interval formats (to be implemented by concrete parser)
    fn parse_complex_interval_format(
        &mut self,
        interval_str: &str,
        start_unit: &str,
    ) -> Result<Expression>;
}
