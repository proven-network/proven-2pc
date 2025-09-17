//! LIKE pattern matching operator implementation

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct LikeOperator;

impl BinaryOperator for LikeOperator {
    fn name(&self) -> &'static str {
        "pattern matching"
    }

    fn symbol(&self) -> &'static str {
        "LIKE"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // LIKE is NOT commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, _) = unwrap_nullable_pair(left, right);

        // Both operands must be string types
        match (left_inner, right_inner) {
            (Str, Str) | (Text, Text) | (Str, Text) | (Text, Str) => Ok(Bool),
            _ => Err(Error::TypeMismatch {
                expected: "STRING types".into(),
                found: format!("{:?} LIKE {:?}", left, right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        match (left, right) {
            // NULL handling
            (Null, _) | (_, Null) => Ok(Null),

            // String pattern matching
            (Str(text), Str(pattern)) => {
                let result = match_pattern(text, pattern)?;
                Ok(Bool(result))
            }

            _ => Err(Error::TypeMismatch {
                expected: "STRING values".into(),
                found: format!("{:?} LIKE {:?}", left, right),
            }),
        }
    }
}

/// Match SQL LIKE pattern
/// % matches zero or more characters
/// _ matches exactly one character
/// \ escapes the next character
fn match_pattern(text: &str, pattern: &str) -> Result<bool> {
    // Convert SQL pattern to regex pattern
    let regex_pattern = sql_pattern_to_regex(pattern);

    // Use regex crate for pattern matching
    let re = regex::Regex::new(&regex_pattern)
        .map_err(|e| Error::InvalidValue(format!("Invalid LIKE pattern: {}", e)))?;

    Ok(re.is_match(text))
}

/// Convert SQL LIKE pattern to regex pattern
fn sql_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            // Escape special regex characters
            match ch {
                '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
                | '\\' => {
                    regex.push('\\');
                    regex.push(ch);
                }
                _ => regex.push(ch),
            }
            escaped = false;
        } else {
            match ch {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                '\\' => escaped = true,
                // Escape special regex characters
                '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' => {
                    regex.push('\\');
                    regex.push(ch);
                }
                _ => regex.push(ch),
            }
        }
    }

    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like() {
        let op = LikeOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Str, &DataType::Str).unwrap(),
            DataType::Bool
        );

        // Execution - exact match
        assert_eq!(
            op.execute(
                &Value::Str("hello".to_string()),
                &Value::Str("hello".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Pattern with %
        assert_eq!(
            op.execute(
                &Value::Str("hello world".to_string()),
                &Value::Str("hello%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(
                &Value::Str("hello world".to_string()),
                &Value::Str("%world".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(
                &Value::Str("hello world".to_string()),
                &Value::Str("%o%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Pattern with _
        assert_eq!(
            op.execute(
                &Value::Str("hello".to_string()),
                &Value::Str("h_llo".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            op.execute(
                &Value::Str("hello".to_string()),
                &Value::Str("h__lo".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        // No match
        assert_eq!(
            op.execute(
                &Value::Str("hello".to_string()),
                &Value::Str("goodbye%".to_string())
            )
            .unwrap(),
            Value::Bool(false)
        );

        // NULL handling
        assert_eq!(
            op.execute(&Value::Str("hello".to_string()), &Value::Null)
                .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_sql_pattern_to_regex() {
        assert_eq!(sql_pattern_to_regex("hello"), "^hello$");
        assert_eq!(sql_pattern_to_regex("hello%"), "^hello.*$");
        assert_eq!(sql_pattern_to_regex("%world"), "^.*world$");
        assert_eq!(sql_pattern_to_regex("h_llo"), "^h.llo$");
        assert_eq!(sql_pattern_to_regex("h%o_d"), "^h.*o.d$");
        assert_eq!(sql_pattern_to_regex("\\%hello"), "^%hello$");
        assert_eq!(sql_pattern_to_regex("\\_test"), "^_test$");
    }
}
