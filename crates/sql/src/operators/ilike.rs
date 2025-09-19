//! ILIKE pattern matching operator implementation (case-insensitive LIKE)

use super::helpers::*;
use super::traits::BinaryOperator;
use crate::error::{Error, Result};
use crate::types::{DataType, Value};

pub struct ILikeOperator;

impl BinaryOperator for ILikeOperator {
    fn name(&self) -> &'static str {
        "case-insensitive pattern matching"
    }

    fn symbol(&self) -> &'static str {
        "ILIKE"
    }

    fn is_commutative(&self, _left: &DataType, _right: &DataType) -> bool {
        false // ILIKE is NOT commutative
    }

    fn validate(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        let (left_inner, right_inner, _) = unwrap_nullable_pair(left, right);

        // Both operands must be string types
        match (left_inner, right_inner) {
            (Str, Str) | (Text, Text) | (Str, Text) | (Text, Str) => Ok(Bool),
            _ => Err(Error::TypeMismatch {
                expected: "STRING types".into(),
                found: format!("{:?} ILIKE {:?}", left, right),
            }),
        }
    }

    fn execute(&self, left: &Value, right: &Value) -> Result<Value> {
        use Value::*;

        match (left, right) {
            // NULL handling
            (Null, _) | (_, Null) => Ok(Null),

            // String pattern matching (case-insensitive)
            (Str(text), Str(pattern)) => {
                // Convert both to lowercase for case-insensitive comparison
                let text_lower = text.to_lowercase();
                let pattern_lower = pattern.to_lowercase();
                let result = match_pattern(&text_lower, &pattern_lower)?;
                Ok(Bool(result))
            }

            _ => Err(Error::TypeMismatch {
                expected: "STRING values".into(),
                found: format!("{:?} ILIKE {:?}", left, right),
            }),
        }
    }
}

/// Match SQL LIKE pattern (shared with LIKE operator)
/// % matches zero or more characters
/// _ matches exactly one character
/// \ escapes the next character
fn match_pattern(text: &str, pattern: &str) -> Result<bool> {
    // Convert SQL pattern to regex pattern
    let regex_pattern = sql_pattern_to_regex(pattern);

    // Use regex crate for pattern matching
    let re = regex::Regex::new(&regex_pattern)
        .map_err(|e| Error::InvalidValue(format!("Invalid ILIKE pattern: {}", e)))?;

    Ok(re.is_match(text))
}

/// Convert SQL LIKE/ILIKE pattern to regex pattern
fn sql_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    let chars = pattern.chars().peekable();
    let mut escaped = false;

    for ch in chars {
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
    fn test_ilike_case_insensitive() {
        let op = ILikeOperator;

        // Type validation
        assert_eq!(
            op.validate(&DataType::Str, &DataType::Str).unwrap(),
            DataType::Bool
        );

        // Case insensitive matching
        assert_eq!(
            op.execute(
                &Value::Str("HELLO".to_string()),
                &Value::Str("hello".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            op.execute(
                &Value::Str("Hello World".to_string()),
                &Value::Str("HELLO%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            op.execute(
                &Value::Str("HELLO".to_string()),
                &Value::Str("%ell%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        // Pattern with _
        assert_eq!(
            op.execute(
                &Value::Str("HeLLo".to_string()),
                &Value::Str("h_llo".to_string())
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
    fn test_ilike_complex_patterns() {
        let op = ILikeOperator;

        // Mixed case patterns
        assert_eq!(
            op.execute(
                &Value::Str("PostgreSQL".to_string()),
                &Value::Str("postgres%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            op.execute(
                &Value::Str("PostgreSQL".to_string()),
                &Value::Str("%sql".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );

        assert_eq!(
            op.execute(
                &Value::Str("PostgreSQL".to_string()),
                &Value::Str("P_STG_E%".to_string())
            )
            .unwrap(),
            Value::Bool(true)
        );
    }
}
