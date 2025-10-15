//! TRIM, LTRIM, RTRIM functions - remove whitespace from strings

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// Helper function to trim specific characters from a string
fn trim_chars<'a>(s: &'a str, chars: &str, trim_spec: &str) -> &'a str {
    // Convert chars string into a set of characters to trim
    let char_set: std::collections::HashSet<char> = chars.chars().collect();

    match trim_spec {
        "LEADING" => {
            // Trim from the start
            s.trim_start_matches(|c: char| char_set.contains(&c))
        }
        "TRAILING" => {
            // Trim from the end
            s.trim_end_matches(|c: char| char_set.contains(&c))
        }
        "BOTH" => {
            // Trim from both sides
            s.trim_start_matches(|c: char| char_set.contains(&c))
                .trim_end_matches(|c: char| char_set.contains(&c))
        }
        _ => s,
    }
}

pub struct TrimFunction;
pub struct LtrimFunction;
pub struct RtrimFunction;

impl Function for TrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        // TRIM can take 1, 2, or 3 arguments:
        // 1 arg: TRIM(source)
        // 2 args: TRIM(trim_spec, source) - trim_spec is a string literal "LEADING"/"TRAILING"/"BOTH"
        // 3 args: TRIM(trim_spec, chars, source)
        if arg_types.is_empty() || arg_types.len() > 3 {
            return Err(Error::ExecutionError(format!(
                "TRIM takes 1-3 arguments, got {}",
                arg_types.len()
            )));
        }

        // Check the last argument (source) is a string type
        let source_type = &arg_types[arg_types.len() - 1];
        match source_type {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type for source".into(),
                    found: source_type.to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type for source".into(),
                found: source_type.to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.is_empty() || args.len() > 3 {
            return Err(Error::ExecutionError(format!(
                "TRIM takes 1-3 arguments, got {}",
                args.len()
            )));
        }

        // Parse arguments based on count
        let (trim_spec, chars_to_trim, source) = match args.len() {
            1 => {
                // TRIM(source) - default to BOTH, trim whitespace
                ("BOTH", None, &args[0])
            }
            2 => {
                // TRIM(trim_spec, source) - trim whitespace with given spec
                let spec = match &args[0] {
                    Value::Str(s) => s.as_str(),
                    _ => {
                        return Err(Error::ExecutionError(
                            "TRIM specification must be a string".into(),
                        ));
                    }
                };
                (spec, None, &args[1])
            }
            3 => {
                // TRIM(trim_spec, chars, source) - trim specific chars with given spec
                let spec = match &args[0] {
                    Value::Str(s) => s.as_str(),
                    _ => {
                        return Err(Error::ExecutionError(
                            "TRIM specification must be a string".into(),
                        ));
                    }
                };
                let chars = match &args[1] {
                    Value::Str(s) => Some(s.as_str()),
                    Value::Null => None,
                    _ => {
                        return Err(Error::TypeMismatch {
                            expected: "string".into(),
                            found: args[1].data_type().to_string(),
                        });
                    }
                };
                (spec, chars, &args[2])
            }
            _ => unreachable!(),
        };

        // Handle NULL source
        match source {
            Value::Null => Ok(Value::Null),
            Value::Str(s) => {
                // If chars_to_trim is NULL, result is NULL
                if chars_to_trim.is_none() && args.len() == 3 {
                    return Ok(Value::Null);
                }

                let result = if let Some(chars) = chars_to_trim {
                    // Trim specific characters
                    trim_chars(s, chars, trim_spec)
                } else {
                    // Trim whitespace
                    match trim_spec {
                        "LEADING" => s.trim_start(),
                        "TRAILING" => s.trim_end(),
                        "BOTH" => s.trim(),
                        _ => {
                            return Err(Error::ExecutionError(format!(
                                "Invalid TRIM specification: {}",
                                trim_spec
                            )));
                        }
                    }
                };
                Ok(Value::string(result))
            }
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: source.data_type().to_string(),
            }),
        }
    }
}

impl Function for LtrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "LTRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "LTRIM takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "LTRIM takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.trim_start())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

impl Function for RtrimFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "RTRIM",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 1 {
            return Err(Error::ExecutionError(format!(
                "RTRIM takes exactly 1 argument, got {}",
                arg_types.len()
            )));
        }

        match &arg_types[0] {
            DataType::Text | DataType::Str => Ok(DataType::Text),
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Text | DataType::Str => Ok(DataType::Nullable(Box::new(DataType::Text))),
                _ => Err(Error::TypeMismatch {
                    expected: "string type".into(),
                    found: arg_types[0].to_string(),
                }),
            },
            _ => Err(Error::TypeMismatch {
                expected: "string type".into(),
                found: arg_types[0].to_string(),
            }),
        }
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "RTRIM takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Str(s) => Ok(Value::string(s.trim_end())),
            Value::Null => Ok(Value::Null),
            _ => Err(Error::TypeMismatch {
                expected: "string".into(),
                found: args[0].data_type().to_string(),
            }),
        }
    }
}

/// Register the TRIM functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(TrimFunction));
    registry.register(Box::new(LtrimFunction));
    registry.register(Box::new(RtrimFunction));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_hlc::{HlcTimestamp, NodeId};

    #[test]
    fn test_trim_execute() {
        let func = TrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim both sides
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello world"));

        // No whitespace
        let result = func.execute(&[Value::string("hello")], &context).unwrap();
        assert_eq!(result, Value::string("hello"));

        // Only whitespace
        let result = func.execute(&[Value::string("   ")], &context).unwrap();
        assert_eq!(result, Value::string(""));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_ltrim_execute() {
        let func = LtrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim left side only
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("hello world  "));

        // No left whitespace
        let result = func.execute(&[Value::string("hello  ")], &context).unwrap();
        assert_eq!(result, Value::string("hello  "));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_rtrim_execute() {
        let func = RtrimFunction;
        let context = ExecutionContext::new(HlcTimestamp::new(1000, 0, NodeId::new(1)), 0);

        // Trim right side only
        let result = func
            .execute(&[Value::string("  hello world  ")], &context)
            .unwrap();
        assert_eq!(result, Value::string("  hello world"));

        // No right whitespace
        let result = func.execute(&[Value::string("  hello")], &context).unwrap();
        assert_eq!(result, Value::string("  hello"));

        // NULL handling
        let result = func.execute(&[Value::Null], &context).unwrap();
        assert_eq!(result, Value::Null);
    }
}
