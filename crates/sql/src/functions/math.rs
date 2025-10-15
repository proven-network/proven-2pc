//! Trigonometric math functions - SIN, COS, TAN, ASIN, ACOS, ATAN

use super::{Function, FunctionRegistry, FunctionSignature};
use crate::error::{Error, Result};
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use crate::types::{Value, ValueExt};

/// Helper function to convert a Value to f64 for math operations
fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::I8(v) => Ok(*v as f64),
        Value::I16(v) => Ok(*v as f64),
        Value::I32(v) => Ok(*v as f64),
        Value::I64(v) => Ok(*v as f64),
        Value::I128(v) => Ok(*v as f64),
        Value::U8(v) => Ok(*v as f64),
        Value::U16(v) => Ok(*v as f64),
        Value::U32(v) => Ok(*v as f64),
        Value::U64(v) => Ok(*v as f64),
        Value::U128(v) => Ok(*v as f64),
        Value::F32(v) => Ok(*v as f64),
        Value::F64(v) => Ok(*v),
        Value::Decimal(d) => {
            use rust_decimal::prelude::ToPrimitive;
            Ok(d.to_f64().unwrap_or(0.0))
        }
        _ => Err(Error::TypeMismatch {
            expected: "numeric".into(),
            found: value.data_type().to_string(),
        }),
    }
}

/// Helper function to validate numeric types for trig functions
fn validate_numeric_arg(
    arg_types: &[DataType],
    func_name: &str,
    expected_args: usize,
) -> Result<DataType> {
    if arg_types.len() != expected_args {
        return Err(Error::ExecutionError(format!(
            "{} takes exactly {} argument(s), got {}",
            func_name,
            expected_args,
            arg_types.len()
        )));
    }

    for arg_type in arg_types {
        match arg_type {
            DataType::I8
            | DataType::I16
            | DataType::I32
            | DataType::I64
            | DataType::I128
            | DataType::U8
            | DataType::U16
            | DataType::U32
            | DataType::U64
            | DataType::U128
            | DataType::F32
            | DataType::F64
            | DataType::Decimal(_, _) => {}
            DataType::Null => {}
            DataType::Nullable(inner) => match inner.as_ref() {
                DataType::Null
                | DataType::I8
                | DataType::I16
                | DataType::I32
                | DataType::I64
                | DataType::I128
                | DataType::U8
                | DataType::U16
                | DataType::U32
                | DataType::U64
                | DataType::U128
                | DataType::F32
                | DataType::F64
                | DataType::Decimal(_, _) => {}
                _ => {
                    return Err(Error::TypeMismatch {
                        expected: "numeric type".into(),
                        found: arg_type.to_string(),
                    });
                }
            },
            _ => {
                return Err(Error::TypeMismatch {
                    expected: "numeric type".into(),
                    found: arg_type.to_string(),
                });
            }
        }
    }

    // Always returns F64 (or Nullable F64 if input can be null)
    Ok(DataType::F64)
}

// SIN function
pub struct SinFunction;

impl Function for SinFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "SIN",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "SIN", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError("SIN takes exactly 1 argument".into()));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.sin()))
            }
        }
    }
}

// COS function
pub struct CosFunction;

impl Function for CosFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "COS",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "COS", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError("COS takes exactly 1 argument".into()));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.cos()))
            }
        }
    }
}

// TAN function
pub struct TanFunction;

impl Function for TanFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "TAN",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "TAN", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError("TAN takes exactly 1 argument".into()));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.tan()))
            }
        }
    }
}

// ASIN function (arc sine)
pub struct AsinFunction;

impl Function for AsinFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ASIN",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "ASIN", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "ASIN takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.asin()))
            }
        }
    }
}

// ACOS function (arc cosine)
pub struct AcosFunction;

impl Function for AcosFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ACOS",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "ACOS", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "ACOS takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.acos()))
            }
        }
    }
}

// ATAN function (arc tangent)
pub struct AtanFunction;

impl Function for AtanFunction {
    fn signature(&self) -> &FunctionSignature {
        static SIGNATURE: FunctionSignature = FunctionSignature {
            name: "ATAN",
            is_aggregate: false,
        };
        &SIGNATURE
    }

    fn validate(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_numeric_arg(arg_types, "ATAN", 1)
    }

    fn execute(&self, args: &[Value], _context: &ExecutionContext) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::ExecutionError(
                "ATAN takes exactly 1 argument".into(),
            ));
        }

        match &args[0] {
            Value::Null => Ok(Value::Null),
            v => {
                let val = value_to_f64(v)?;
                Ok(Value::F64(val.atan()))
            }
        }
    }
}

/// Register all trigonometric math functions
pub fn register(registry: &mut FunctionRegistry) {
    registry.register(Box::new(SinFunction));
    registry.register(Box::new(CosFunction));
    registry.register(Box::new(TanFunction));
    registry.register(Box::new(AsinFunction));
    registry.register(Box::new(AcosFunction));
    registry.register(Box::new(AtanFunction));
}
