use super::*;
use crate::types::{DataType, Value};

#[test]
fn test_integer_widening() {
    // I64 to I128 (widening - always safe)
    let value = Value::I64(1000000000000);
    let result = coerce_value(value, &DataType::I128).unwrap();
    assert_eq!(result, Value::I128(1000000000000));

    // I32 to I64 (widening - always safe)
    let value = Value::I32(42);
    let result = coerce_value(value, &DataType::I64).unwrap();
    assert_eq!(result, Value::I64(42));
}

#[test]
fn test_integer_narrowing() {
    // I64 to I32 (narrowing - may fail)
    let value = Value::I64(42);
    let result = coerce_value(value, &DataType::I32).unwrap();
    assert_eq!(result, Value::I32(42));

    // I64 to I32 with overflow should fail
    let value = Value::I64(i64::MAX);
    let result = coerce_value(value, &DataType::I32);
    assert!(result.is_err());
}

#[test]
fn test_null_coercion() {
    // NULL can be coerced to any type
    let value = Value::Null;
    let result = coerce_value(value.clone(), &DataType::I128).unwrap();
    assert_eq!(result, Value::Null);

    let result = coerce_value(value.clone(), &DataType::Str).unwrap();
    assert_eq!(result, Value::Null);
}

#[test]
fn test_integer_to_decimal() {
    let value = Value::I64(42);
    let result = coerce_value(value, &DataType::Decimal(Some(10), Some(2))).unwrap();
    match result {
        Value::Decimal(d) => assert_eq!(d.to_string(), "42"),
        _ => panic!("Expected Decimal"),
    }
}
