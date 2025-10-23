//! SQRT and POWER function tests
//! Based on gluesql/test-suite/src/function/sqrt_power.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_sqrt_basic() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT
            SQRT(4.0) as sqrt_1,
            SQRT(0.07) as sqrt_2",
    );
    assert_eq!(results.len(), 1);

    assert_eq!(results[0].get("sqrt_1").unwrap(), &Value::F64(2.0));
    assert_eq!(
        results[0].get("sqrt_2").unwrap(),
        &Value::F64(0.07_f64.sqrt())
    );

    ctx.commit();
}

#[test]
fn test_sqrt_with_integer() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT SQRT(64) as sqrt_with_int");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sqrt_with_int").unwrap(), &Value::F64(8.0));

    ctx.commit();
}

#[test]
fn test_sqrt_with_zero() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT SQRT(0) as sqrt_with_zero");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sqrt_with_zero").unwrap(), &Value::F64(0.0));

    ctx.commit();
}

#[test]
fn test_sqrt_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SQRT('string') AS sqrt", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_sqrt_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT SQRT(NULL) AS sqrt");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sqrt").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_sqrt_negative_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SQRT(-1) AS sqrt", "SQRT of negative number");

    ctx.commit();
}

#[test]
fn test_power_basic() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT
            POWER(2.0, 4) as power_1,
            POWER(0.07, 3) as power_2",
    );
    assert_eq!(results.len(), 1);

    assert_eq!(results[0].get("power_1").unwrap(), &Value::F64(16.0));
    assert_eq!(
        results[0].get("power_2").unwrap(),
        &Value::F64(0.07_f64.powf(3.0))
    );

    ctx.commit();
}

#[test]
fn test_power_with_zero() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT
            POWER(0, 4) as power_with_zero,
            POWER(3, 0) as power_to_zero",
    );
    assert_eq!(results.len(), 1);

    assert_eq!(results[0].get("power_with_zero").unwrap(), &Value::F64(0.0));
    assert_eq!(results[0].get("power_to_zero").unwrap(), &Value::F64(1.0));

    ctx.commit();
}

#[test]
fn test_power_with_float_exponent() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT POWER(32, 3.0) as power_with_float");
    assert_eq!(results.len(), 1);

    assert_eq!(
        results[0].get("power_with_float").unwrap(),
        &Value::F64(32.0_f64.powf(3.0))
    );

    ctx.commit();
}

#[test]
fn test_power_with_negative_exponent() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT POWER(2, -2) as power_negative");
    assert_eq!(results.len(), 1);

    assert_eq!(results[0].get("power_negative").unwrap(), &Value::F64(0.25));

    ctx.commit();
}

#[test]
fn test_power_base_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT POWER('string', 'string') AS power", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_power_exponent_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT POWER(2.0, 'string') AS power", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_power_base_as_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT POWER('string', 2.0) AS power", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_power_with_null_both() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT POWER(NULL, NULL) AS power");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("power").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_power_with_null_exponent() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT POWER(2.0, NULL) AS power");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("power").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_power_with_null_base() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT POWER(NULL, 2.0) AS power");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("power").unwrap(), &Value::Null);

    ctx.commit();
}
