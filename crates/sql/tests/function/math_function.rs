//! Math function tests (SIN, COS, TAN, etc.)
//! Based on gluesql/test-suite/src/function/math_function.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_sin_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT SIN(0.5) AS sin1, SIN(1) AS sin2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sin1").unwrap(), &Value::F64(0.5_f64.sin()));
    assert_eq!(results[0].get("sin2").unwrap(), &Value::F64(1.0_f64.sin()));

    ctx.commit();
}

#[test]
fn test_sin_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT SIN(null) AS sin");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sin").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_sin_with_boolean_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SIN(true) AS sin", "numeric");
    ctx.assert_error_contains("SELECT SIN(false) AS sin", "numeric");

    ctx.commit();
}

#[test]
fn test_sin_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SIN('string') AS sin", "numeric");

    ctx.commit();
}

#[test]
fn test_sin_no_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SIN() AS sin", "1 argument");

    ctx.commit();
}

#[test]
fn test_sin_too_many_arguments_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT SIN(1.0, 2.0) AS sin", "1 argument");

    ctx.commit();
}

#[test]
fn test_cos_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT COS(0.5) AS cos1, COS(1) AS cos2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("cos1").unwrap(), &Value::F64(0.5_f64.cos()));
    assert_eq!(results[0].get("cos2").unwrap(), &Value::F64(1.0_f64.cos()));

    ctx.commit();
}

#[test]
fn test_cos_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT COS(null) AS cos");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("cos").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_cos_with_boolean_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT COS(true) AS cos", "numeric");
    ctx.assert_error_contains("SELECT COS(false) AS cos", "numeric");

    ctx.commit();
}

#[test]
fn test_cos_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT COS('string') AS cos", "numeric");

    ctx.commit();
}

#[test]
fn test_tan_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TAN(0.5) AS tan1, TAN(1) AS tan2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("tan1").unwrap(), &Value::F64(0.5_f64.tan()));
    assert_eq!(results[0].get("tan2").unwrap(), &Value::F64(1.0_f64.tan()));

    ctx.commit();
}

#[test]
fn test_tan_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TAN(null) AS tan");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("tan").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_tan_with_boolean_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT TAN(true) AS tan", "numeric");
    ctx.assert_error_contains("SELECT TAN(false) AS tan", "numeric");

    ctx.commit();
}

#[test]
fn test_asin_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ASIN(0.5) AS asin1, ASIN(1) AS asin2");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("asin1").unwrap(),
        &Value::F64(0.5_f64.asin())
    );
    assert_eq!(
        results[0].get("asin2").unwrap(),
        &Value::F64(1.0_f64.asin())
    );

    ctx.commit();
}

#[test]
fn test_asin_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ASIN(null) AS asin");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("asin").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_asin_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT ASIN('string') AS asin", "numeric");

    ctx.commit();
}

#[test]
fn test_acos_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ACOS(0.5) AS acos1, ACOS(1) AS acos2");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("acos1").unwrap(),
        &Value::F64(0.5_f64.acos())
    );
    assert_eq!(
        results[0].get("acos2").unwrap(),
        &Value::F64(1.0_f64.acos())
    );

    ctx.commit();
}

#[test]
fn test_acos_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ACOS(null) AS acos");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("acos").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_acos_with_boolean_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT ACOS(true) AS acos", "numeric");

    ctx.commit();
}

#[test]
fn test_atan_function() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ATAN(0.5) AS atan1, ATAN(1) AS atan2");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("atan1").unwrap(),
        &Value::F64(0.5_f64.atan())
    );
    assert_eq!(
        results[0].get("atan2").unwrap(),
        &Value::F64(1.0_f64.atan())
    );

    ctx.commit();
}

#[test]
fn test_atan_with_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT ATAN(null) AS atan");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("atan").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_atan_with_string_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT ATAN('string') AS atan", "numeric");

    ctx.commit();
}

#[test]
fn test_atan_with_boolean_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT ATAN(true) AS atan", "numeric");

    ctx.commit();
}

#[test]
fn test_math_functions_type_validation() {
    let mut ctx = setup_test();

    // All math functions should accept integers
    let results = ctx.query("SELECT SIN(1), COS(1), TAN(1), ASIN(0), ACOS(0), ATAN(1)");
    assert_eq!(results.len(), 1);

    // All math functions should reject non-numeric types
    ctx.assert_error_contains("SELECT SIN('not a number')", "numeric");
    ctx.assert_error_contains("SELECT COS('not a number')", "numeric");
    ctx.assert_error_contains("SELECT TAN('not a number')", "numeric");

    ctx.commit();
}
