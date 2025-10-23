//! Bitwise right shift operation tests
//! Based on gluesql/test-suite/src/bitwise_shift_right.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_bitwise_shift_right_basic() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");
    ctx.exec("INSERT INTO Test (id, num) VALUES (1, 1)");
    ctx.exec("INSERT INTO Test (id, num) VALUES (2, 2)");
    ctx.exec("INSERT INTO Test (id, num) VALUES (3, 4)");
    ctx.exec("INSERT INTO Test (id, num) VALUES (4, 8)");

    let results = ctx.query("SELECT (num >> 1) as num FROM Test ORDER BY id");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(0));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(1));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[3].get("num").unwrap(), &Value::I32(4));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_literals() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 32 >> 2 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(8));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_by_zero() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 32 >> 0 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(32));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_to_zero() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 1 >> 5 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(0));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE OverflowTest (id INTEGER, num INTEGER)");
    ctx.exec("INSERT INTO OverflowTest (id, num) VALUES (1, 1)");

    // Shifting by 65 bits exceeds I32 bit width (32 bits)
    ctx.assert_error_contains(
        "SELECT (num >> 65) as overflowed FROM OverflowTest",
        "exceeds bit width",
    );

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_with_null_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullTest (id INTEGER, num INTEGER)");
    ctx.exec("INSERT INTO NullTest (id, num) VALUES (NULL, 1)");

    let results = ctx.query("SELECT id, num FROM NullTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_null_operand() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT NULL >> 2 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_by_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT 32 >> NULL AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_negative_shift_should_error() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT 32 >> -1 AS result", "non-negative");

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_negative_number() {
    let mut ctx = setup_test();

    // Right shift on negative number should use arithmetic shift (sign-extending)
    let results = ctx.query("SELECT -8 >> 1 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(-4));

    ctx.commit();
}

#[test]
fn test_bitwise_shift_right_max_shift() {
    let mut ctx = setup_test();

    // Shift by 31 (max for I32)
    let results = ctx.query("SELECT 2147483647 >> 31 AS result");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(0));

    ctx.commit();
}
