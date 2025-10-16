//! Unary operator tests
//! Based on gluesql/test-suite/src/unary_operator.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]
fn test_unary_minus_operator() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT, v2 FLOAT, v3 TEXT, v4 INT, v5 INT, v6 TINYINT)");
    ctx.exec("INSERT INTO Test VALUES (10, 10.5, 'hello', -5, 1000, 20)");

    let results = ctx.query("SELECT -v1 as v1, -v2 as v2, v3, -v4 as v4, -v6 as v6 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I32(-10));
    assert_eq!(results[0].get("v2").unwrap(), &Value::F64(-10.5));
    assert_eq!(
        results[0].get("v3").unwrap(),
        &Value::Str("hello".to_owned())
    );
    assert_eq!(results[0].get("v4").unwrap(), &Value::I32(5));
    assert_eq!(results[0].get("v6").unwrap(), &Value::I8(-20));

    ctx.commit();
}

#[test]
fn test_double_unary_minus() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT -(-10) as v1, -(-10) as v2 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("v2").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_unary_minus_on_text_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v3 TEXT)");
    ctx.exec("INSERT INTO Test VALUES ('hello')");

    ctx.assert_error_contains("SELECT -v3 as v3 FROM Test", "non-numeric");

    ctx.commit();
}

#[test]
fn test_unary_minus_on_text_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT -'errrr' as v1 FROM Test", "non-numeric");

    ctx.commit();
}

#[test]
fn test_unary_plus_operator() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT +10 as v1, +(+10) as v2 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("v2").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_unary_plus_on_text_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v3 TEXT)");
    ctx.exec("INSERT INTO Test VALUES ('hello')");

    ctx.assert_error_contains("SELECT +v3 as v3 FROM Test", "non-numeric");

    ctx.commit();
}

#[test]
fn test_unary_plus_on_text_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT +'errrr' as v1 FROM Test", "non-numeric");

    ctx.commit();
}

#[test]
fn test_factorial_operator_on_integer() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (10)");

    let results = ctx.query("SELECT v1! as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I64(3628800));

    ctx.commit();
}

#[test]
fn test_factorial_operator_on_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT 4! as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I64(24));

    ctx.commit();
}

#[test]
fn test_factorial_on_float() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v2 FLOAT)");
    ctx.exec("INSERT INTO Test VALUES (10.5)");

    ctx.assert_error_contains("SELECT v2! as v1 FROM Test", "integer");

    ctx.commit();
}

#[test]
fn test_factorial_on_text() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v3 TEXT)");
    ctx.exec("INSERT INTO Test VALUES ('hello')");

    ctx.assert_error_contains("SELECT v3! as v1 FROM Test", "integer");

    ctx.commit();
}

#[test]
fn test_factorial_on_negative_number() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v4 INT)");
    ctx.exec("INSERT INTO Test VALUES (-5)");

    ctx.assert_error_contains("SELECT v4! as v4 FROM Test", "negative");

    ctx.commit();
}

#[test]
fn test_factorial_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v5 INT)");
    ctx.exec("INSERT INTO Test VALUES (1000)");

    ctx.assert_error_contains("SELECT v5! as v5 FROM Test", "overflow");

    ctx.commit();
}

#[test]
fn test_factorial_on_negative_expression() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v6 TINYINT)");
    ctx.exec("INSERT INTO Test VALUES (20)");

    ctx.assert_error_contains("SELECT (-v6)! as v6 FROM Test", "negative");

    ctx.commit();
}

#[test]
fn test_factorial_overflow_on_expression() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v6 TINYINT)");
    ctx.exec("INSERT INTO Test VALUES (20)");

    ctx.assert_error_contains("SELECT (v6 * 2)! as v6 FROM Test", "overflow");

    ctx.commit();
}

#[test]
fn test_factorial_on_negative_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT (-5)! as v4 FROM Test", "negative");

    ctx.commit();
}

#[test]
fn test_factorial_on_float_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT (5.5)! as v4 FROM Test", "integer");

    ctx.commit();
}

#[test]
fn test_factorial_on_text_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT 'errrr'! as v1 FROM Test", "integer");

    ctx.commit();
}

#[test]
fn test_factorial_overflow_on_large_literal() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT 1000! as v4 FROM Test", "overflow");

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_unsigned_int8() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS TINYINT UNSIGNED)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::U8(254));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_unsigned_int16() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS SMALLINT UNSIGNED)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::U16(65534));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_unsigned_int32() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS INT UNSIGNED)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::U32(4294967294));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_unsigned_int64() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS BIGINT UNSIGNED)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("v1").unwrap(),
        &Value::U64(18446744073709551614)
    );

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_unsigned_int128() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS HUGEINT UNSIGNED)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("v1").unwrap(),
        &Value::U128(340282366920938463463374607431768211454)
    );

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_signed_int8() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS TINYINT)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I8(-2));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_signed_int16() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS SMALLINT)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I16(-2));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_signed_int32() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS INT)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I32(-2));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_signed_int64() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~1 as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I32(-2));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_signed_int128() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~(CAST(1 AS HUGEINT)) as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::I128(-2));

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    let results = ctx.query("SELECT ~NULL as v1 FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v1").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_float() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT ~(5.5) as v4 FROM Test", "non-integer");

    ctx.commit();
}

#[test]
fn test_bitwise_not_on_text() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (v1 INT)");
    ctx.exec("INSERT INTO Test VALUES (1)");

    ctx.assert_error_contains("SELECT ~'error' as v1 FROM Test", "integer");

    ctx.commit();
}
