//! SMALLINT UNSIGNED data type tests
//! Based on gluesql/test-suite/src/data_type/uint16.rs

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_uint16_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one SMALLINT UNSIGNED, field_two SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U16(1));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::U16(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U16(2));
    assert_eq!(results[1].get("field_two").unwrap(), &Value::U16(2));
    assert_eq!(results[2].get("field_one").unwrap(), &Value::U16(3));
    assert_eq!(results[2].get("field_two").unwrap(), &Value::U16(3));
    assert_eq!(results[3].get("field_one").unwrap(), &Value::U16(4));
    assert_eq!(results[3].get("field_two").unwrap(), &Value::U16(4));

    ctx.commit();
}

#[test]
fn test_insert_uint16_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_test (id INT, value SMALLINT UNSIGNED)");

    // Test various SMALLINT UNSIGNED values (0 to 65535)
    ctx.exec("INSERT INTO uint16_test VALUES (1, 0)");
    ctx.exec("INSERT INTO uint16_test VALUES (2, 1)");
    ctx.exec("INSERT INTO uint16_test VALUES (3, 255)");
    ctx.exec("INSERT INTO uint16_test VALUES (4, 256)");
    ctx.exec("INSERT INTO uint16_test VALUES (5, 32767)");
    ctx.exec("INSERT INTO uint16_test VALUES (6, 32768)");
    ctx.exec("INSERT INTO uint16_test VALUES (7, 65535)");

    let results = ctx.query("SELECT id, value FROM uint16_test ORDER BY id");
    assert_eq!(results.len(), 7);
    assert_eq!(results[0].get("value").unwrap(), &Value::U16(0));
    assert_eq!(results[1].get("value").unwrap(), &Value::U16(1));
    assert_eq!(results[2].get("value").unwrap(), &Value::U16(255));
    assert_eq!(results[3].get("value").unwrap(), &Value::U16(256));
    assert_eq!(results[4].get("value").unwrap(), &Value::U16(32767));
    assert_eq!(results[5].get("value").unwrap(), &Value::U16(32768));
    assert_eq!(results[6].get("value").unwrap(), &Value::U16(65535));

    ctx.commit();
}

#[test]
#[should_panic(expected = "Cannot convert -1 to SMALLINT UNSIGNED")]
fn test_insert_uint16_negative_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_test (value SMALLINT UNSIGNED)");
    // Negative values are not allowed for SMALLINT UNSIGNED
    ctx.exec("INSERT INTO uint16_test VALUES (-1)");
}

#[test]
#[should_panic(expected = "Cannot convert 65536 to SMALLINT UNSIGNED")]
fn test_insert_uint16_overflow_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_test (value SMALLINT UNSIGNED)");
    // Values > 65535 are not allowed for SMALLINT UNSIGNED
    ctx.exec("INSERT INTO uint16_test VALUES (65536)");
}

#[test]
fn test_uint16_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one SMALLINT UNSIGNED, field_two SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    // Test greater than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 2 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U16(3));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U16(4));

    // Test greater than or equal
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one >= 2 ORDER BY field_one");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U16(2));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U16(3));
    assert_eq!(results[2].get("field_one").unwrap(), &Value::U16(4));

    // Test equality
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U16(2));

    // Test less than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 3 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U16(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U16(2));

    ctx.commit();
}

#[test]
fn test_uint16_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_math (a SMALLINT UNSIGNED, b SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO uint16_math VALUES (100, 30)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM uint16_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sum").unwrap(), &Value::U16(130));

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM uint16_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("diff").unwrap(), &Value::U16(70));

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM uint16_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("product").unwrap(), &Value::U16(3000));

    // Test division (integer division)
    let results = ctx.query("SELECT a / b AS quotient FROM uint16_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("quotient").unwrap(), &Value::U16(3));

    // Test modulo
    let results = ctx.query("SELECT a % b AS remainder FROM uint16_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("remainder").unwrap(), &Value::U16(10));

    ctx.commit();
}

#[test]
fn test_uint16_range_boundaries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_bounds (value SMALLINT UNSIGNED)");

    // Test minimum value (0)
    ctx.exec("INSERT INTO uint16_bounds VALUES (0)");

    // Test maximum value (65535)
    ctx.exec("INSERT INTO uint16_bounds VALUES (65535)");

    let results = ctx.query("SELECT value FROM uint16_bounds ORDER BY value");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), &Value::U16(0));
    assert_eq!(results[1].get("value").unwrap(), &Value::U16(65535));

    // Test operations at boundaries
    ctx.exec("CREATE TABLE uint16_boundary_ops (a SMALLINT UNSIGNED, b SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO uint16_boundary_ops VALUES (65535, 0)");

    // Test 65535 - 0 = 65535
    let results = ctx.query("SELECT a - b AS diff FROM uint16_boundary_ops");
    assert_eq!(results[0].get("diff").unwrap(), &Value::U16(65535));

    ctx.commit();
}

#[test]
#[should_panic(expected = "overflow")]
fn test_uint16_addition_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_overflow (a SMALLINT UNSIGNED, b SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO uint16_overflow VALUES (65535, 1)");

    // This should cause an overflow
    ctx.query("SELECT a + b AS sum FROM uint16_overflow");
}

#[test]
#[should_panic(expected = "underflow")]
fn test_uint16_subtraction_underflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_underflow (a SMALLINT UNSIGNED, b SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO uint16_underflow VALUES (0, 1)");

    // This should cause an underflow
    ctx.query("SELECT a - b AS diff FROM uint16_underflow");
}

#[test]
fn test_uint16_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint16_nulls (id INT, value SMALLINT UNSIGNED)");
    ctx.exec("INSERT INTO uint16_nulls VALUES (1, 1000), (2, NULL), (3, 2000)");

    let results =
        ctx.query("SELECT id, value FROM uint16_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), &Value::U16(1000));
    assert_eq!(results[1].get("value").unwrap(), &Value::U16(2000));

    // Test NULL propagation in arithmetic
    let results = ctx.query("SELECT id, value + 500 as result FROM uint16_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::U16(1500));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::U16(2500));

    ctx.commit();
}

#[test]
fn test_cast_to_uint16() {
    let mut ctx = setup_test();

    // Test CAST from string
    let results = ctx.query("SELECT CAST('12345' AS SMALLINT UNSIGNED) AS uint16_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint16_val").unwrap(), &Value::U16(12345));

    // Test CAST from integer
    let results = ctx.query("SELECT CAST(4200 AS SMALLINT UNSIGNED) AS uint16_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint16_val").unwrap(), &Value::U16(4200));

    // Test CAST from larger integer type
    ctx.exec("CREATE TABLE test_cast (i INT)");
    ctx.exec("INSERT INTO test_cast VALUES (10000)");

    let results = ctx.query("SELECT CAST(i AS SMALLINT UNSIGNED) AS uint16_val FROM test_cast");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint16_val").unwrap(), &Value::U16(10000));

    ctx.commit();
}
