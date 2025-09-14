//! UINT8 data type tests
//! Based on gluesql/test-suite/src/data_type/uint8.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_uint8_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one TINYINT UNSIGNED, field_two TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("field_one").unwrap(), "U8(1)");
    assert_eq!(results[0].get("field_two").unwrap(), "U8(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U8(2)");
    assert_eq!(results[1].get("field_two").unwrap(), "U8(2)");
    assert_eq!(results[2].get("field_one").unwrap(), "U8(3)");
    assert_eq!(results[2].get("field_two").unwrap(), "U8(3)");
    assert_eq!(results[3].get("field_one").unwrap(), "U8(4)");
    assert_eq!(results[3].get("field_two").unwrap(), "U8(4)");

    ctx.commit();
}

#[test]
fn test_insert_uint8_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_test (id INT, value TINYINT UNSIGNED)");

    // Test various UINT8 values (0 to 255)
    ctx.exec("INSERT INTO uint8_test VALUES (1, 0)");
    ctx.exec("INSERT INTO uint8_test VALUES (2, 1)");
    ctx.exec("INSERT INTO uint8_test VALUES (3, 127)");
    ctx.exec("INSERT INTO uint8_test VALUES (4, 128)");
    ctx.exec("INSERT INTO uint8_test VALUES (5, 255)");

    let results = ctx.query("SELECT id, value FROM uint8_test ORDER BY id");
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].get("value").unwrap(), "U8(0)");
    assert_eq!(results[1].get("value").unwrap(), "U8(1)");
    assert_eq!(results[2].get("value").unwrap(), "U8(127)");
    assert_eq!(results[3].get("value").unwrap(), "U8(128)");
    assert_eq!(results[4].get("value").unwrap(), "U8(255)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "Cannot convert -1 to TINYINT UNSIGNED")]
fn test_insert_uint8_negative_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_test (value TINYINT UNSIGNED)");
    // Negative values are not allowed for UINT8
    ctx.exec("INSERT INTO uint8_test VALUES (-1)");
}

#[test]
#[should_panic(expected = "Cannot convert 256 to TINYINT UNSIGNED")]
fn test_insert_uint8_overflow_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_test (value TINYINT UNSIGNED)");
    // Values > 255 are not allowed for UINT8
    ctx.exec("INSERT INTO uint8_test VALUES (256)");
}

#[test]
fn test_uint8_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one TINYINT UNSIGNED, field_two TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    // Test greater than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 2 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U8(3)");
    assert_eq!(results[1].get("field_one").unwrap(), "U8(4)");

    // Test greater than or equal
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one >= 2 ORDER BY field_one");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("field_one").unwrap(), "U8(2)");
    assert_eq!(results[1].get("field_one").unwrap(), "U8(3)");
    assert_eq!(results[2].get("field_one").unwrap(), "U8(4)");

    // Test equality
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("field_one").unwrap(), "U8(2)");

    // Test less than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 3 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U8(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U8(2)");

    ctx.commit();
}

#[test]
fn test_uint8_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_math (a TINYINT UNSIGNED, b TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO uint8_math VALUES (10, 3)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM uint8_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sum").unwrap(), "U8(13)");

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM uint8_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("diff").unwrap(), "U8(7)");

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM uint8_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("product").unwrap(), "U8(30)");

    // Test division (integer division)
    let results = ctx.query("SELECT a / b AS quotient FROM uint8_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("quotient").unwrap(), "U8(3)");

    // Test modulo
    let results = ctx.query("SELECT a % b AS remainder FROM uint8_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("remainder").unwrap(), "U8(1)");

    ctx.commit();
}

#[test]
fn test_uint8_range_boundaries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_bounds (value TINYINT UNSIGNED)");

    // Test minimum value (0)
    ctx.exec("INSERT INTO uint8_bounds VALUES (0)");

    // Test maximum value (255)
    ctx.exec("INSERT INTO uint8_bounds VALUES (255)");

    let results = ctx.query("SELECT value FROM uint8_bounds ORDER BY value");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), "U8(0)");
    assert_eq!(results[1].get("value").unwrap(), "U8(255)");

    // Test operations at boundaries
    ctx.exec("CREATE TABLE uint8_boundary_ops (a TINYINT UNSIGNED, b TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO uint8_boundary_ops VALUES (255, 0)");

    // Test that 255 + 1 would overflow (this should panic)
    // We'll test this separately in another test

    // Test 255 - 0 = 255
    let results = ctx.query("SELECT a - b AS diff FROM uint8_boundary_ops");
    assert_eq!(results[0].get("diff").unwrap(), "U8(255)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "overflow")]
fn test_uint8_addition_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_overflow (a TINYINT UNSIGNED, b TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO uint8_overflow VALUES (255, 1)");

    // This should cause an overflow
    ctx.query("SELECT a + b AS sum FROM uint8_overflow");
}

#[test]
#[should_panic(expected = "underflow")]
fn test_uint8_subtraction_underflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_underflow (a TINYINT UNSIGNED, b TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO uint8_underflow VALUES (0, 1)");

    // This should cause an underflow
    ctx.query("SELECT a - b AS diff FROM uint8_underflow");
}

#[test]
fn test_uint8_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint8_nulls (id INT, value TINYINT UNSIGNED)");
    ctx.exec("INSERT INTO uint8_nulls VALUES (1, 10), (2, NULL), (3, 20)");

    let results =
        ctx.query("SELECT id, value FROM uint8_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), "U8(10)");
    assert_eq!(results[1].get("value").unwrap(), "U8(20)");

    // Test NULL propagation in arithmetic
    let results = ctx.query("SELECT id, value + 5 as result FROM uint8_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), "U8(15)");
    assert_eq!(results[1].get("result").unwrap(), "Null");
    assert_eq!(results[2].get("result").unwrap(), "U8(25)");

    ctx.commit();
}

#[test]
fn test_cast_to_uint8() {
    let mut ctx = setup_test();

    // Test CAST from string
    let results = ctx.query("SELECT CAST('123' AS TINYINT UNSIGNED) AS uint8_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint8_val").unwrap(), "U8(123)");

    // Test CAST from integer
    let results = ctx.query("SELECT CAST(42 AS TINYINT UNSIGNED) AS uint8_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint8_val").unwrap(), "U8(42)");

    // Test CAST from larger integer type
    ctx.exec("CREATE TABLE test_cast (i INT)");
    ctx.exec("INSERT INTO test_cast VALUES (100)");

    let results = ctx.query("SELECT CAST(i AS TINYINT UNSIGNED) AS uint8_val FROM test_cast");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint8_val").unwrap(), "U8(100)");

    ctx.commit();
}
