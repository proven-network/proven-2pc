//! BIGINT UNSIGNED data type tests
//! Based on gluesql/test-suite/src/data_type/uint64.rs

use crate::common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_uint64_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one BIGINT UNSIGNED, field_two BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U64(1));
    assert_eq!(results[0].get("field_two").unwrap(), &Value::U64(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U64(2));
    assert_eq!(results[1].get("field_two").unwrap(), &Value::U64(2));
    assert_eq!(results[2].get("field_one").unwrap(), &Value::U64(3));
    assert_eq!(results[2].get("field_two").unwrap(), &Value::U64(3));
    assert_eq!(results[3].get("field_one").unwrap(), &Value::U64(4));
    assert_eq!(results[3].get("field_two").unwrap(), &Value::U64(4));

    ctx.commit();
}

#[test]
fn test_insert_uint64_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_test (id INT, value BIGINT UNSIGNED)");

    // Test various BIGINT UNSIGNED values (0 to 18446744073709551615)
    ctx.exec("INSERT INTO uint64_test VALUES (1, 0)");
    ctx.exec("INSERT INTO uint64_test VALUES (2, 1)");
    ctx.exec("INSERT INTO uint64_test VALUES (3, 4294967295)");
    ctx.exec("INSERT INTO uint64_test VALUES (4, 4294967296)");
    ctx.exec("INSERT INTO uint64_test VALUES (5, 9223372036854775807)");
    ctx.exec("INSERT INTO uint64_test VALUES (6, 9223372036854775808)");
    ctx.exec("INSERT INTO uint64_test VALUES (7, 18446744073709551615)");

    let results = ctx.query("SELECT id, value FROM uint64_test ORDER BY id");
    assert_eq!(results.len(), 7);
    assert_eq!(results[0].get("value").unwrap(), &Value::U64(0));
    assert_eq!(results[1].get("value").unwrap(), &Value::U64(1));
    assert_eq!(results[2].get("value").unwrap(), &Value::U64(4294967295));
    assert_eq!(results[3].get("value").unwrap(), &Value::U64(4294967296));
    assert_eq!(
        results[4].get("value").unwrap(),
        &Value::U64(9223372036854775807)
    );
    assert_eq!(
        results[5].get("value").unwrap(),
        &Value::U64(9223372036854775808)
    );
    assert_eq!(
        results[6].get("value").unwrap(),
        &Value::U64(18446744073709551615)
    );

    ctx.commit();
}

#[test]
#[should_panic(expected = "Cannot convert -1 to BIGINT UNSIGNED")]
fn test_insert_uint64_negative_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_test (value BIGINT UNSIGNED)");
    // Negative values are not allowed for BIGINT UNSIGNED
    ctx.exec("INSERT INTO uint64_test VALUES (-1)");
}

#[test]
#[should_panic(expected = "Cannot convert 18446744073709551616 to BIGINT UNSIGNED")]
fn test_insert_uint64_overflow_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_test (value BIGINT UNSIGNED)");
    // Values > 18446744073709551615 are not allowed for BIGINT UNSIGNED
    ctx.exec("INSERT INTO uint64_test VALUES (18446744073709551616)");
}

#[test]
fn test_uint64_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one BIGINT UNSIGNED, field_two BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    // Test greater than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 2 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U64(3));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U64(4));

    // Test greater than or equal
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one >= 2 ORDER BY field_one");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U64(2));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U64(3));
    assert_eq!(results[2].get("field_one").unwrap(), &Value::U64(4));

    // Test equality
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U64(2));

    // Test less than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 3 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), &Value::U64(1));
    assert_eq!(results[1].get("field_one").unwrap(), &Value::U64(2));

    ctx.commit();
}

#[test]
fn test_uint64_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_math (a BIGINT UNSIGNED, b BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO uint64_math VALUES (1000000000, 300000000)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM uint64_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sum").unwrap(), &Value::U64(1300000000));

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM uint64_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("diff").unwrap(), &Value::U64(700000000));

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM uint64_math");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("product").unwrap(),
        &Value::U64(300000000000000000)
    );

    // Test division (integer division)
    let results = ctx.query("SELECT a / b AS quotient FROM uint64_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("quotient").unwrap(), &Value::U64(3));

    // Test modulo
    let results = ctx.query("SELECT a % b AS remainder FROM uint64_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("remainder").unwrap(), &Value::U64(100000000));

    ctx.commit();
}

#[test]
fn test_uint64_range_boundaries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_bounds (value BIGINT UNSIGNED)");

    // Test minimum value (0)
    ctx.exec("INSERT INTO uint64_bounds VALUES (0)");

    // Test maximum value (18446744073709551615)
    ctx.exec("INSERT INTO uint64_bounds VALUES (18446744073709551615)");

    let results = ctx.query("SELECT value FROM uint64_bounds ORDER BY value");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), &Value::U64(0));
    assert_eq!(
        results[1].get("value").unwrap(),
        &Value::U64(18446744073709551615)
    );

    // Test operations at boundaries
    ctx.exec("CREATE TABLE uint64_boundary_ops (a BIGINT UNSIGNED, b BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO uint64_boundary_ops VALUES (18446744073709551615, 0)");

    // Test 18446744073709551615 - 0 = 18446744073709551615
    let results = ctx.query("SELECT a - b AS diff FROM uint64_boundary_ops");
    assert_eq!(
        results[0].get("diff").unwrap(),
        &Value::U64(18446744073709551615)
    );

    ctx.commit();
}

#[test]
#[should_panic(expected = "overflow")]
fn test_uint64_addition_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_overflow (a BIGINT UNSIGNED, b BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO uint64_overflow VALUES (18446744073709551615, 1)");

    // This should cause an overflow
    ctx.query("SELECT a + b AS sum FROM uint64_overflow");
}

#[test]
#[should_panic(expected = "underflow")]
fn test_uint64_subtraction_underflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_underflow (a BIGINT UNSIGNED, b BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO uint64_underflow VALUES (0, 1)");

    // This should cause an underflow
    ctx.query("SELECT a - b AS diff FROM uint64_underflow");
}

#[test]
fn test_uint64_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint64_nulls (id INT, value BIGINT UNSIGNED)");
    ctx.exec("INSERT INTO uint64_nulls VALUES (1, 10000000000), (2, NULL), (3, 20000000000)");

    let results =
        ctx.query("SELECT id, value FROM uint64_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), &Value::U64(10000000000));
    assert_eq!(results[1].get("value").unwrap(), &Value::U64(20000000000));

    // Test NULL propagation in arithmetic
    let results =
        ctx.query("SELECT id, value + 5000000000 as result FROM uint64_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), &Value::U64(15000000000));
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert_eq!(results[2].get("result").unwrap(), &Value::U64(25000000000));

    ctx.commit();
}

#[test]
fn test_cast_to_uint64() {
    let mut ctx = setup_test();

    // Test CAST from string
    let results = ctx.query("SELECT CAST('12345678901' AS BIGINT UNSIGNED) AS uint64_val");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("uint64_val").unwrap(),
        &Value::U64(12345678901)
    );

    // Test CAST from integer
    let results = ctx.query("SELECT CAST(4200000000 AS BIGINT UNSIGNED) AS uint64_val");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("uint64_val").unwrap(),
        &Value::U64(4200000000)
    );

    // Test CAST from larger integer type
    ctx.exec("CREATE TABLE test_cast (i INT)");
    ctx.exec("INSERT INTO test_cast VALUES (100000000)");

    let results = ctx.query("SELECT CAST(i AS BIGINT UNSIGNED) AS uint64_val FROM test_cast");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("uint64_val").unwrap(),
        &Value::U64(100000000)
    );

    ctx.commit();
}
