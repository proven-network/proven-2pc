//! HUGEINT UNSIGNED data type tests
//! Based on gluesql/test-suite/src/data_type/uint128.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_uint128_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one HUGEINT UNSIGNED, field_two HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("field_one").unwrap(), "U128(1)");
    assert_eq!(results[0].get("field_two").unwrap(), "U128(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U128(2)");
    assert_eq!(results[1].get("field_two").unwrap(), "U128(2)");
    assert_eq!(results[2].get("field_one").unwrap(), "U128(3)");
    assert_eq!(results[2].get("field_two").unwrap(), "U128(3)");
    assert_eq!(results[3].get("field_one").unwrap(), "U128(4)");
    assert_eq!(results[3].get("field_two").unwrap(), "U128(4)");

    ctx.commit();
}

#[test]
fn test_insert_uint128_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_test (id INT, value HUGEINT UNSIGNED)");

    // Test various HUGEINT UNSIGNED values (0 to 340282366920938463463374607431768211455)
    ctx.exec("INSERT INTO uint128_test VALUES (1, 0)");
    ctx.exec("INSERT INTO uint128_test VALUES (2, 1)");
    ctx.exec("INSERT INTO uint128_test VALUES (3, 18446744073709551615)");
    ctx.exec("INSERT INTO uint128_test VALUES (4, 18446744073709551616)");
    ctx.exec("INSERT INTO uint128_test VALUES (5, 170141183460469231731687303715884105727)");
    ctx.exec("INSERT INTO uint128_test VALUES (6, 170141183460469231731687303715884105728)");
    ctx.exec("INSERT INTO uint128_test VALUES (7, 340282366920938463463374607431768211455)");

    let results = ctx.query("SELECT id, value FROM uint128_test ORDER BY id");
    assert_eq!(results.len(), 7);
    assert_eq!(results[0].get("value").unwrap(), "U128(0)");
    assert_eq!(results[1].get("value").unwrap(), "U128(1)");
    assert_eq!(
        results[2].get("value").unwrap(),
        "U128(18446744073709551615)"
    );
    assert_eq!(
        results[3].get("value").unwrap(),
        "U128(18446744073709551616)"
    );
    assert_eq!(
        results[4].get("value").unwrap(),
        "U128(170141183460469231731687303715884105727)"
    );
    assert_eq!(
        results[5].get("value").unwrap(),
        "U128(170141183460469231731687303715884105728)"
    );
    assert_eq!(
        results[6].get("value").unwrap(),
        "U128(340282366920938463463374607431768211455)"
    );

    ctx.commit();
}

#[test]
#[should_panic(expected = "Cannot convert -1 to HUGEINT UNSIGNED")]
fn test_insert_uint128_negative_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_test (value HUGEINT UNSIGNED)");
    // Negative values are not allowed for HUGEINT UNSIGNED
    ctx.exec("INSERT INTO uint128_test VALUES (-1)");
}

#[test]
#[should_panic(expected = "invalid integer")]
fn test_insert_uint128_overflow_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_test (value HUGEINT UNSIGNED)");
    // Values > 340282366920938463463374607431768211455 are not allowed for HUGEINT UNSIGNED
    ctx.exec("INSERT INTO uint128_test VALUES (340282366920938463463374607431768211456)");
}

#[test]
fn test_uint128_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one HUGEINT UNSIGNED, field_two HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    // Test greater than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 2 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U128(3)");
    assert_eq!(results[1].get("field_one").unwrap(), "U128(4)");

    // Test greater than or equal
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one >= 2 ORDER BY field_one");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("field_one").unwrap(), "U128(2)");
    assert_eq!(results[1].get("field_one").unwrap(), "U128(3)");
    assert_eq!(results[2].get("field_one").unwrap(), "U128(4)");

    // Test equality
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("field_one").unwrap(), "U128(2)");

    // Test less than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 3 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U128(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U128(2)");

    ctx.commit();
}

#[test]
fn test_uint128_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_math (a HUGEINT UNSIGNED, b HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO uint128_math VALUES (10000000000000000000, 3000000000000000000)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM uint128_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sum").unwrap(), "U128(13000000000000000000)");

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM uint128_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("diff").unwrap(), "U128(7000000000000000000)");

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM uint128_math");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("product").unwrap(),
        "U128(30000000000000000000000000000000000000)"
    );

    // Test division (integer division)
    let results = ctx.query("SELECT a / b AS quotient FROM uint128_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("quotient").unwrap(), "U128(3)");

    // Test modulo
    let results = ctx.query("SELECT a % b AS remainder FROM uint128_math");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("remainder").unwrap(),
        "U128(1000000000000000000)"
    );

    ctx.commit();
}

#[test]
fn test_uint128_range_boundaries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_bounds (value HUGEINT UNSIGNED)");

    // Test minimum value (0)
    ctx.exec("INSERT INTO uint128_bounds VALUES (0)");

    // Test maximum value (340282366920938463463374607431768211455)
    ctx.exec("INSERT INTO uint128_bounds VALUES (340282366920938463463374607431768211455)");

    let results = ctx.query("SELECT value FROM uint128_bounds ORDER BY value");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), "U128(0)");
    assert_eq!(
        results[1].get("value").unwrap(),
        "U128(340282366920938463463374607431768211455)"
    );

    // Test operations at boundaries
    ctx.exec("CREATE TABLE uint128_boundary_ops (a HUGEINT UNSIGNED, b HUGEINT UNSIGNED)");
    ctx.exec(
        "INSERT INTO uint128_boundary_ops VALUES (340282366920938463463374607431768211455, 0)",
    );

    // Test 340282366920938463463374607431768211455 - 0 = 340282366920938463463374607431768211455
    let results = ctx.query("SELECT a - b AS diff FROM uint128_boundary_ops");
    assert_eq!(
        results[0].get("diff").unwrap(),
        "U128(340282366920938463463374607431768211455)"
    );

    ctx.commit();
}

#[test]
#[should_panic(expected = "overflow")]
fn test_uint128_addition_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_overflow (a HUGEINT UNSIGNED, b HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO uint128_overflow VALUES (340282366920938463463374607431768211455, 1)");

    // This should cause an overflow
    ctx.query("SELECT a + b AS sum FROM uint128_overflow");
}

#[test]
#[should_panic(expected = "underflow")]
fn test_uint128_subtraction_underflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_underflow (a HUGEINT UNSIGNED, b HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO uint128_underflow VALUES (0, 1)");

    // This should cause an underflow
    ctx.query("SELECT a - b AS diff FROM uint128_underflow");
}

#[test]
fn test_uint128_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint128_nulls (id INT, value HUGEINT UNSIGNED)");
    ctx.exec("INSERT INTO uint128_nulls VALUES (1, 100000000000000000000), (2, NULL), (3, 200000000000000000000)");

    let results =
        ctx.query("SELECT id, value FROM uint128_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("value").unwrap(),
        "U128(100000000000000000000)"
    );
    assert_eq!(
        results[1].get("value").unwrap(),
        "U128(200000000000000000000)"
    );

    // Test NULL propagation in arithmetic
    let results = ctx
        .query("SELECT id, value + 50000000000000000000 as result FROM uint128_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("result").unwrap(),
        "U128(150000000000000000000)"
    );
    assert_eq!(results[1].get("result").unwrap(), "Null");
    assert_eq!(
        results[2].get("result").unwrap(),
        "U128(250000000000000000000)"
    );

    ctx.commit();
}

#[test]
fn test_cast_to_uint128() {
    let mut ctx = setup_test();

    // Test CAST from string
    let results =
        ctx.query("SELECT CAST('123456789012345678901' AS HUGEINT UNSIGNED) AS uint128_val");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("uint128_val").unwrap(),
        "U128(123456789012345678901)"
    );

    // Test CAST from integer
    let results = ctx.query("SELECT CAST(42000000000000000000 AS HUGEINT UNSIGNED) AS uint128_val");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("uint128_val").unwrap(),
        "U128(42000000000000000000)"
    );

    // Test CAST from larger integer type
    ctx.exec("CREATE TABLE test_cast (i INT)");
    ctx.exec("INSERT INTO test_cast VALUES (1000000000)");

    let results = ctx.query("SELECT CAST(i AS HUGEINT UNSIGNED) AS uint128_val FROM test_cast");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint128_val").unwrap(), "U128(1000000000)");

    ctx.commit();
}
