//! INT UNSIGNED data type tests
//! Based on gluesql/test-suite/src/data_type/uint32.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_uint32_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one INT UNSIGNED, field_two INT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("field_one").unwrap(), "U32(1)");
    assert_eq!(results[0].get("field_two").unwrap(), "U32(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U32(2)");
    assert_eq!(results[1].get("field_two").unwrap(), "U32(2)");
    assert_eq!(results[2].get("field_one").unwrap(), "U32(3)");
    assert_eq!(results[2].get("field_two").unwrap(), "U32(3)");
    assert_eq!(results[3].get("field_one").unwrap(), "U32(4)");
    assert_eq!(results[3].get("field_two").unwrap(), "U32(4)");

    ctx.commit();
}

#[test]
fn test_insert_uint32_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_test (id INT, value INT UNSIGNED)");

    // Test various INT UNSIGNED values (0 to 4294967295)
    ctx.exec("INSERT INTO uint32_test VALUES (1, 0)");
    ctx.exec("INSERT INTO uint32_test VALUES (2, 1)");
    ctx.exec("INSERT INTO uint32_test VALUES (3, 65535)");
    ctx.exec("INSERT INTO uint32_test VALUES (4, 65536)");
    ctx.exec("INSERT INTO uint32_test VALUES (5, 2147483647)");
    ctx.exec("INSERT INTO uint32_test VALUES (6, 2147483648)");
    ctx.exec("INSERT INTO uint32_test VALUES (7, 4294967295)");

    let results = ctx.query("SELECT id, value FROM uint32_test ORDER BY id");
    assert_eq!(results.len(), 7);
    assert_eq!(results[0].get("value").unwrap(), "U32(0)");
    assert_eq!(results[1].get("value").unwrap(), "U32(1)");
    assert_eq!(results[2].get("value").unwrap(), "U32(65535)");
    assert_eq!(results[3].get("value").unwrap(), "U32(65536)");
    assert_eq!(results[4].get("value").unwrap(), "U32(2147483647)");
    assert_eq!(results[5].get("value").unwrap(), "U32(2147483648)");
    assert_eq!(results[6].get("value").unwrap(), "U32(4294967295)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "Cannot convert -1 to INT UNSIGNED")]
fn test_insert_uint32_negative_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_test (value INT UNSIGNED)");
    // Negative values are not allowed for INT UNSIGNED
    ctx.exec("INSERT INTO uint32_test VALUES (-1)");
}

#[test]
#[should_panic(expected = "Cannot convert 4294967296 to INT UNSIGNED")]
fn test_insert_uint32_overflow_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_test (value INT UNSIGNED)");
    // Values > 4294967295 are not allowed for INT UNSIGNED
    ctx.exec("INSERT INTO uint32_test VALUES (4294967296)");
}

#[test]
fn test_uint32_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (field_one INT UNSIGNED, field_two INT UNSIGNED)");
    ctx.exec("INSERT INTO Item VALUES (1, 1), (2, 2), (3, 3), (4, 4)");

    // Test greater than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one > 2 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U32(3)");
    assert_eq!(results[1].get("field_one").unwrap(), "U32(4)");

    // Test greater than or equal
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one >= 2 ORDER BY field_one");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("field_one").unwrap(), "U32(2)");
    assert_eq!(results[1].get("field_one").unwrap(), "U32(3)");
    assert_eq!(results[2].get("field_one").unwrap(), "U32(4)");

    // Test equality
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("field_one").unwrap(), "U32(2)");

    // Test less than
    let results = ctx.query("SELECT field_one FROM Item WHERE field_one < 3 ORDER BY field_one");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("field_one").unwrap(), "U32(1)");
    assert_eq!(results[1].get("field_one").unwrap(), "U32(2)");

    ctx.commit();
}

#[test]
fn test_uint32_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_math (a INT UNSIGNED, b INT UNSIGNED)");
    ctx.exec("INSERT INTO uint32_math VALUES (100000, 30000)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM uint32_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("sum").unwrap(), "U32(130000)");

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM uint32_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("diff").unwrap(), "U32(70000)");

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM uint32_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("product").unwrap(), "U32(3000000000)");

    // Test division (integer division)
    let results = ctx.query("SELECT a / b AS quotient FROM uint32_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("quotient").unwrap(), "U32(3)");

    // Test modulo
    let results = ctx.query("SELECT a % b AS remainder FROM uint32_math");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("remainder").unwrap(), "U32(10000)");

    ctx.commit();
}

#[test]
fn test_uint32_range_boundaries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_bounds (value INT UNSIGNED)");

    // Test minimum value (0)
    ctx.exec("INSERT INTO uint32_bounds VALUES (0)");

    // Test maximum value (4294967295)
    ctx.exec("INSERT INTO uint32_bounds VALUES (4294967295)");

    let results = ctx.query("SELECT value FROM uint32_bounds ORDER BY value");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), "U32(0)");
    assert_eq!(results[1].get("value").unwrap(), "U32(4294967295)");

    // Test operations at boundaries
    ctx.exec("CREATE TABLE uint32_boundary_ops (a INT UNSIGNED, b INT UNSIGNED)");
    ctx.exec("INSERT INTO uint32_boundary_ops VALUES (4294967295, 0)");

    // Test 4294967295 - 0 = 4294967295
    let results = ctx.query("SELECT a - b AS diff FROM uint32_boundary_ops");
    assert_eq!(results[0].get("diff").unwrap(), "U32(4294967295)");

    ctx.commit();
}

#[test]
#[should_panic(expected = "overflow")]
fn test_uint32_addition_overflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_overflow (a INT UNSIGNED, b INT UNSIGNED)");
    ctx.exec("INSERT INTO uint32_overflow VALUES (4294967295, 1)");

    // This should cause an overflow
    ctx.query("SELECT a + b AS sum FROM uint32_overflow");
}

#[test]
#[should_panic(expected = "underflow")]
fn test_uint32_subtraction_underflow() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_underflow (a INT UNSIGNED, b INT UNSIGNED)");
    ctx.exec("INSERT INTO uint32_underflow VALUES (0, 1)");

    // This should cause an underflow
    ctx.query("SELECT a - b AS diff FROM uint32_underflow");
}

#[test]
fn test_uint32_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uint32_nulls (id INT, value INT UNSIGNED)");
    ctx.exec("INSERT INTO uint32_nulls VALUES (1, 100000), (2, NULL), (3, 200000)");

    let results =
        ctx.query("SELECT id, value FROM uint32_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("value").unwrap(), "U32(100000)");
    assert_eq!(results[1].get("value").unwrap(), "U32(200000)");

    // Test NULL propagation in arithmetic
    let results = ctx.query("SELECT id, value + 50000 as result FROM uint32_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), "U32(150000)");
    assert_eq!(results[1].get("result").unwrap(), "Null");
    assert_eq!(results[2].get("result").unwrap(), "U32(250000)");

    ctx.commit();
}

#[test]
fn test_cast_to_uint32() {
    let mut ctx = setup_test();

    // Test CAST from string
    let results = ctx.query("SELECT CAST('1234567' AS INT UNSIGNED) AS uint32_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint32_val").unwrap(), "U32(1234567)");

    // Test CAST from integer
    let results = ctx.query("SELECT CAST(4200000 AS INT UNSIGNED) AS uint32_val");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint32_val").unwrap(), "U32(4200000)");

    // Test CAST from larger integer type
    ctx.exec("CREATE TABLE test_cast (i INT)");
    ctx.exec("INSERT INTO test_cast VALUES (1000000)");

    let results = ctx.query("SELECT CAST(i AS INT UNSIGNED) AS uint32_val FROM test_cast");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("uint32_val").unwrap(), "U32(1000000)");

    ctx.commit();
}
