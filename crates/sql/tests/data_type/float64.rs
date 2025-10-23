//! FLOAT64 data type tests
//! Note: In our SQL engine, FLOAT and DOUBLE both map to F64 (64-bit float)

use crate::common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_float64_columns() {
    let mut ctx = setup_test();

    // FLOAT and DOUBLE are both 64-bit floats (F64)
    ctx.exec("CREATE TABLE coordinates (x FLOAT, y DOUBLE)");
    ctx.exec("INSERT INTO coordinates VALUES (0.123456789012345, 0.987654321098765)");

    let results = ctx.query("SELECT x, y FROM coordinates");
    assert_eq!(results.len(), 1);

    // Check that values are stored as F64 with higher precision than F32
    let x = results[0].get("x").unwrap();
    let y = results[0].get("y").unwrap();

    // F64 has much better precision than F32
    assert!(x.to_string().starts_with("0.123456789"));
    assert!(y.to_string().starts_with("0.987654321"));

    ctx.commit();
}

#[test]
fn test_insert_float64_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float64_test (id INT, value FLOAT)");

    // Test various float values with higher precision
    ctx.exec("INSERT INTO float64_test VALUES (1, 3.141592653589793)"); // Pi
    ctx.exec("INSERT INTO float64_test VALUES (2, -2.718281828459045)"); // -e
    ctx.exec("INSERT INTO float64_test VALUES (3, 0.0)");
    ctx.exec("INSERT INTO float64_test VALUES (4, 1234567890.123456789)");
    ctx.exec("INSERT INTO float64_test VALUES (5, 0.0000000000000001)");

    let results = ctx.query("SELECT id, value FROM float64_test ORDER BY id");
    assert_eq!(results.len(), 5);

    // F64 maintains much higher precision than F32
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("3.14159265")
    );
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("-2.71828182")
    );

    // Zero might be displayed as 0 or 0.0
    let zero = results[2].get("value").unwrap();
    assert!(zero == &Value::F64(0.0) || zero.to_string().contains("0"));

    assert!(
        results[3]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1234567890")
    );
    assert!(results[4].get("value").unwrap().is_float());

    ctx.commit();
}

#[test]
fn test_float64_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float64_values (a DOUBLE, b DOUBLE)");
    ctx.exec("INSERT INTO float64_values VALUES (10.123456789, 2.987654321)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM float64_values");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("sum")
            .unwrap()
            .to_string()
            .starts_with("13.11111111")
    );

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM float64_values");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("diff")
            .unwrap()
            .to_string()
            .starts_with("7.135802468")
    );

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM float64_values");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("product")
            .unwrap()
            .to_string()
            .starts_with("30.2453894")
    );

    // Test division
    let results = ctx.query("SELECT a / b AS quotient FROM float64_values");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("quotient")
            .unwrap()
            .to_string()
            .starts_with("3.3884297")
    );

    ctx.commit();
}

#[test]
fn test_float64_comparison_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float64_data (x FLOAT, y FLOAT)");
    ctx.exec("INSERT INTO float64_data VALUES (2.123456789, 1.987654321)");
    ctx.exec("INSERT INTO float64_data VALUES (3.555555555, 2.444444444)");
    ctx.exec("INSERT INTO float64_data VALUES (1.111111111, 3.333333333)");

    // Test equality with high precision
    let results = ctx.query("SELECT x, y FROM float64_data WHERE x = 2.123456789");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("y").unwrap().to_string().contains("1.98765"));

    // Test greater than
    let results = ctx.query("SELECT x FROM float64_data WHERE x > 2.0 ORDER BY x");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("x").unwrap().to_string().contains("2.12345"));
    assert!(results[1].get("x").unwrap().to_string().contains("3.55555"));

    // Test UPDATE with WHERE using precise values
    ctx.exec("UPDATE float64_data SET x = 5.999999999, y = 4.888888888 WHERE x = 2.123456789");

    let results = ctx.query("SELECT x, y FROM float64_data WHERE x = 5.999999999");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("y").unwrap().to_string().contains("4.88888"));

    ctx.commit();
}

#[test]
fn test_float64_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float64_order (value DOUBLE)");
    ctx.exec("INSERT INTO float64_order VALUES (3.141592653589793)");
    ctx.exec("INSERT INTO float64_order VALUES (1.414213562373095)"); // sqrt(2)
    ctx.exec("INSERT INTO float64_order VALUES (2.718281828459045)"); // e
    ctx.exec("INSERT INTO float64_order VALUES (0.577215664901532)"); // Euler-Mascheroni
    ctx.exec("INSERT INTO float64_order VALUES (1.618033988749895)"); // Golden ratio

    // Test ascending order
    let results = ctx.query("SELECT value FROM float64_order ORDER BY value ASC");
    assert_eq!(results.len(), 5);

    // Check values maintain precision in ordering
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .contains("0.57721")
    );
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1.41421")
    );
    assert!(
        results[2]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1.61803")
    );
    assert!(
        results[3]
            .get("value")
            .unwrap()
            .to_string()
            .contains("2.71828")
    );
    assert!(
        results[4]
            .get("value")
            .unwrap()
            .to_string()
            .contains("3.14159")
    );

    ctx.commit();
}

#[test]
fn test_float64_special_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE special_float64 (id INT, value DOUBLE)");

    // Test special float values
    ctx.exec("INSERT INTO special_float64 VALUES (1, NaN)");
    ctx.exec("INSERT INTO special_float64 VALUES (2, Infinity)");
    ctx.exec("INSERT INTO special_float64 VALUES (3, -Infinity)");

    let results = ctx.query("SELECT id, value FROM special_float64 ORDER BY id");
    assert_eq!(results.len(), 3);

    // NaN
    assert!(results[0].get("value").unwrap().to_string().contains("NaN"));

    // Infinity
    assert!(results[1].get("value").unwrap().to_string().contains("inf"));

    // -Infinity
    assert!(results[2].get("value").unwrap().to_string().contains("inf"));

    ctx.commit();
}

#[test]
fn test_cast_to_float64() {
    let mut ctx = setup_test();

    // Test CAST from string to FLOAT/DOUBLE (F64)
    let results = ctx.query("SELECT CAST('-123.456789012345' AS FLOAT) AS float64");
    assert_eq!(results.len(), 1);

    let value = results[0].get("float64").unwrap();
    assert!(value.is_float());
    assert!(value.to_string().contains("-123.45678"));

    // Test CAST from integer to DOUBLE
    let results = ctx.query("SELECT CAST(42 AS DOUBLE) AS float64");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("float64")
            .unwrap()
            .to_string()
            .contains("42")
    );

    // Test CAST from REAL (F32) to DOUBLE (F64)
    ctx.exec("CREATE TABLE test_cast (r REAL)");
    ctx.exec("INSERT INTO test_cast VALUES (3.14159)");

    let results = ctx.query("SELECT CAST(r AS DOUBLE) AS float64 FROM test_cast");
    assert_eq!(results.len(), 1);
    // F32 to F64 conversion preserves the F32 precision
    assert!(
        results[0]
            .get("float64")
            .unwrap()
            .to_string()
            .starts_with("3.14")
    );

    ctx.commit();
}

#[test]
fn test_float64_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float64_nulls (id INT, value DOUBLE)");
    ctx.exec("INSERT INTO float64_nulls VALUES (1, 1.123456789), (2, NULL), (3, 2.987654321)");

    let results =
        ctx.query("SELECT id, value FROM float64_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1.12345")
    );
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .contains("2.98765")
    );

    // Test NULL propagation in arithmetic
    let results =
        ctx.query("SELECT id, value + 1.111111111 as result FROM float64_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert!(
        results[0]
            .get("result")
            .unwrap()
            .to_string()
            .contains("2.234567")
    );
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert!(
        results[2]
            .get("result")
            .unwrap()
            .to_string()
            .contains("4.098765")
    );

    ctx.commit();
}

#[test]
fn test_float64_precision() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE precision64_test (value DOUBLE)");

    // F64 has about 15-17 decimal digits of precision
    ctx.exec("INSERT INTO precision64_test VALUES (123456789012345.6)");
    ctx.exec("INSERT INTO precision64_test VALUES (0.123456789012345678)");

    let results = ctx.query("SELECT value FROM precision64_test ORDER BY value");
    assert_eq!(results.len(), 2);

    // Small number should maintain high precision
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("0.123456789")
    );

    // Large number maintains more digits than F32
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .contains("123456789012345")
    );

    ctx.commit();
}

#[test]
fn test_float_vs_double_keywords() {
    let mut ctx = setup_test();

    // Test that both FLOAT and DOUBLE work and produce F64 values
    ctx.exec("CREATE TABLE float_double_test (f FLOAT, d DOUBLE)");
    ctx.exec("INSERT INTO float_double_test VALUES (1.23456789, 9.87654321)");

    let results = ctx.query("SELECT f, d FROM float_double_test");
    assert_eq!(results.len(), 1);

    // Both should be F64
    assert!(
        results[0]
            .get("f")
            .unwrap()
            .to_string()
            .starts_with("1.23456789")
    );
    assert!(
        results[0]
            .get("d")
            .unwrap()
            .to_string()
            .starts_with("9.87654321")
    );

    ctx.commit();
}

#[test]
fn test_float64_mixed_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE mixed_float64 (f FLOAT)");
    ctx.exec("INSERT INTO mixed_float64 VALUES (2.5)");

    // Test mixed operations with integers and decimals
    let results = ctx.query(
        "
        SELECT
            f + 1 AS f_plus_int,
            f * 2.0 AS f_times_decimal,
            f / 3 AS f_div_int
        FROM mixed_float64
    ",
    );

    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("f_plus_int")
            .unwrap()
            .to_string()
            .contains("3.5")
    );
    assert!(
        results[0]
            .get("f_times_decimal")
            .unwrap()
            .to_string()
            .contains("5")
    );
    assert!(
        results[0]
            .get("f_div_int")
            .unwrap()
            .to_string()
            .contains("0.833")
    );

    ctx.commit();
}

#[test]
fn test_float64_nan_comparison_behavior() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE nan64_test (id INT, value DOUBLE)");
    ctx.exec("INSERT INTO nan64_test VALUES (1, NaN), (2, 1.5), (3, NaN), (4, 2.5)");

    // First check what NaN = NaN evaluates to
    let results = ctx.query("SELECT NaN = NaN as result");
    assert_eq!(results[0].get("result").unwrap(), &Value::Bool(false));

    // NaN = NaN should return false (no matches)
    let results = ctx.query("SELECT id FROM nan64_test WHERE value = NaN");
    assert_eq!(results.len(), 0, "NaN = NaN should not match any rows");

    // Regular comparisons should exclude NaN
    let results = ctx.query("SELECT id FROM nan64_test WHERE value > 0 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(4));

    // NaN is not NULL
    let results = ctx.query("SELECT id FROM nan64_test WHERE value IS NOT NULL");
    assert_eq!(results.len(), 4, "NaN is not NULL");

    ctx.commit();
}
