//! FLOAT32 data type tests
//! Based on gluesql/test-suite/src/data_type/float32.rs
//! Note: In our SQL engine, REAL maps to F32 (32-bit float)

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_float32_columns() {
    let mut ctx = setup_test();

    // REAL is the SQL type for 32-bit floats (F32)
    ctx.exec("CREATE TABLE line (x REAL, y REAL)");
    ctx.exec("INSERT INTO line VALUES (0.3134, 0.156)");

    let results = ctx.query("SELECT x, y FROM line");
    assert_eq!(results.len(), 1);

    // Check that values are stored as F32
    let x = results[0].get("x").unwrap();
    let y = results[0].get("y").unwrap();

    // F32 values might have slight precision differences
    assert!(x.to_string().starts_with("0.3134"));
    assert!(y.to_string().starts_with("0.156"));

    ctx.commit();
}

#[test]
fn test_insert_float32_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float_test (id INT, value REAL)");

    // Test various float values
    ctx.exec("INSERT INTO float_test VALUES (1, 3.14159)");
    ctx.exec("INSERT INTO float_test VALUES (2, -2.71828)");
    ctx.exec("INSERT INTO float_test VALUES (3, 0.0)");
    ctx.exec("INSERT INTO float_test VALUES (4, 1000000.5)");
    ctx.exec("INSERT INTO float_test VALUES (5, 0.0000001)");

    let results = ctx.query("SELECT id, value FROM float_test ORDER BY id");
    assert_eq!(results.len(), 5);

    // F32 has limited precision, so exact matches might not work
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("3.14")
    );
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("-2.71")
    );
    // F32 might display 0 as either "F32(0)" or "F32(0.0)"
    let zero = results[2].get("value").unwrap();
    assert!(zero == &Value::F32(0.0) || zero.to_string().contains("0"));
    assert!(
        results[3]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1000000")
    );
    assert!(results[4].get("value").unwrap().is_float());

    ctx.commit();
}

#[test]
fn test_select_float32_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE line (x REAL, y REAL)");
    ctx.exec("INSERT INTO line VALUES (0.3134, 0.156)");

    let results = ctx.query("SELECT x, y FROM line");
    assert_eq!(results.len(), 1);

    // Verify values can be selected correctly
    assert!(results[0].get("x").unwrap().is_float());
    assert!(results[0].get("y").unwrap().is_float());

    ctx.commit();
}

#[test]
fn test_float32_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE floats (a REAL, b REAL)");
    ctx.exec("INSERT INTO floats VALUES (10.5, 2.5)");

    // Test addition
    let results = ctx.query("SELECT a + b AS sum FROM floats");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("sum").unwrap().to_string().contains("13"));

    // Test subtraction
    let results = ctx.query("SELECT a - b AS diff FROM floats");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("diff").unwrap().to_string().contains("8"));

    // Test multiplication
    let results = ctx.query("SELECT a * b AS product FROM floats");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("product")
            .unwrap()
            .to_string()
            .contains("26.25")
    );

    // Test division
    let results = ctx.query("SELECT a / b AS quotient FROM floats");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("quotient")
            .unwrap()
            .to_string()
            .contains("4.2")
    );

    ctx.commit();
}

#[test]
fn test_float32_comparison_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE line (x REAL, y REAL)");
    ctx.exec("INSERT INTO line VALUES (2.0, 1.0)");
    ctx.exec("INSERT INTO line VALUES (3.5, 2.5)");
    ctx.exec("INSERT INTO line VALUES (1.0, 3.0)");

    // Test equality
    let results = ctx.query("SELECT x, y FROM line WHERE x = 2.0");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("y").unwrap().to_string().contains("1"));

    // Test greater than
    let results = ctx.query("SELECT x FROM line WHERE x > 2.0 ORDER BY x");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("x").unwrap().to_string().contains("3.5"));

    // Test less than
    let results = ctx.query("SELECT x FROM line WHERE x < 2.0");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("x").unwrap().to_string().contains("1"));

    // Test UPDATE with WHERE
    ctx.exec("UPDATE line SET x = 5.0, y = 4.0 WHERE x = 2.0 AND y = 1.0");

    let results = ctx.query("SELECT x, y FROM line WHERE x = 5.0");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("y").unwrap().to_string().contains("4"));

    // Test DELETE with WHERE
    ctx.exec("DELETE FROM line WHERE x = 5.0 AND y = 4.0");

    let results = ctx.query("SELECT COUNT(*) as cnt FROM line");
    assert_eq!(results[0].get("cnt").unwrap(), &Value::I64(2));

    ctx.commit();
}

#[test]
fn test_float32_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE floats (value REAL)");
    ctx.exec("INSERT INTO floats VALUES (3.5), (1.2), (2.8), (0.5), (4.1)");

    // Test ascending order
    let results = ctx.query("SELECT value FROM floats ORDER BY value ASC");
    assert_eq!(results.len(), 5);

    // Check order (values should be 0.5, 1.2, 2.8, 3.5, 4.1)
    assert!(results[0].get("value").unwrap().to_string().contains("0.5"));
    assert!(results[1].get("value").unwrap().to_string().contains("1.2"));
    assert!(results[2].get("value").unwrap().to_string().contains("2.8"));
    assert!(results[3].get("value").unwrap().to_string().contains("3.5"));
    assert!(results[4].get("value").unwrap().to_string().contains("4.1"));

    // Test descending order
    let results = ctx.query("SELECT value FROM floats ORDER BY value DESC");
    assert_eq!(results.len(), 5);
    assert!(results[0].get("value").unwrap().to_string().contains("4.1"));
    assert!(results[4].get("value").unwrap().to_string().contains("0.5"));

    ctx.commit();
}

#[test]
fn test_float32_special_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE special_floats (id INT, value REAL)");

    // Test special float values - use keywords, not strings
    ctx.exec("INSERT INTO special_floats VALUES (1, NaN)");
    ctx.exec("INSERT INTO special_floats VALUES (2, Infinity)");
    ctx.exec("INSERT INTO special_floats VALUES (3, -Infinity)");

    let results = ctx.query("SELECT id, value FROM special_floats ORDER BY id");
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
fn test_cast_to_float32() {
    let mut ctx = setup_test();

    // Test CAST from string to REAL (F32)
    let results = ctx.query("SELECT CAST('-71.064544' AS REAL) AS float32");
    assert_eq!(results.len(), 1);

    let value = results[0].get("float32").unwrap();
    assert!(value.is_float());
    assert!(value.to_string().contains("-71.06"));

    // Test CAST from integer to REAL
    let results = ctx.query("SELECT CAST(42 AS REAL) AS float32");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("float32")
            .unwrap()
            .to_string()
            .contains("42")
    );

    // Test CAST from decimal to REAL
    ctx.exec("CREATE TABLE test (d DECIMAL)");
    ctx.exec("INSERT INTO test VALUES (3.14159265359)");

    let results = ctx.query("SELECT CAST(d AS REAL) AS float32 FROM test");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("float32")
            .unwrap()
            .to_string()
            .starts_with("3.14")
    );

    ctx.commit();
}

#[test]
fn test_float32_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE float_nulls (id INT, value REAL)");
    ctx.exec("INSERT INTO float_nulls VALUES (1, 1.5), (2, NULL), (3, 2.5)");

    let results =
        ctx.query("SELECT id, value FROM float_nulls WHERE value IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("value").unwrap().to_string().contains("1.5"));
    assert!(results[1].get("value").unwrap().to_string().contains("2.5"));

    // Test NULL propagation in arithmetic
    let results = ctx.query("SELECT id, value + 1.0 as result FROM float_nulls ORDER BY id");
    assert_eq!(results.len(), 3);
    assert!(
        results[0]
            .get("result")
            .unwrap()
            .to_string()
            .contains("2.5")
    );
    assert_eq!(results[1].get("result").unwrap(), &Value::Null);
    assert!(
        results[2]
            .get("result")
            .unwrap()
            .to_string()
            .contains("3.5")
    );

    ctx.commit();
}

#[test]
fn test_float32_precision() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE precision_test (value REAL)");

    // F32 has about 7 decimal digits of precision
    ctx.exec("INSERT INTO precision_test VALUES (1234567.8)");
    ctx.exec("INSERT INTO precision_test VALUES (0.12345678)");

    let results = ctx.query("SELECT value FROM precision_test ORDER BY value");
    assert_eq!(results.len(), 2);

    // Small number should maintain some precision
    assert!(
        results[0]
            .get("value")
            .unwrap()
            .to_string()
            .starts_with("0.123")
    );

    // Large number might lose precision in least significant digits
    assert!(
        results[1]
            .get("value")
            .unwrap()
            .to_string()
            .contains("1234567")
    );

    ctx.commit();
}

#[test]
fn test_nan_comparison_behavior() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE nan_test (id INT, value REAL)");
    ctx.exec("INSERT INTO nan_test VALUES (1, NaN), (2, 1.0), (3, NaN)");

    // NaN = NaN should return false (or NULL in SQL)
    let results = ctx.query("SELECT id FROM nan_test WHERE value = NaN");
    assert_eq!(results.len(), 0, "NaN = NaN should not match any rows");

    // NaN comparisons with regular numbers
    let results = ctx.query("SELECT id FROM nan_test WHERE value > 0");
    assert_eq!(
        results.len(),
        1,
        "Only regular numbers should match comparisons"
    );
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // IS NULL vs IS NOT NULL for NaN
    let results = ctx.query("SELECT id FROM nan_test WHERE value IS NULL");
    assert_eq!(results.len(), 0, "NaN is not NULL");

    let results = ctx.query("SELECT id FROM nan_test WHERE value IS NOT NULL");
    assert_eq!(results.len(), 3, "NaN is not NULL");

    // ORDER BY with NaN values
    let results = ctx.query("SELECT id, value FROM nan_test ORDER BY value");
    assert_eq!(results.len(), 3);
    // NaN values typically sort last or first depending on implementation
    // Just verify we get all 3 rows back

    ctx.commit();
}
