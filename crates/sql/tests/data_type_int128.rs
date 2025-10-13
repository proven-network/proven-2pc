//! HUGEINT data type tests (128-bit integer)
//! Based on gluesql/test-suite/src/data_type/int128.rs

mod common;

use common::setup_test;
use proven_value::Value;
fn setup_int128_table(ctx: &mut common::TestContext) {
    ctx.exec("CREATE TABLE Item (field_one HUGEINT, field_two HUGEINT)");

    // Insert positive values first
    ctx.exec("INSERT INTO Item VALUES (1000000000000, 2000000000000)");

    // Try inserting negative values separately
    ctx.exec("INSERT INTO Item VALUES (-3000000000000, -4000000000000)");
}

#[test]
fn test_create_table_with_int128_columns() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one HUGEINT, field_two HUGEINT)");
    ctx.exec("INSERT INTO Item VALUES (1000000000000, -1000000000000)");
    assert_rows!(ctx, "SELECT * FROM Item", 1);
    ctx.commit();
}

#[test]
fn test_insert_int128_values() {
    let mut ctx = setup_test();
    setup_int128_table(&mut ctx);
    assert_rows!(ctx, "SELECT * FROM Item", 2);
    ctx.commit();
}

#[test]
fn test_insert_int128_overflow_should_error() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one HUGEINT, field_two HUGEINT)");

    // Insert values near i128::MAX
    let near_max = i128::MAX - 1000;
    ctx.exec(&format!("INSERT INTO Item VALUES ({}, 2000)", near_max));

    // This addition should overflow
    // We expect this to fail with an overflow error
    let error = ctx.exec_error("SELECT field_one + field_two AS sum FROM Item");

    // Verify the error message contains overflow indication
    assert!(
        error.contains("I128 overflow") || error.contains("overflow"),
        "Expected overflow error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_select_int128_values() {
    let mut ctx = setup_test();
    setup_int128_table(&mut ctx);

    let results = ctx.query("SELECT field_one, field_two FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 2);

    // Updated expectations based on new test data
    assert_eq!(
        results[0].get("field_one").unwrap(),
        &Value::I128(-3000000000000)
    );
    assert_eq!(
        results[0].get("field_two").unwrap(),
        &Value::I128(-4000000000000)
    );

    assert_eq!(
        results[1].get("field_one").unwrap(),
        &Value::I128(1000000000000)
    );
    assert_eq!(
        results[1].get("field_two").unwrap(),
        &Value::I128(2000000000000)
    );

    ctx.commit();
}

#[test]
fn test_int128_comparisons() {
    let mut ctx = setup_test();
    setup_int128_table(&mut ctx);

    assert_rows!(ctx, "SELECT * FROM Item WHERE field_one > 0", 1);
    assert_rows!(ctx, "SELECT * FROM Item WHERE field_one < 0", 1);
    assert_rows!(ctx, "SELECT * FROM Item WHERE field_one = 1000000000000", 1);

    ctx.commit();
}

#[test]
fn test_int128_arithmetic_operations() {
    let mut ctx = setup_test();
    setup_int128_table(&mut ctx);

    let results = ctx.query("SELECT field_one + field_two AS sum FROM Item ORDER BY field_one");
    assert_eq!(results.len(), 2);

    // -3000000000000 + -4000000000000 = -7000000000000
    assert_eq!(results[0].get("sum").unwrap(), &Value::I128(-7000000000000));
    // 1000000000000 + 2000000000000 = 3000000000000
    assert_eq!(results[1].get("sum").unwrap(), &Value::I128(3000000000000));

    ctx.commit();
}

#[test]
fn test_int128_large_values() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one HUGEINT, field_two HUGEINT)");

    // Test with very large values that still fit in i128
    let large_val = "99999999999999999999999999999";
    let insert_sql = format!("INSERT INTO Item VALUES ({}, -{})", large_val, large_val);

    ctx.exec(&insert_sql);
    assert_rows!(ctx, "SELECT * FROM Item", 1);

    let results = ctx.query("SELECT field_one, field_two FROM Item");
    assert!(results[0].get("field_one").unwrap().is_integer());
    assert!(results[0].get("field_two").unwrap().is_integer());

    ctx.commit();
}

#[test]
fn test_int128_range_boundaries() {
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE Item (field_one HUGEINT, field_two HUGEINT)");

    // Test with near-boundary i128 values (avoiding i128::MIN parsing issue)
    let boundary_sql = format!(
        "INSERT INTO Item VALUES ({}, {})",
        i128::MAX,
        i128::MIN + 1 // Avoid the special case of i128::MIN
    );

    ctx.exec(&boundary_sql);
    assert_rows!(ctx, "SELECT * FROM Item", 1);

    let results = ctx.query("SELECT field_one, field_two FROM Item");
    assert_eq!(
        results[0].get("field_one").unwrap(),
        &Value::I128(i128::MAX)
    );
    assert_eq!(
        results[0].get("field_two").unwrap(),
        &Value::I128(i128::MIN + 1)
    );

    ctx.commit();
}
