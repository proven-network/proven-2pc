//! UPPER and LOWER function tests
//! Based on gluesql/test-suite/src/function/upper_lower.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;

fn setup_test_table(ctx: &mut common::TestContext) {
    TableBuilder::new(ctx, "Item").create_simple("name TEXT, opt_name TEXT");
    ctx.exec("INSERT INTO Item VALUES ('abcd', 'efgi'), ('Abcd', NULL), ('ABCD', 'EfGi')");
}

#[test]
fn test_lower_function_in_where_clause() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // LOWER in WHERE clause should find all case variations
    let results = ctx.query("SELECT name FROM Item WHERE LOWER(name) = 'abcd'");
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("abcd".to_string())
    );
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("Abcd".to_string())
    );
    assert_eq!(
        results[2].get("name").unwrap(),
        &Value::Str("ABCD".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_and_upper_function_basic() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    let results = ctx.query("SELECT LOWER(name), UPPER(name) FROM Item");
    assert_eq!(results.len(), 3);

    // All LOWER results should be 'abcd'
    assert_eq!(
        results[0].get("LOWER(name)").unwrap(),
        &Value::Str("abcd".to_string())
    );
    assert_eq!(
        results[1].get("LOWER(name)").unwrap(),
        &Value::Str("abcd".to_string())
    );
    assert_eq!(
        results[2].get("LOWER(name)").unwrap(),
        &Value::Str("abcd".to_string())
    );

    // All UPPER results should be 'ABCD'
    assert_eq!(
        results[0].get("UPPER(name)").unwrap(),
        &Value::Str("ABCD".to_string())
    );
    assert_eq!(
        results[1].get("UPPER(name)").unwrap(),
        &Value::Str("ABCD".to_string())
    );
    assert_eq!(
        results[2].get("UPPER(name)").unwrap(),
        &Value::Str("ABCD".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_and_upper_with_literals() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    let results =
        ctx.query("SELECT LOWER('Abcd') as lower, UPPER('abCd') as upper FROM Item LIMIT 1");
    assert_eq!(results.len(), 1);

    assert_eq!(
        results[0].get("lower").unwrap(),
        &Value::Str("abcd".to_string())
    );
    assert_eq!(
        results[0].get("upper").unwrap(),
        &Value::Str("ABCD".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_and_upper_with_null_values() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // SQL standard: NULL in -> NULL out
    let results = ctx.query("SELECT LOWER(opt_name), UPPER(opt_name) FROM Item");
    assert_eq!(results.len(), 3);

    // First row: 'efgi' -> lowercase and uppercase
    assert_eq!(
        results[0].get("LOWER(opt_name)").unwrap(),
        &Value::Str("efgi".to_string())
    );
    assert_eq!(
        results[0].get("UPPER(opt_name)").unwrap(),
        &Value::Str("EFGI".to_string())
    );

    // Second row: NULL -> NULL
    assert_eq!(results[1].get("LOWER(opt_name)").unwrap(), &Value::Null);
    assert_eq!(results[1].get("UPPER(opt_name)").unwrap(), &Value::Null);

    // Third row: 'EfGi' -> lowercase and uppercase
    assert_eq!(
        results[2].get("LOWER(opt_name)").unwrap(),
        &Value::Str("efgi".to_string())
    );
    assert_eq!(
        results[2].get("UPPER(opt_name)").unwrap(),
        &Value::Str("EFGI".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_function_wrong_type() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Should error on non-string type
    ctx.assert_error_contains("SELECT LOWER(1) FROM Item", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_upper_function_wrong_type() {
    let mut ctx = setup_test();
    setup_test_table(&mut ctx);

    // Should error on non-string type
    ctx.assert_error_contains("SELECT UPPER(1) FROM Item", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_lower_with_empty_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LOWER('') as empty");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("empty").unwrap(),
        &Value::Str("".to_string())
    );

    ctx.commit();
}

#[test]
fn test_upper_with_empty_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT UPPER('') as empty");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("empty").unwrap(),
        &Value::Str("".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_with_numbers_and_symbols() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT LOWER('ABC123!@#XYZ') as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("abc123!@#xyz".to_string())
    );

    ctx.commit();
}

#[test]
fn test_upper_with_numbers_and_symbols() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT UPPER('abc123!@#xyz') as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("ABC123!@#XYZ".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_with_unicode() {
    let mut ctx = setup_test();

    // Test with non-ASCII characters
    let results = ctx.query("SELECT LOWER('CAFÉ') as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("café".to_string())
    );

    ctx.commit();
}

#[test]
fn test_upper_with_unicode() {
    let mut ctx = setup_test();

    // Test with non-ASCII characters
    let results = ctx.query("SELECT UPPER('café') as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("CAFÉ".to_string())
    );

    ctx.commit();
}

#[test]
fn test_nested_upper_lower() {
    let mut ctx = setup_test();

    // Test nested function calls
    let results = ctx.query("SELECT UPPER(LOWER('MiXeD')) as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("MIXED".to_string())
    );

    let results = ctx.query("SELECT LOWER(UPPER('MiXeD')) as result");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("result").unwrap(),
        &Value::Str("mixed".to_string())
    );

    ctx.commit();
}

#[test]
fn test_lower_upper_with_null_literal() {
    let mut ctx = setup_test();

    // SQL standard: NULL in -> NULL out
    let results = ctx.query("SELECT LOWER(NULL) as lower_null, UPPER(NULL) as upper_null");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("lower_null").unwrap(), &Value::Null);
    assert_eq!(results[0].get("upper_null").unwrap(), &Value::Null);

    ctx.commit();
}
