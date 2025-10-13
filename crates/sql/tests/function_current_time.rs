//! CURRENT_TIME function tests
//! Based on gluesql/test-suite/src/function/current_time.rs

mod common;

use common::{TableBuilder, setup_test};
use proven_value::Value;
#[test]
fn test_create_table_with_current_time_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (time TIME DEFAULT CURRENT_TIME)");

    ctx.commit();
}

#[test]
fn test_insert_time_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("time TIME")
        .insert_values("('06:42:40'), ('23:59:59')");

    ctx.assert_row_count("SELECT * FROM Item", 2);

    ctx.commit();
}

#[test]
fn test_current_time_is_not_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT CURRENT_TIME IS NOT NULL as is_not_null");

    assert_eq!(results.len(), 1);
    let value = results[0].get("is_not_null").unwrap();
    // Should be Bool(true) or similar representation
    assert!(value.to_string().contains("true") || value == &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_current_time_in_valid_range() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT CURRENT_TIME >= TIME '00:00:00' AND CURRENT_TIME <= TIME '23:59:59' as is_valid_range"
    );

    assert_eq!(results.len(), 1);
    let value = results[0].get("is_valid_range").unwrap();
    // Should be Bool(true) or similar representation
    assert!(value.to_string().contains("true") || value == &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_current_time_with_parentheses() {
    let mut ctx = setup_test();

    // Test that CURRENT_TIME() with parentheses also works (for compatibility)
    let results = ctx.query("SELECT CURRENT_TIME() IS NOT NULL as is_not_null");

    assert_eq!(results.len(), 1);
    let value = results[0].get("is_not_null").unwrap();
    assert!(value.to_string().contains("true") || value == &Value::Bool(true));

    ctx.commit();
}
