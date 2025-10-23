//! CURRENT_TIMESTAMP function tests
//! Based on gluesql/test-suite/src/function/current_timestamp.rs

use crate::common::{TableBuilder, setup_test};
use proven_value::Value;
#[test]
fn test_create_table_with_current_timestamp_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

    ctx.commit();
}

#[test]
fn test_insert_timestamp_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("timestamp TIMESTAMP")
        .insert_values("('2021-10-13 06:42:40'), ('9999-12-31 23:59:40')");

    ctx.assert_row_count("SELECT * FROM Item", 2);

    ctx.commit();
}

#[test]
fn test_current_timestamp_is_not_null() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT CURRENT_TIMESTAMP IS NOT NULL as is_not_null");

    assert_eq!(results.len(), 1);
    let value = results[0].get("is_not_null").unwrap();
    assert!(value.to_string().contains("true") || value == &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_current_timestamp_with_parentheses() {
    let mut ctx = setup_test();

    // Test that CURRENT_TIMESTAMP() with parentheses also works (for compatibility)
    let results = ctx.query("SELECT CURRENT_TIMESTAMP() IS NOT NULL as is_not_null");

    assert_eq!(results.len(), 1);
    let value = results[0].get("is_not_null").unwrap();
    assert!(value.to_string().contains("true") || value == &Value::Bool(true));

    ctx.commit();
}
