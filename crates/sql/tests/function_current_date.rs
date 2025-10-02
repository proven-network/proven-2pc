//! CURRENT_DATE function tests
//! Based on gluesql/test-suite/src/function/current_date.rs

mod common;

use common::{TableBuilder, setup_test};

#[test]
fn test_create_table_with_current_date_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Item (date DATE DEFAULT CURRENT_DATE)");

    ctx.commit();
}

#[test]
fn test_insert_date_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("date DATE")
        .insert_values("('2021-06-15'), ('9999-12-31')");

    ctx.assert_row_count("SELECT * FROM Item", 2);

    ctx.commit();
}

#[test]
fn test_filter_by_current_date() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Item")
        .create_simple("date DATE")
        .insert_values("('2020-01-01'), ('9999-12-31')");

    // Query for dates in the future (9999-12-31 should be greater than CURRENT_DATE)
    let results = ctx.query("SELECT date FROM Item WHERE date > CURRENT_DATE");

    // Should return at least the far future date
    assert!(!results.is_empty(), "Should find at least one future date");

    // Verify at least one result contains the far future date
    let has_future_date = results.iter().any(|row| {
        let date_value = row.get("date").unwrap();
        date_value.contains("9999")
    });
    assert!(
        has_future_date,
        "Should include the far future date (9999-12-31)"
    );

    ctx.commit();
}

#[test]
fn test_current_date_basic() {
    let mut ctx = setup_test();

    // Just verify CURRENT_DATE returns something that looks like a date
    let results = ctx.query("SELECT CURRENT_DATE AS today");

    assert_eq!(results.len(), 1);
    let date_str = results[0].get("today").unwrap();
    // Should contain a date value (basic validation)
    assert!(date_str.contains("Date") || date_str.contains("-"));

    ctx.commit();
}

#[test]
fn test_current_date_with_parentheses() {
    let mut ctx = setup_test();

    // Test that CURRENT_DATE() with parentheses also works (for compatibility)
    let results = ctx.query("SELECT CURRENT_DATE() AS today");

    assert_eq!(results.len(), 1);
    let date_str = results[0].get("today").unwrap();
    // Should contain a date value (basic validation)
    assert!(date_str.contains("Date") || date_str.contains("-"));

    ctx.commit();
}
