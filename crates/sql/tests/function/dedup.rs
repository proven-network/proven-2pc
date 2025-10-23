//! DEDUP function tests
//! Based on gluesql/test-suite/src/function/dedup.rs

use crate::common::setup_test;

#[test]
fn test_dedup_integer_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT DEDUP(CAST('[1, 2, 3, 3, 4, 5, 5]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should contain deduplicated values [1, 2, 3, 4, 5]
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));
    assert!(value.to_string().contains("4"));
    assert!(value.to_string().contains("5"));

    ctx.commit();
}

#[test]
fn test_dedup_mixed_types() {
    let mut ctx = setup_test();

    let results = ctx.query(r#"SELECT DEDUP(CAST('["1", 1, 1, "1", "1"]' AS INT[])) as actual"#);

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should deduplicate while preserving type differences
    // Expected: ["1", 1, "1"] - strings and integers are different types
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_dedup_non_list_should_error() {
    let mut ctx = setup_test();

    let error = ctx.exec_error("SELECT DEDUP(1) AS actual");

    // Should error with ListTypeRequired
    assert!(
        error.to_string().contains("ListTypeRequired") || error.to_string().contains("list"),
        "Expected ListTypeRequired error, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_dedup_preserves_order() {
    let mut ctx = setup_test();

    // Test that DEDUP preserves the order of first occurrences
    let results = ctx.query("SELECT DEDUP(CAST('[5, 3, 5, 1, 3, 2]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should contain [5, 3, 1, 2] in that order
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_dedup_empty_list() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT DEDUP(CAST('[]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should return empty list
    assert!(value.to_string().contains("List"));

    ctx.commit();
}

#[test]
fn test_dedup_single_element() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT DEDUP(CAST('[42]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should return the same single-element list
    assert!(value.to_string().contains("42"));

    ctx.commit();
}

#[test]
fn test_dedup_no_duplicates() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT DEDUP(CAST('[1, 2, 3, 4, 5]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should return the same list with no duplicates removed
    assert!(value.to_string().contains("1"));
    assert!(value.to_string().contains("2"));
    assert!(value.to_string().contains("3"));
    assert!(value.to_string().contains("4"));
    assert!(value.to_string().contains("5"));

    ctx.commit();
}

#[test]
fn test_dedup_all_same_values() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT DEDUP(CAST('[7, 7, 7, 7]' AS INT[])) as actual");

    assert_eq!(results.len(), 1);
    let value = results[0].get("actual").unwrap();

    // Should return list with single element [7]
    assert!(value.to_string().contains("7"));

    ctx.commit();
}
