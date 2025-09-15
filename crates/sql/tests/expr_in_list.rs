//! IN list expression tests
//! Based on gluesql/test-suite/src/expr/in_list.rs

mod common;
use common::test_query;

#[test]
fn test_in_with_integer_list() {
    // Test value in list - should return true
    test_query("SELECT 1 IN (1, 2, 3) AS result", vec![vec!["true"]]);

    // Test value not in list - should return false
    test_query("SELECT 4 IN (1, 2, 3) AS result", vec![vec!["false"]]);

    // Test with negative numbers
    test_query(
        "SELECT -1 IN (-3, -2, -1, 0, 1) AS result",
        vec![vec!["true"]],
    );
}

#[test]
fn test_in_with_string_list() {
    // Test string in list - should return true
    test_query(
        "SELECT 'apple' IN ('apple', 'banana', 'cherry') AS result",
        vec![vec!["true"]],
    );

    // Test string not in list - should return false
    test_query(
        "SELECT 'grape' IN ('apple', 'banana', 'cherry') AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_in_with_float_list() {
    // Test float in list - should return true
    test_query(
        "SELECT 2.5 IN (1.5, 2.5, 3.5) AS result",
        vec![vec!["true"]],
    );

    // Test float not in list - should return false
    test_query(
        "SELECT 4.5 IN (1.5, 2.5, 3.5) AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_not_in_with_list() {
    // Test value not in list - should return true
    test_query("SELECT 1 NOT IN (2, 3, 4) AS result", vec![vec!["true"]]);

    // Test value in list with NOT IN - should return false
    test_query("SELECT 1 NOT IN (1, 2, 3) AS result", vec![vec!["false"]]);
}

#[test]
fn test_in_with_null_values() {
    // NULL IN (...) should return NULL
    test_query("SELECT NULL IN (1, 2, 3) AS result", vec![vec!["NULL"]]);

    // Value found in list with NULL - should return true
    test_query("SELECT 1 IN (1, NULL, 3) AS result", vec![vec!["true"]]);

    // Value not found in list with NULL - should return NULL
    test_query("SELECT 4 IN (1, NULL, 3) AS result", vec![vec!["NULL"]]);
}

#[test]
fn test_in_with_single_value() {
    // Single value list - match
    test_query("SELECT 1 IN (1) AS result", vec![vec!["true"]]);

    // Single value list - no match
    test_query("SELECT 2 IN (1) AS result", vec![vec!["false"]]);
}

#[test]
fn test_in_with_empty_list() {
    // Empty list should always return false
    test_query("SELECT 1 IN () AS result", vec![vec!["false"]]);
}

#[test]
fn test_in_with_duplicate_values() {
    // List with duplicates - value found
    test_query("SELECT 1 IN (1, 1, 2) AS result", vec![vec!["true"]]);

    // List with duplicates - value not found
    test_query("SELECT 3 IN (1, 1, 2, 2) AS result", vec![vec!["false"]]);
}

#[test]
fn test_in_with_expressions() {
    // Expression on left side
    test_query("SELECT (1 + 1) IN (2, 3, 4) AS result", vec![vec!["true"]]);

    // Expressions in list
    test_query(
        "SELECT 2 IN (1 + 1, 2 + 1, 3 + 1) AS result",
        vec![vec!["true"]],
    );
    test_query(
        "SELECT 5 IN (1 + 1, 2 + 1, 3 + 1) AS result",
        vec![vec!["false"]],
    );
}

#[ignore = "IN with subquery not yet implemented"]
#[test]
fn test_in_with_subquery() {
    // This test would need table setup first
    // TODO: Test SELECT 1 IN (SELECT id FROM table) - subquery in IN clause
}

#[test]
fn test_in_with_different_data_types() {
    // Test implicit type conversion if supported
    test_query("SELECT 1 IN (1.0, 2.0, 3.0) AS result", vec![vec!["true"]]);

    // Test with mixed numeric types
    test_query("SELECT 2.0 IN (1, 2, 3) AS result", vec![vec!["true"]]);
}
