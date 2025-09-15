//! BETWEEN expression tests
//! Based on gluesql/test-suite/src/expr/between.rs

mod common;
use common::test_query;

#[test]
fn test_between_with_equal_values() {
    // Value equals both bounds
    test_query("SELECT 0 BETWEEN 0 AND 0 AS result", vec![vec!["true"]]);
    test_query("SELECT 1 BETWEEN 1 AND 1 AS result", vec![vec!["true"]]);

    // Value not equal to single-value range
    test_query("SELECT 2 BETWEEN 1 AND 1 AS result", vec![vec!["false"]]);
}

#[test]
fn test_between_within_range() {
    // Value within range
    test_query("SELECT 2 BETWEEN 1 AND 3 AS result", vec![vec!["true"]]);

    // Negative range
    test_query("SELECT -1 BETWEEN -1 AND 1 AS result", vec![vec!["true"]]);
    test_query("SELECT 0 BETWEEN -1 AND 1 AS result", vec![vec!["true"]]);
}

#[test]
fn test_between_outside_range() {
    // Value below range
    test_query("SELECT 1 BETWEEN 2 AND 3 AS result", vec![vec!["false"]]);

    // Value above range
    test_query("SELECT 4 BETWEEN 1 AND 3 AS result", vec![vec!["false"]]);
}

#[test]
fn test_between_boundary_cases() {
    // Test with extreme values
    test_query(
        "SELECT -9223372036854775808 BETWEEN -9223372036854775808 AND 9223372036854775807 AS result",
        vec![vec!["true"]],
    );
    test_query(
        "SELECT 9223372036854775807 BETWEEN -9223372036854775808 AND 9223372036854775807 AS result",
        vec![vec!["true"]],
    );
}

#[test]
fn test_between_with_float_values() {
    // Float within range
    test_query(
        "SELECT 2.5 BETWEEN 1.0 AND 3.0 AS result",
        vec![vec!["true"]],
    );

    // Float outside range
    test_query(
        "SELECT 0.5 BETWEEN 1.0 AND 3.0 AS result",
        vec![vec!["false"]],
    );
    test_query(
        "SELECT 3.5 BETWEEN 1.0 AND 3.0 AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_between_with_date_values() {
    // Date within range
    test_query(
        "SELECT DATE '2020-06-15' BETWEEN DATE '2020-01-01' AND DATE '2020-12-31' AS result",
        vec![vec!["true"]],
    );

    // Date outside range
    test_query(
        "SELECT DATE '2019-12-31' BETWEEN DATE '2020-01-01' AND DATE '2020-12-31' AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_between_with_string_values() {
    // String within alphabetical range
    test_query(
        "SELECT 'B' BETWEEN 'A' AND 'C' AS result",
        vec![vec!["true"]],
    );
    test_query(
        "SELECT 'Bob' BETWEEN 'Alice' AND 'Charlie' AS result",
        vec![vec!["true"]],
    );

    // String outside alphabetical range
    test_query(
        "SELECT 'D' BETWEEN 'A' AND 'C' AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_not_between_expressions() {
    // Value not in range - should return true
    test_query("SELECT 1 NOT BETWEEN 2 AND 3 AS result", vec![vec!["true"]]);
    test_query("SELECT 4 NOT BETWEEN 1 AND 3 AS result", vec![vec!["true"]]);

    // Value in range with NOT BETWEEN - should return false
    test_query(
        "SELECT 2 NOT BETWEEN 1 AND 3 AS result",
        vec![vec!["false"]],
    );
}

#[test]
fn test_between_with_null_values() {
    // NULL as test value
    test_query("SELECT NULL BETWEEN 1 AND 3 AS result", vec![vec!["NULL"]]);

    // NULL as lower bound
    test_query("SELECT 2 BETWEEN NULL AND 3 AS result", vec![vec!["NULL"]]);

    // NULL as upper bound
    test_query("SELECT 2 BETWEEN 1 AND NULL AS result", vec![vec!["NULL"]]);

    // Both bounds NULL
    test_query(
        "SELECT 2 BETWEEN NULL AND NULL AS result",
        vec![vec!["NULL"]],
    );
}

#[ignore = "BETWEEN with columns not yet implemented"]
#[test]
fn test_between_with_column_references() {
    // This test would need table setup first
    // TODO: Test BETWEEN with table column references
}

#[test]
fn test_between_with_expressions() {
    // Expressions in all positions
    test_query(
        "SELECT (1 + 1) BETWEEN (0 + 1) AND (2 + 1) AS result",
        vec![vec!["true"]],
    );
    test_query(
        "SELECT (5 - 3) BETWEEN (1 * 1) AND (1 + 2) AS result",
        vec![vec!["true"]],
    );

    // Expression result outside range
    test_query(
        "SELECT (10 / 2) BETWEEN 1 AND 3 AS result",
        vec![vec!["false"]],
    );
}

#[ignore = "BETWEEN with subqueries not yet implemented"]
#[test]
fn test_between_with_subqueries() {
    // This test would need table setup first
    // TODO: Test BETWEEN with subqueries as bounds if supported
}
