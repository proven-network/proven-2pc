//! BETWEEN expression tests
//! Based on gluesql/test-suite/src/expr/between.rs

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_equal_values() {
    // TODO: Test SELECT 0 BETWEEN 0 AND 0 - should return true
    // TODO: Test SELECT 1 BETWEEN 1 AND 1 - should return true
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_within_range() {
    // TODO: Test SELECT 2 BETWEEN 1 AND 3 - should return true (within range)
    // TODO: Test SELECT -1 BETWEEN -1 AND 1 - should return true (negative range)
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_outside_range() {
    // TODO: Test SELECT 1 BETWEEN 2 AND 3 - should return false (outside range)
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_boundary_cases() {
    // TODO: Test SELECT i128::MIN BETWEEN i128::MIN AND i128::MAX - should return true
    // TODO: Test SELECT i128::MAX BETWEEN i128::MIN AND i128::MAX - should return true
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_float_values() {
    // TODO: Test SELECT 2.5 BETWEEN 1.0 AND 3.0 - float between test
    // TODO: Test SELECT 0.5 BETWEEN 1.0 AND 3.0 - float outside range
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_date_values() {
    // TODO: Test SELECT '2020-06-15' BETWEEN '2020-01-01' AND '2020-12-31' - date between test
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_string_values() {
    // TODO: Test SELECT 'B' BETWEEN 'A' AND 'C' - string between test (alphabetical order)
}

#[ignore = "not yet implemented"]
#[test]
fn test_not_between_expressions() {
    // TODO: Test SELECT 1 NOT BETWEEN 2 AND 3 - should return true
    // TODO: Test SELECT 2 NOT BETWEEN 1 AND 3 - should return false
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_null_values() {
    // TODO: Test SELECT NULL BETWEEN 1 AND 3 - should return NULL
    // TODO: Test SELECT 2 BETWEEN NULL AND 3 - should return NULL
    // TODO: Test SELECT 2 BETWEEN 1 AND NULL - should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_column_references() {
    // TODO: Test BETWEEN with table column references
    // TODO: Test BETWEEN where bounds come from other columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_expressions() {
    // TODO: Test SELECT (1 + 1) BETWEEN (0 + 1) AND (2 + 1) - expressions in BETWEEN
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_subqueries() {
    // TODO: Test BETWEEN with subqueries as bounds if supported
}
