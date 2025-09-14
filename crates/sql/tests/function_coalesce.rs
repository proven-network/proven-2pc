//! COALESCE function tests
//! Based on gluesql/test-suite/src/function/coalesce.rs

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_no_arguments() {
    // TODO: Test SELECT COALESCE() should fail with FunctionRequiresMoreArguments
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_single_null() {
    // TODO: Test SELECT COALESCE(NULL) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_null_and_value() {
    // TODO: Test SELECT COALESCE(NULL, 42) should return 42
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_with_subqueries() {
    // TODO: Test SELECT COALESCE((SELECT NULL), (SELECT 42)) should return 42
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_nested() {
    // TODO: Test nested COALESCE(COALESCE(NULL), COALESCE(NULL, 'Answer to the Ultimate Question of Life'))
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_non_null_first() {
    // TODO: Test SELECT COALESCE('Hitchhiker', NULL) should return 'Hitchhiker'
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_all_nulls() {
    // TODO: Test SELECT COALESCE(NULL, NULL, NULL) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_multiple_integers() {
    // TODO: Test SELECT COALESCE(NULL, 42, 84) should return 42 (first non-null)
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_multiple_floats() {
    // TODO: Test SELECT COALESCE(NULL, 1.23, 4.56) should return 1.23
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_multiple_booleans() {
    // TODO: Test SELECT COALESCE(NULL, TRUE, FALSE) should return TRUE
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_invalid_nested_expression() {
    // TODO: Test SELECT COALESCE(NULL, COALESCE()) should fail with FunctionRequiresMoreArguments
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_with_table_columns() {
    // TODO: Test COALESCE with table column values and different types of default values
}

#[ignore = "not yet implemented"]
#[test]
fn test_coalesce_multiple_table_columns() {
    // TODO: Test COALESCE(text_value, integer_value, float_value, boolean_value) from table
}
