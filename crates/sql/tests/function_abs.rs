//! ABS function tests
//! Based on gluesql/test-suite/src/function/abs.rs

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_integers() {
    // TODO: Test SELECT ABS(1), ABS(-1), ABS(+1) should all return 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_floats() {
    // TODO: Test SELECT ABS(1.5), ABS(-1.5), ABS(+1.5) should all return 1.5
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_zeros() {
    // TODO: Test SELECT ABS(0), ABS(-0), ABS(+0) should all return 0
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_abs() {
    // TODO: Test CREATE TABLE SingleItem (id integer, int8 int8, dec decimal)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_negative_values() {
    // TODO: Test INSERT INTO SingleItem VALUES (0, -1, -2)
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_table_columns() {
    // TODO: Test SELECT ABS(id), ABS(int8), ABS(dec) FROM SingleItem - should return absolute values from table columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_string_input() {
    // TODO: Test SELECT ABS('string') should fail with FunctionRequiresFloatValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_null_input() {
    // TODO: Test SELECT ABS(NULL) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_boolean_inputs() {
    // TODO: Test SELECT ABS(TRUE) and SELECT ABS(FALSE) should fail with FunctionRequiresFloatValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_abs_with_wrong_number_of_arguments() {
    // TODO: Test SELECT ABS('string', 'string2') should fail with FunctionArgsLengthNotMatching (expected: 1, found: 2)
}
