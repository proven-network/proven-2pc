//! ROUND function tests
//! Based on gluesql/test-suite/src/function/round.rs

#[ignore = "not yet implemented"]
#[test]
fn test_round_basic_values() {
    // TODO: Test SELECT ROUND(0.3), ROUND(-0.8), ROUND(10), ROUND(6.87421) - should round to nearest integer
}

#[ignore = "not yet implemented"]
#[test]
fn test_round_with_string_input() {
    // TODO: Test SELECT ROUND('string') should fail with FunctionRequiresFloatValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_round_with_null_input() {
    // TODO: Test SELECT ROUND(NULL) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_round_with_boolean_inputs() {
    // TODO: Test SELECT ROUND(TRUE) and SELECT ROUND(FALSE) should fail with FunctionRequiresFloatValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_round_with_wrong_number_of_arguments() {
    // TODO: Test SELECT ROUND('string', 'string2') should fail with FunctionArgsLengthNotMatching (expected: 1, found: 2)
}
