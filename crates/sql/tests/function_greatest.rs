//! GREATEST function tests
//! Based on gluesql/test-suite/src/function/greatest.rs

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_integers() {
    // TODO: Test SELECT GREATEST(1,6,9,7,0,10) AS goat
    // Should return 10
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_floats() {
    // TODO: Test SELECT GREATEST(1.2,6.8,9.6,7.4,0.1,10.5) AS goat
    // Should return 10.5
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_strings() {
    // TODO: Test SELECT GREATEST('bibibik', 'babamba', 'melona') AS goat
    // Should return "melona" (lexicographical order)
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_dates() {
    // TODO: Test SELECT GREATEST(DATE '2023-07-17', DATE '2022-07-17', DATE '2023-06-17', DATE '2024-07-17', DATE '2024-07-18') AS goat
    // Should return DATE '2024-07-18'
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_no_arguments_should_error() {
    // TODO: Test SELECT GREATEST() AS goat
    // Should error: FunctionArgsLengthNotMatchingMin (expected_minimum: 2, found: 0)
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_mixed_types_should_error() {
    // TODO: Test SELECT GREATEST(1, 2, 'bibibik') AS goat
    // Should error: NonComparableArgumentError
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_with_null_should_error() {
    // TODO: Test SELECT GREATEST(NULL, 'bibibik', 'babamba', 'melona') AS goat
    // Should error: NonComparableArgumentError
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_all_nulls_should_error() {
    // TODO: Test SELECT GREATEST(NULL, NULL, NULL) AS goat
    // Should error: NonComparableArgumentError
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_booleans() {
    // TODO: Test SELECT GREATEST(true, false) AS goat
    // Should return true
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_function_signature() {
    // TODO: Test that GREATEST requires at least 2 comparable arguments
}

#[ignore = "not yet implemented"]
#[test]
fn test_greatest_single_argument_should_error() {
    // TODO: Test that GREATEST with single argument errors appropriately
}
