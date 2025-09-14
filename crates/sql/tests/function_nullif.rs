//! NULLIF function tests
//! Based on gluesql/test-suite/src/function/nullif.rs

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_equal_integers() {
    // TODO: Test SELECT NULLIF(0, 0) should return NULL when integers are equal
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_different_integers() {
    // TODO: Test SELECT NULLIF(1, 0) should return first argument when integers are different
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_equal_strings() {
    // TODO: Test SELECT NULLIF('hello', 'hello') should return NULL when strings are equal
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_different_strings() {
    // TODO: Test SELECT NULLIF('hello', 'helle') should return first argument when strings are different
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_equal_dates() {
    // TODO: Test SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-01', '%Y-%m-%d')) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_different_dates() {
    // TODO: Test SELECT NULLIF(TO_DATE('2025-01-01', '%Y-%m-%d'), TO_DATE('2025-01-02', '%Y-%m-%d')) should return first date
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_no_arguments() {
    // TODO: Test SELECT NULLIF() should fail with FunctionArgsLengthNotMatching (expected: 2, found: 0)
}

#[ignore = "not yet implemented"]
#[test]
fn test_nullif_one_argument() {
    // TODO: Test SELECT NULLIF(1) should fail with FunctionArgsLengthNotMatching (expected: 2, found: 1)
}
