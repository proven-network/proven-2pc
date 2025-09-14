//! UPPER and LOWER function tests
//! Based on gluesql/test-suite/src/function/upper_lower.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_upper_lower_defaults() {
    // TODO: Test CREATE TABLE Item (name TEXT DEFAULT UPPER('abc'), opt_name TEXT NULL DEFAULT LOWER('ABC'))
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_mixed_case_values() {
    // TODO: Test INSERT INTO Item VALUES ('abcd', 'efgi'), ('Abcd', NULL), ('ABCD', 'EfGi')
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_function_in_where_clause() {
    // TODO: Test SELECT name FROM Item WHERE LOWER(name) = 'abcd' - should find all case variations
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_and_upper_function_basic() {
    // TODO: Test SELECT LOWER(name), UPPER(name) FROM Item - should convert all names to lowercase/uppercase
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_and_upper_with_literals() {
    // TODO: Test SELECT LOWER('Abcd'), UPPER('abCd') FROM Item LIMIT 1 - should convert literal strings
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_and_upper_with_null_values() {
    // TODO: Test SELECT LOWER(opt_name), UPPER(opt_name) FROM Item - should handle NULL values properly
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_function_no_arguments() {
    // TODO: Test SELECT LOWER() FROM Item should fail with FunctionArgsLengthNotMatching
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_function_wrong_type() {
    // TODO: Test SELECT LOWER(1) FROM Item should fail with FunctionRequiresStringValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_lower_function_named_arguments() {
    // TODO: Test SELECT LOWER(a => 2) FROM Item should fail with NamedFunctionArgNotSupported
}
