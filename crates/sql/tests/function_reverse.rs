//! REVERSE function tests
//! Based on gluesql/test-suite/src/function/reverse.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_reverse_default() {
    // TODO: Test CREATE TABLE Item (name TEXT DEFAULT REVERSE('world')) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_text_data() {
    // TODO: Test INSERT INTO Item VALUES ('Let''s meet') should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_string() {
    // TODO: Test SELECT REVERSE(name) AS test FROM Item
    // Should return "teem s'teL" (reversed "Let's meet")
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_non_string_should_error() {
    // TODO: Test SELECT REVERSE(1) AS test FROM Item
    // Should error: FunctionRequiresStringValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_null_test() {
    // TODO: Test CREATE TABLE NullTest (name TEXT null) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_null_value() {
    // TODO: Test INSERT INTO NullTest VALUES (null) should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_null_returns_null() {
    // TODO: Test SELECT REVERSE(name) AS test FROM NullTest should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_empty_string() {
    // TODO: Test REVERSE('') returns empty string
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_single_character() {
    // TODO: Test REVERSE with single character string
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_special_characters() {
    // TODO: Test REVERSE with strings containing special characters
}

#[ignore = "not yet implemented"]
#[test]
fn test_reverse_function_signature() {
    // TODO: Test that REVERSE requires exactly 1 string argument
}
