//! SUBSTR function tests
//! Based on gluesql/test-suite/src/function/substr.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_substr_default() {
    // TODO: Test CREATE TABLE Item (name TEXT DEFAULT SUBSTR('abc', 0, 2))
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_test_data() {
    // TODO: Test INSERT INTO Item VALUES ('Blop mc blee'), ('B'), ('Steven the &long named$ folken!')
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_additional_tables() {
    // TODO: Test CREATE TABLE SingleItem (food TEXT) and INSERT INTO SingleItem VALUES (SUBSTR('LobSter',1))
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_null_tables() {
    // TODO: Test CREATE TABLE NullName (name TEXT NULL) and CREATE TABLE NullNumber (number INTEGER NULL)
}

#[ignore = "not yet implemented"]
#[test]
fn test_nested_substr() {
    // TODO: Test SELECT SUBSTR(SUBSTR(name, 1), 1) AS test FROM Item
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_in_where_clause() {
    // TODO: Test SELECT * FROM Item WHERE name = SUBSTR('ABC', 2, 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_comparison_operations() {
    // TODO: Test SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = 'B'
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_functions() {
    // TODO: Test SELECT * FROM Item WHERE SUBSTR(name, 1, 1) = UPPER('b')
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_comparison_with_substr() {
    // TODO: Test SELECT * FROM Item WHERE SUBSTR(name, 1, 4) = SUBSTR('Blop', 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_greater_than_operations() {
    // TODO: Test SELECT * FROM Item WHERE SUBSTR(name, 1, 4) > SUBSTR('Blop', 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_two_parameter_form() {
    // TODO: Test SELECT SUBSTR(name, 2) AS test FROM Item (without length parameter)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_out_of_bounds() {
    // TODO: Test SELECT SUBSTR(name, 999) AS test FROM Item should return empty strings
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_negative_indices() {
    // TODO: Test SELECT SUBSTR('ABC', -3, 0), SUBSTR('ABC', -1, 3), SUBSTR('ABC', -1, 4)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_zero_and_large_indices() {
    // TODO: Test SELECT SUBSTR('ABC', 0, 3), SUBSTR('ABC', 1, 999), SUBSTR('ABC', -1000, 1003)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_nested_function_calls() {
    // TODO: Test SELECT SUBSTR(SUBSTR('ABC', 2, 3), 1, 1) AS test FROM SingleItem
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_null_parameters() {
    // TODO: Test SELECT SUBSTR('ABC', -1, NULL), SUBSTR(name, 3) from NullName, SUBSTR('Words', number) from NullNumber
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_error_cases() {
    // TODO: Test various error cases like SUBSTR in boolean context, SUBSTR(1, 1), SUBSTR('Words', 1.1), SUBSTR('Words', 1, -4)
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_arithmetic_operations() {
    // TODO: Test error cases with arithmetic operations like SUBSTR('123', 2, 3) - '3'
}

#[ignore = "not yet implemented"]
#[test]
fn test_substr_with_unary_operations() {
    // TODO: Test error cases with unary operations like +SUBSTR, -SUBSTR, SUBSTR!, ~SUBSTR
}
