//! PREPEND function tests
//! Based on gluesql/test-suite/src/function/prepend.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list_and_elements() {
    // TODO: Test CREATE TABLE Prepend (id INTEGER, items LIST, element INTEGER, element2 TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_list_data() {
    // TODO: Test INSERT INTO Prepend VALUES (1, '[1, 2, 3]', 0, 'Foo')
}

#[ignore = "not yet implemented"]
#[test]
fn test_prepend_integer_to_list() {
    // TODO: Test SELECT PREPEND(items, element) as myprepend FROM Prepend
    // Should return [0, 1, 2, 3]
}

#[ignore = "not yet implemented"]
#[test]
fn test_prepend_text_to_list() {
    // TODO: Test SELECT PREPEND(items, element2) as myprepend FROM Prepend
    // Should return ["Foo", 1, 2, 3]
}

#[ignore = "not yet implemented"]
#[test]
fn test_prepend_non_list_should_error() {
    // TODO: Test SELECT PREPEND(element, element2) as myprepend FROM Prepend
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_prepend_operation() {
    // TODO: Test CREATE TABLE Foo (elements LIST) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_with_prepend_function() {
    // TODO: Test INSERT INTO Foo VALUES (PREPEND(CAST('[1, 2, 3]' AS LIST), 0))
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_prepended_elements() {
    // TODO: Test SELECT elements as myprepend FROM Foo should return [0, 1, 2, 3]
}

#[ignore = "not yet implemented"]
#[test]
fn test_prepend_mixed_types() {
    // TODO: Test that PREPEND works with mixed data types in lists
}

#[ignore = "not yet implemented"]
#[test]
fn test_prepend_with_cast() {
    // TODO: Test PREPEND with CAST('[1, 2, 3]' AS LIST)
}
