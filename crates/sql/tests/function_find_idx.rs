//! FIND_IDX function tests
//! Based on gluesql/test-suite/src/function/find_idx.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_text_search() {
    // TODO: Test CREATE TABLE Meal (menu Text null) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_text_data() {
    // TODO: Test INSERT INTO Meal VALUES ('pork'), ('burger') should return Payload::Insert(1) each
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_substring_search() {
    // TODO: Test SELECT FIND_IDX(menu, 'rg') AS test FROM Meal
    // Should return 0 for 'pork' (not found), 3 for 'burger' (index of 'rg')
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_with_start_offset() {
    // TODO: Test SELECT FIND_IDX(menu, 'r', 4) AS test FROM Meal
    // Should return 0 for 'pork' (not found), 6 for 'burger' (finds 'r' at index 6 starting from index 4)
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_empty_substring() {
    // TODO: Test SELECT FIND_IDX('cheese', '') AS test
    // Should return 0 (empty string found at beginning)
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_single_character() {
    // TODO: Test SELECT FIND_IDX('cheese', 's') AS test
    // Should return 5 (index of 's' in 'cheese')
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_with_offset() {
    // TODO: Test SELECT FIND_IDX('cheese burger', 'e', 5) AS test
    // Should return 6 (finds 'e' at index 6 starting search from index 5)
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_with_null_substring() {
    // TODO: Test SELECT FIND_IDX('cheese', NULL) AS test
    // Should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_non_string_substring_should_error() {
    // TODO: Test SELECT FIND_IDX('cheese', 1) AS test
    // Should error: FunctionRequiresStringValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_non_integer_offset_should_error() {
    // TODO: Test SELECT FIND_IDX('cheese', 's', '5') AS test
    // Should error: FunctionRequiresIntegerValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_negative_offset_should_error() {
    // TODO: Test SELECT FIND_IDX('cheese', 's', -1) AS test
    // Should error: NonPositiveIntegerOffsetInFindIdx
}

#[ignore = "not yet implemented"]
#[test]
fn test_find_idx_function_signatures() {
    // TODO: Test that FIND_IDX supports both 2-argument and 3-argument forms
}
