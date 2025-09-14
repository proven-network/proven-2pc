//! SPLICE function tests
//! Based on gluesql/test-suite/src/function/splice.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list() {
    // TODO: Test CREATE TABLE ListTable (id INTEGER, items LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_various_list_types() {
    // TODO: Test INSERT INTO ListTable VALUES (1, '[1, 2, 3]'), (2, '["1", "2", "3"]'), (3, '["1", 2, 3]')
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_remove_elements() {
    // TODO: Test SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3) AS actual
    // Should return [1, 4, 5] (removes 3 elements starting at index 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_remove_and_insert_elements() {
    // TODO: Test SELECT SPLICE(CAST('[1, 2, 3, 4, 5]' AS List), 1, 3, CAST('[100, 99]' AS List)) AS actual
    // Should return [1, 100, 99, 4, 5] (removes 3 elements and inserts 2 new ones)
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_with_negative_index() {
    // TODO: Test SELECT SPLICE(CAST('[1, 2, 3]' AS List), -1, 2, CAST('[100, 99]' AS List)) AS actual
    // Should return [100, 99, 3] (negative index from end)
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_remove_beyond_array_length() {
    // TODO: Test SELECT SPLICE(CAST('[1, 2, 3]' AS List), 1, 100, CAST('[100, 99]' AS List)) AS actual
    // Should return [1, 100, 99] (removes to end of array when count exceeds length)
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_non_list_first_arg_should_error() {
    // TODO: Test SELECT SPLICE(1, 2, 3) AS actual
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_non_list_replacement_should_error() {
    // TODO: Test SELECT SPLICE(CAST('[1, 2, 3]' AS List), 2, 4, 9) AS actual
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_function_signature() {
    // TODO: Test that SPLICE supports both 3-argument (remove only) and 4-argument (remove and insert) forms
}

#[ignore = "not yet implemented"]
#[test]
fn test_splice_index_bounds_handling() {
    // TODO: Test various edge cases for index bounds and negative indices
}
