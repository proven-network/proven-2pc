//! SLICE function tests
//! Based on gluesql/test-suite/src/function/slice.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list() {
    // TODO: Test CREATE TABLE Test (list LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_list_data() {
    // TODO: Test INSERT INTO Test VALUES ('[1,2,3,4]')
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_from_start_with_length() {
    // TODO: Test SELECT SLICE(list, 0, 2) AS value FROM Test
    // Should return [1, 2]
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_full_array() {
    // TODO: Test SELECT SLICE(list, 0, 4) AS value FROM Test
    // Should return [1, 2, 3, 4]
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_beyond_array_size() {
    // TODO: Test SELECT SLICE(list, 2, 5) AS value FROM Test
    // Should return [3, 4] (clips to array bounds)
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_start_beyond_array() {
    // TODO: Test SELECT SLICE(list, 100, 5) AS value FROM Test
    // Should return empty array []
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_non_list_should_error() {
    // TODO: Test SELECT SLICE(1, 2, 2) AS value FROM Test
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_non_integer_start_should_error() {
    // TODO: Test SELECT SLICE(list, 'b', 5) AS value FROM Test
    // Should error: FunctionRequiresIntegerValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_negative_index() {
    // TODO: Test SELECT SLICE(list, -1, 1) AS value FROM Test
    // Should return [4] (negative index from end)
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_negative_index_multiple() {
    // TODO: Test SELECT SLICE(list, -2, 4) AS value FROM Test
    // Should return [3, 4]
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_large_start_index() {
    // TODO: Test SELECT SLICE(list, 9999, 4) AS value FROM Test
    // Should return empty array []
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_large_length() {
    // TODO: Test SELECT SLICE(list, 0, 1234) AS value FROM Test
    // Should return [1, 2, 3, 4] (clips to array size)
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_large_negative_index() {
    // TODO: Test SELECT SLICE(list, -234, 4) AS value FROM Test
    // Should return [1, 2, 3, 4] (negative index too large converts to 0)
}

#[ignore = "not yet implemented"]
#[test]
fn test_slice_non_integer_length_should_error() {
    // TODO: Test SELECT SLICE(list, 2, 'a') AS value FROM Test
    // Should error: FunctionRequiresIntegerValue
}
