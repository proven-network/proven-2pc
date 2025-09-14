//! SKIP function tests
//! Based on gluesql/test-suite/src/function/skip.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list() {
    // TODO: Test CREATE TABLE Test (id INTEGER, list LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_list_data() {
    // TODO: Test INSERT INTO Test (id, list) VALUES (1,'[1,2,3,4,5]')
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_elements_normal_usage() {
    // TODO: Test SELECT SKIP(list, 2) as col1 FROM Test
    // Should return [3, 4, 5] (skips first 2 elements)
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_beyond_list_length() {
    // TODO: Test SELECT SKIP(list, 6) as col1 FROM Test
    // Should return empty array [] (skip count exceeds list length)
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_with_null_list() {
    // TODO: Test SELECT SKIP(NULL, 2) as col1 FROM Test
    // Should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_with_null_count() {
    // TODO: Test SELECT SKIP(list, NULL) as col1 FROM Test
    // Should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_non_integer_parameter_should_error() {
    // TODO: Test SELECT SKIP(list, 'd') as col1 FROM Test
    // Should error: FunctionRequiresIntegerValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_non_list_should_error() {
    // TODO: Test SELECT SKIP(id, 2) as col1 FROM Test
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_negative_size_should_error() {
    // TODO: Test SELECT SKIP(list, -2) as col1 FROM Test
    // Should error: FunctionRequiresUSizeValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_function_signature() {
    // TODO: Test that SKIP requires exactly 2 arguments: list and count
}

#[ignore = "not yet implemented"]
#[test]
fn test_skip_edge_cases() {
    // TODO: Test SKIP with count=0 (should return original list)
}
