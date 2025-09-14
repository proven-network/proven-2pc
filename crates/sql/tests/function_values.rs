//! VALUES function tests
//! Based on gluesql/test-suite/src/function/values.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_map() {
    // TODO: Test CREATE TABLE USER (id INTEGER, data MAP)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_various_maps() {
    // TODO: Test INSERT INTO USER VALUES
    // (1, '{"id": 1, "name": "alice", "is_male": false}'),
    // (2, '{"name": "bob"}'),
    // (3, '{}')
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_sorted_descending() {
    // TODO: Test SELECT SORT(VALUES(data), 'DESC') as result FROM USER WHERE id=1
    // Should return [1, false, "alice"] (values sorted by descending order)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_sorted_ascending() {
    // TODO: Test SELECT SORT(VALUES(data), 'ASC') as result FROM USER WHERE id=1
    // Should return ["alice", false, 1] (values sorted by ascending order)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_single_value_map() {
    // TODO: Test SELECT VALUES(data) as result FROM USER WHERE id=2
    // Should return ["bob"]
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_empty_map() {
    // TODO: Test SELECT VALUES(data) as result FROM USER WHERE id=3
    // Should return empty array []
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_non_map_should_error() {
    // TODO: Test SELECT VALUES(id) FROM USER WHERE id=1
    // Should error: MapTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_function_signature() {
    // TODO: Test that VALUES requires exactly 1 MAP argument
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_mixed_data_types() {
    // TODO: Test that VALUES returns mixed data types (strings, numbers, booleans) correctly
}
