//! KEYS function tests
//! Based on gluesql/test-suite/src/function/keys.rs

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
fn test_keys_returns_all_keys_sorted() {
    // TODO: Test SELECT SORT(KEYS(data), 'ASC') as result FROM USER WHERE id=1
    // Should return ["id", "is_male", "name"] (sorted alphabetically)
}

#[ignore = "not yet implemented"]
#[test]
fn test_keys_single_key_map() {
    // TODO: Test SELECT KEYS(data) as result FROM USER WHERE id=2
    // Should return ["name"]
}

#[ignore = "not yet implemented"]
#[test]
fn test_keys_empty_map() {
    // TODO: Test SELECT KEYS(data) as result FROM USER WHERE id=3
    // Should return empty array []
}

#[ignore = "not yet implemented"]
#[test]
fn test_keys_non_map_should_error() {
    // TODO: Test SELECT KEYS(id) FROM USER WHERE id=1
    // Should error: MapTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_keys_function_signature() {
    // TODO: Test that KEYS requires exactly 1 MAP argument
}

#[ignore = "not yet implemented"]
#[test]
fn test_keys_with_null_map() {
    // TODO: Test KEYS with NULL map value
}
