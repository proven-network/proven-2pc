//! ENTRIES function tests
//! Based on gluesql/test-suite/src/function/entries.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_map_column() {
    // TODO: Test CREATE TABLE Item (map MAP) should return Payload::Create
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_map_data() {
    // TODO: Test INSERT INTO Item VALUES ('{"name":"GlueSQL"}') should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_returns_key_value_pairs() {
    // TODO: Test SELECT ENTRIES(map) AS test FROM Item
    // Should return [[["name", "GlueSQL"]] (list of key-value pair lists)
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_requires_map_value() {
    // TODO: Test SELECT ENTRIES(1) FROM Item
    // Should error: FunctionRequiresMapValue
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_with_multiple_keys() {
    // TODO: Test ENTRIES with map containing multiple key-value pairs
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_with_empty_map() {
    // TODO: Test ENTRIES with empty map returns empty list
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_preserves_value_types() {
    // TODO: Test that ENTRIES preserves the original data types of values
}

#[ignore = "not yet implemented"]
#[test]
fn test_entries_nested_values() {
    // TODO: Test ENTRIES with map containing nested objects or arrays
}
