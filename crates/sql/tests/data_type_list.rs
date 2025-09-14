//! LIST data type functionality tests
//! Based on gluesql/test-suite/src/data_type/list.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list_column() {
    // TODO: Test CREATE TABLE ListType (id INTEGER, items LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_simple_integer_list() {
    // TODO: Test inserting '[1, 2, 3]' into LIST column
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_mixed_type_list() {
    // TODO: Test inserting '["hello", "world", 30, true, [9,8]]' (strings, numbers, booleans, nested arrays)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_complex_nested_list() {
    // TODO: Test inserting '[{ "foo": 100, "bar": [true, 0, [10.5, false] ] }, 10, 20]'
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_list_values() {
    // TODO: Test SELECT id, items FROM ListType returns proper list values
}

#[ignore = "not yet implemented"]
#[test]
fn test_unwrap_list_elements() {
    // TODO: Test UNWRAP(items, '1') to access second element by index
}

#[ignore = "not yet implemented"]
#[test]
fn test_unwrap_nested_list_and_object_paths() {
    // TODO: Test UNWRAP(items, '0.foo') and UNWRAP(items, '0.bar.2.0') for complex paths
}

#[ignore = "not yet implemented"]
#[test]
fn test_list_index_access_with_brackets() {
    // TODO: Test items[1] AS second for accessing list elements with bracket notation
}

#[ignore = "not yet implemented"]
#[test]
fn test_list_index_without_alias() {
    // TODO: Test SELECT id, items[1] FROM ListType (without AS alias)
}

#[ignore = "not yet implemented"]
#[test]
fn test_nested_list_and_object_bracket_access() {
    // TODO: Test items['3']['0'] for accessing nested elements with string indices
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_literal_to_list() {
    // TODO: Test CAST('[1, 2, 3]' AS LIST) AS list
}

#[ignore = "not yet implemented"]
#[test]
fn test_group_by_list_column() {
    // TODO: Test SELECT id FROM ListType GROUP BY items
}

#[ignore = "not yet implemented"]
#[test]
fn test_invalid_selector_on_non_list() {
    // TODO: Test items['not']['list'] should error: SelectorRequiresMapOrListTypes
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_json_object_into_list_should_error() {
    // TODO: Test INSERT INTO ListType VALUES (1, '{ "a": 10 }') should error: JsonArrayTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_invalid_json_should_error() {
    // TODO: Test INSERT INTO ListType VALUES (1, '{{ ok [1, 2, 3] }') should error: InvalidJsonString
}
