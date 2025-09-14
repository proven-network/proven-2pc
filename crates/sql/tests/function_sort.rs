//! SORT function tests
//! Based on gluesql/test-suite/src/function/sort.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list() {
    // TODO: Test CREATE TABLE Test1 (list LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_unordered_list() {
    // TODO: Test INSERT INTO Test1 (list) VALUES ('[2, 1, 4, 3]')
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_list_default_order() {
    // TODO: Test SELECT SORT(list) AS list FROM Test1
    // Should return [1, 2, 3, 4] (default ascending order)
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_list_ascending() {
    // TODO: Test SELECT SORT(list, 'ASC') AS list FROM Test1
    // Should return [1, 2, 3, 4]
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_list_descending() {
    // TODO: Test SELECT SORT(list, 'DESC') AS list FROM Test1
    // Should return [4, 3, 2, 1]
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_invalid_order_should_error() {
    // TODO: Test SELECT SORT(list, 'WRONG') AS list FROM Test1
    // Should error: InvalidSortOrder
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_non_string_order_should_error() {
    // TODO: Test SELECT SORT(list, 1) AS list FROM Test1
    // Should error: InvalidSortOrder
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_mixed_types() {
    // TODO: Test CREATE TABLE Test2 (id INTEGER, list LIST)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_mixed_type_list() {
    // TODO: Test INSERT INTO Test2 (id, list) VALUES (1, '[2, "1", ["a", "b"], 3]')
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_non_list_should_error() {
    // TODO: Test SELECT SORT(id) AS list FROM Test2
    // Should error: ListTypeRequired
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_incomparable_types_should_error() {
    // TODO: Test SELECT SORT(list) AS list FROM Test2
    // Should error: InvalidSortType (cannot compare mixed types)
}

#[ignore = "not yet implemented"]
#[test]
fn test_sort_supports_both_signatures() {
    // TODO: Test that SORT supports both SORT(list) and SORT(list, order) signatures
}
