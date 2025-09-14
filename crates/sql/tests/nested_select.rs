//! Nested SELECT and subquery tests
//! Based on gluesql/test-suite/src/nested_select.rs

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_literals() {
    // TODO: Test SELECT * FROM Request WHERE quantity IN (5, 1) - 6 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_not_in_with_literals() {
    // TODO: Test SELECT * FROM Request WHERE quantity NOT IN (5, 1) - 9 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_simple_subquery() {
    // TODO: Test SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE id = 3) - 4 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_subquery_from_request() {
    // TODO: Test SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request) - 4 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_correlated_subquery() {
    // TODO: Test SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id = Player.id) - 4 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_nested_correlated_subquery() {
    // TODO: Test SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE user_id IN (Player.id)) - 4 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_quantity_filter_subquery() {
    // TODO: Test SELECT * FROM Player WHERE id IN (SELECT user_id FROM Request WHERE quantity IN (6, 7, 8, 9)) - 2 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_in_with_name_filter_subquery() {
    // TODO: Test SELECT * FROM Request WHERE user_id IN (SELECT id FROM Player WHERE name IN ('Taehoon', 'Hwan')) - 9 results
}

#[ignore = "not yet implemented"]
#[test]
fn test_where_equals_nonexistent_subquery() {
    // TODO: Test SELECT * FROM Player WHERE id = (SELECT id FROM Player WHERE id = 9) - empty result
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_subquery_with_series_nonexistent() {
    // TODO: Test SELECT (SELECT N FROM SERIES(3) WHERE N = 4) N - returns NULL
}
