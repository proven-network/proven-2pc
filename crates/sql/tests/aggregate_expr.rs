//! Aggregate expressions tests
//! Based on gluesql/test-suite/src/aggregate/expr.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_aggregate_expr() {
    // TODO: Test CREATE TABLE Item (id INTEGER, quantity INTEGER, age INTEGER NULL, total INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_data_for_aggregate_expr() {
    // TODO: Test INSERT INTO Item (id, quantity, age, total) VALUES (1, 10, 11, 1), (2, 0, 90, 2), (3, 9, NULL, 3), (4, 3, 3, 1), (5, 25, NULL, 1) - 5 inserts
}

#[ignore = "not yet implemented"]
#[test]
fn test_between_with_aggregates() {
    // TODO: Test SELECT SUM(quantity) BETWEEN MIN(quantity) AND MAX(quantity) AS test FROM Item - should return false (47 not between 0 and 25)
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_comparing_aggregates() {
    // TODO: Test SELECT CASE SUM(quantity) WHEN MIN(quantity) THEN MAX(id) ELSE COUNT(id) END AS test FROM Item - should return 5 (COUNT(id) because SUM != MIN)
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_when_with_aggregate_condition() {
    // TODO: Test SELECT CASE WHEN SUM(quantity) > 30 THEN MAX(id) ELSE MIN(id) END AS test FROM Item - should return 5 (MAX(id) because SUM > 30)
}
