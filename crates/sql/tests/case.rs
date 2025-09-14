//! CASE expression tests
//! Based on gluesql/test-suite/src/case.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_case() {
    // TODO: Test CREATE TABLE Item (id INTEGER, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_data_for_case() {
    // TODO: Test INSERT INTO Item (id, name) VALUES (1, 'Harry'), (2, 'Ron'), (3, 'Hermione') - 3 inserts
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_value_expression_and_else() {
    // TODO: Test CASE id WHEN 1 THEN name WHEN 2 THEN name WHEN 4 THEN name ELSE 'Malfoy' END - returns Harry, Ron, Malfoy
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_value_expression_without_else() {
    // TODO: Test CASE id WHEN 1 THEN name WHEN 2 THEN name WHEN 4 THEN name END - returns Harry, Ron, NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_boolean_expressions_and_else() {
    // TODO: Test CASE WHEN name = 'Harry' THEN id WHEN name = 'Ron' THEN id WHEN name = 'Hermione' THEN id ELSE 404 END - returns 1, 2, 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_boolean_expressions_without_else() {
    // TODO: Test CASE WHEN name = 'Harry' THEN id WHEN name = 'Ron' THEN id WHEN name = 'Hermion' THEN id END - returns 1, 2, NULL (typo in Hermion)
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_complex_expressions() {
    // TODO: Test CASE WHEN (name = 'Harry') OR (name = 'Ron') THEN (id + 1) WHEN name = ('Hermi' || 'one') THEN (id + 2) ELSE 404 END - returns 2, 3, 5
}

#[ignore = "not yet implemented"]
#[test]
fn test_case_with_collate_unsupported() {
    // TODO: Test CASE 1 COLLATE Item WHEN ... - should error with UnsupportedExpr
}

#[ignore = "not yet implemented"]
#[test]
fn test_collate_expression_unsupported() {
    // TODO: Test SELECT 1 COLLATE Item FROM Item - should error with UnsupportedExpr
}
