//! Migration and error handling tests
//! Based on gluesql/test-suite/src/migrate.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_test_table() {
    // TODO: Test CREATE TABLE Test (id INT, num INT, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_with_expressions() {
    // TODO: Test INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello'), (-(-1), 9, 'World'), (+3, 2 * 2, 'Great')
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_decimal_into_int_should_error() {
    // TODO: Test INSERT INTO Test (id, num, name) VALUES (1.1, 1, 'good') should error: FailedToParseNumber
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_compound_identifier_should_error() {
    // TODO: Test INSERT INTO Test (id, num, name) VALUES (1, 1, a.b) should error: ContextRequiredForIdentEvaluation
}

#[ignore = "not yet implemented"]
#[test]
fn test_unsupported_compound_identifier_should_error() {
    // TODO: Test SELECT * FROM Test WHERE Here.User.id = 1 should error: UnsupportedExpr
}

#[ignore = "not yet implemented"]
#[test]
fn test_natural_join_should_error() {
    // TODO: Test SELECT * FROM Test NATURAL JOIN Test should error: UnsupportedJoinConstraint
}

#[ignore = "not yet implemented"]
#[test]
fn test_unsupported_binary_operator_should_error() {
    // TODO: Test SELECT 1 ^ 2 FROM Test should error: UnsupportedBinaryOperator
}

#[ignore = "not yet implemented"]
#[test]
fn test_union_query_should_error() {
    // TODO: Test SELECT * FROM Test UNION SELECT * FROM Test should error: UnsupportedQuerySetExpr
}

#[ignore = "not yet implemented"]
#[test]
fn test_unknown_identifier_should_error() {
    // TODO: Test SELECT * FROM Test WHERE noname = 1 should error: IdentifierNotFound
}

#[ignore = "not yet implemented"]
#[test]
fn test_table_not_found_should_error() {
    // TODO: Test SELECT * FROM Nothing should error: TableNotFound
}

#[ignore = "not yet implemented"]
#[test]
fn test_truncate_statement_should_error() {
    // TODO: Test TRUNCATE TABLE ProjectUser should error: UnsupportedStatement
}

#[ignore = "not yet implemented"]
#[test]
fn test_distinct_on_should_error() {
    // TODO: Test SELECT DISTINCT ON (id) id, num, name FROM Test should error: SelectDistinctOnNotSupported
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_all_data() {
    // TODO: Test SELECT id, num, name FROM Test returns all inserted data
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_with_where_condition() {
    // TODO: Test SELECT id, num, name FROM Test WHERE id = 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_all_rows() {
    // TODO: Test UPDATE Test SET id = 2 (updates all rows)
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_after_update() {
    // TODO: Test SELECT id, num, name FROM Test after update shows all ids as 2
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_single_column() {
    // TODO: Test SELECT id FROM Test returns three rows with value 2
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_two_columns() {
    // TODO: Test SELECT id, num FROM Test returns id and num columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_limit_offset_query() {
    // TODO: Test SELECT id, num FROM Test LIMIT 1 OFFSET 1 returns second row
}
