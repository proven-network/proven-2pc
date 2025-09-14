//! VALUES clause tests
//! Based on gluesql/test-suite/src/values.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_values() {
    // TODO: Test CREATE TABLE Items (id INTEGER NOT NULL, name TEXT, status TEXT DEFAULT 'ACTIVE' NOT NULL)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_single_column() {
    // TODO: Test VALUES (1), (2), (3) - returns 3 rows with column1
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_multiple_columns() {
    // TODO: Test VALUES (1, 'a'), (2, 'b') - returns 2 rows with column1, column2
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_with_order_by() {
    // TODO: Test VALUES (1, 'a'), (2, 'b') ORDER BY column1 DESC - returns rows in descending order
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_with_limit() {
    // TODO: Test VALUES (1), (2) LIMIT 1 - returns only first row
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_with_offset() {
    // TODO: Test VALUES (1), (2) OFFSET 1 - returns only second row
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_with_null() {
    // TODO: Test VALUES (1, NULL), (2, NULL) - returns rows with NULL values
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_different_number_of_values_error() {
    // TODO: Test VALUES (1), (2, 'b') - should error with NumberOfValuesDifferent
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_different_number_of_values_reverse_error() {
    // TODO: Test VALUES (1, 'a'), (2) - should error with NumberOfValuesDifferent
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_incompatible_data_types_text_number() {
    // TODO: Test VALUES (1, 'a'), (2, 3) - should error with IncompatibleLiteralForDataType (TEXT vs number)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_incompatible_data_types_int_text() {
    // TODO: Test VALUES (1, 'a'), ('b', 'c') - should error with IncompatibleLiteralForDataType (INT vs text)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_incompatible_data_types_with_null() {
    // TODO: Test VALUES (1, NULL), (2, 'a'), (3, 4) - should error with IncompatibleLiteralForDataType
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_as_values() {
    // TODO: Test CREATE TABLE TableFromValues AS VALUES (1, 'a', True, Null, Null), (2, 'b', False, 3, Null)
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_from_values_table() {
    // TODO: Test SELECT * FROM TableFromValues - verify data types and values
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_columns_from_values_table() {
    // TODO: Test SHOW COLUMNS FROM TableFromValues - verify column types (Int, Text, Boolean, Int, Text)
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_as_subquery() {
    // TODO: Test SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS Derived
}

#[ignore = "not yet implemented"]
#[test]
fn test_values_subquery_with_column_aliases() {
    // TODO: Test SELECT column1 AS id, column2 AS name FROM (VALUES (1, 'a'), (2, 'b')) AS Derived
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_with_values() {
    // TODO: Test INSERT INTO Items (id) VALUES (1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_wrong_column_name_error() {
    // TODO: Test INSERT INTO Items (id2) VALUES (1) - should error with WrongColumnName
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_missing_required_column_error() {
    // TODO: Test INSERT INTO Items (name) VALUES ('glue') - should error with LackOfRequiredColumn
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_column_values_mismatch_error() {
    // TODO: Test INSERT INTO Items (id) VALUES (3, 'sql') - should error with ColumnAndValuesNotMatched
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_too_many_values_error() {
    // TODO: Test INSERT INTO Items VALUES (100, 'a', 'b', 1) - should error with TooManyValues
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_table_not_found_error() {
    // TODO: Test INSERT INTO Nothing VALUES (1) - should error with TableNotFound
}
