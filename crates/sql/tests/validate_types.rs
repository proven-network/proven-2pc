//! Data type validation tests
//! Based on gluesql/test-suite/src/validate/types.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_boolean_and_integer_tables() {
    // TODO: Test CREATE TABLE TableB (id BOOLEAN) and TableC (uid INTEGER NOT NULL, null_val INTEGER NULL)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_valid_data() {
    // TODO: Test INSERT INTO TableB VALUES (FALSE) and INSERT INTO TableC VALUES (1, NULL)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_incompatible_data_type_from_select() {
    // TODO: Test INSERT INTO TableB SELECT uid FROM TableC should fail with IncompatibleDataType (INTEGER to BOOLEAN)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_incompatible_literal_for_data_type() {
    // TODO: Test INSERT INTO TableC (uid) VALUES ('A') should fail with IncompatibleLiteralForDataType (TEXT to INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_null_into_not_null_field() {
    // TODO: Test INSERT INTO TableC VALUES (NULL, 30) should fail with NullValueOnNotNullField
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_null_from_select_into_not_null_field() {
    // TODO: Test INSERT INTO TableC SELECT null_val FROM TableC should fail with NullValueOnNotNullField
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_with_incompatible_literal_type() {
    // TODO: Test UPDATE TableC SET uid = TRUE should fail with IncompatibleLiteralForDataType (BOOLEAN to INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_with_incompatible_data_type_from_subquery() {
    // TODO: Test UPDATE TableC SET uid = (SELECT id FROM TableB LIMIT 1) WHERE uid = 1 should fail with IncompatibleDataType (BOOLEAN to INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_to_null_on_not_null_field() {
    // TODO: Test UPDATE TableC SET uid = NULL should fail with NullValueOnNotNullField
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_to_null_from_subquery_on_not_null_field() {
    // TODO: Test UPDATE TableC SET uid = (SELECT null_val FROM TableC) should fail with NullValueOnNotNullField
}
