//! Foreign key constraint tests
//! Based on gluesql/test-suite/src/foreign_key.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_referencing_table_without_primary_key() {
    // TODO: Test creating table with foreign key should fail if referenced table does not have primary key
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_referencing_table_with_only_unique_constraint() {
    // TODO: Test creating table with foreign key should fail if referenced table has only Unique constraint
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_foreign_key_different_data_types() {
    // TODO: Test creating table with foreign key on different data type should fail
}

#[ignore = "not yet implemented"]
#[test]
fn test_unsupported_foreign_key_option_cascade() {
    // TODO: Test unsupported foreign key option: CASCADE
}

#[ignore = "not yet implemented"]
#[test]
fn test_unsupported_foreign_key_option_set_default() {
    // TODO: Test unsupported foreign key option: SET DEFAULT
}

#[ignore = "not yet implemented"]
#[test]
fn test_unsupported_foreign_key_option_set_null() {
    // TODO: Test unsupported foreign key option: SET NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_referencing_column_not_found() {
    // TODO: Test referencing column not found error
}

#[ignore = "not yet implemented"]
#[test]
fn test_referenced_column_not_found() {
    // TODO: Test referenced column not found error
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_foreign_key_success() {
    // TODO: Test creating table with foreign key should succeed if referenced table has primary key. NO ACTION(=RESTRICT) is default
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_without_referenced_value() {
    // TODO: Test insert should fail if there is no referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_null_referenced_value() {
    // TODO: Test NULL should be allowed even if there is no referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_with_valid_referenced_value() {
    // TODO: Test insert should succeed with valid referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_without_referenced_value() {
    // TODO: Test update should fail if there is no referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_to_null_referenced_value() {
    // TODO: Test update to NULL should be allowed even if there is no referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_with_valid_referenced_value() {
    // TODO: Test update should succeed with valid referenced value
}

#[ignore = "not yet implemented"]
#[test]
fn test_delete_referenced_row_with_referencing_value() {
    // TODO: Test deleting referenced row should fail if referencing value exists (NO ACTION gets error)
}

#[ignore = "not yet implemented"]
#[test]
fn test_delete_referencing_table() {
    // TODO: Test deleting referencing table does not care about referenced table
}

#[ignore = "not yet implemented"]
#[test]
fn test_table_with_two_foreign_keys() {
    // TODO: Test table with two foreign keys
}

#[ignore = "not yet implemented"]
#[test]
fn test_update_with_multiple_foreign_keys() {
    // TODO: Test cannot update referenced_id if there is no referenced value in multiple FK scenario
}

#[ignore = "not yet implemented"]
#[test]
fn test_drop_referenced_table_with_referencing_tables() {
    // TODO: Test cannot drop referenced table if referencing table exists
}

#[ignore = "not yet implemented"]
#[test]
fn test_drop_table_with_cascade() {
    // TODO: Test dropping table with CASCADE should drop both table and constraint
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_self_referencing_table() {
    // TODO: Test should create self referencing table
}

#[ignore = "not yet implemented"]
#[test]
fn test_drop_self_referencing_table() {
    // TODO: Test dropping self referencing table should succeed
}
