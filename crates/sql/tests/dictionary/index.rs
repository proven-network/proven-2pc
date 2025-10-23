//! Dictionary index metadata functionality tests
//! Based on gluesql/test-suite/src/dictionary_index.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_simple_index() {
    // TODO: Test CREATE INDEX Foo_id ON Foo (id)
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_expression_index() {
    // TODO: Test CREATE INDEX Foo_id_2 ON Foo (id + 2)
}

#[ignore = "not yet implemented"]
#[test]
fn test_proven_indexes_simple_indexes() {
    // TODO: Test SELECT * FROM PROVEN_INDEXES returns index metadata for simple and expression indexes
    // Should include: TABLE_NAME, INDEX_NAME, ORDER, EXPRESSION, UNIQUENESS
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_primary_key() {
    // TODO: Test CREATE TABLE Bar (id INT PRIMARY KEY, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_expression_index_on_concatenation() {
    // TODO: Test CREATE INDEX Bar_name_concat ON Bar (name + '_')
}

#[ignore = "not yet implemented"]
#[test]
fn test_proven_indexes_with_primary_key() {
    // TODO: Test SELECT * FROM PROVEN_INDEXES includes PRIMARY key index
    // Should show PRIMARY index as unique=true, others as unique=false
}

#[ignore = "not yet implemented"]
#[test]
fn test_proven_indexes_all_tables() {
    // TODO: Test PROVEN_INDEXES returns indexes from both Bar and Foo tables
    // Bar: PRIMARY (unique), Bar_name_concat (not unique)
    // Foo: Foo_id (not unique), Foo_id_2 (not unique)
}

#[ignore = "not yet implemented"]
#[test]
fn test_drop_primary_index_should_error() {
    // TODO: Test DROP INDEX Bar.PRIMARY should error: CannotDropPrimary
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_reserved_index_name_should_error() {
    // TODO: Test CREATE INDEX Primary ON Foo (id) should error: ReservedIndexName
}

#[ignore = "not yet implemented"]
#[test]
fn test_index_metadata_structure() {
    // TODO: Test that PROVEN_INDEXES has correct column structure and data types
}

#[ignore = "not yet implemented"]
#[test]
fn test_index_order_and_expression_fields() {
    // TODO: Test that ORDER field shows "BOTH" and EXPRESSION shows the indexed expression
}
