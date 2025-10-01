//! Dictionary/metadata functionality tests
//! Based on gluesql/test-suite/src/dictionary.rs

#[ignore = "not yet implemented"]
#[test]
fn test_show_version() {
    // TODO: Test SHOW VERSION returns version information
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_tables_empty() {
    // TODO: Test SHOW TABLES returns empty list when no tables exist
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_comment() {
    // TODO: Test CREATE TABLE Foo (id INTEGER, name TEXT NULL, type TEXT NULL) COMMENT='this is table comment'
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_tables_single() {
    // TODO: Test SHOW TABLES returns ["Foo"] after creating one table
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_primary_key_and_comment() {
    // TODO: Test CREATE TABLE Zoo (id INTEGER PRIMARY KEY COMMENT 'hello')
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_unique_and_default() {
    // TODO: Test CREATE TABLE Bar (id INTEGER UNIQUE, name TEXT NOT NULL DEFAULT 'NONE')
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_tables_multiple_sorted() {
    // TODO: Test SHOW TABLES returns ["Bar", "Foo", "Zoo"] (alphabetically sorted)
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_unsupported_keyword_should_error() {
    // TODO: Test SHOW WHATEVER should error: UnsupportedShowVariableKeyword
}

#[ignore = "not yet implemented"]
#[test]
fn test_show_invalid_statement_should_error() {
    // TODO: Test SHOW ME THE CHICKEN should error: UnsupportedShowVariableStatement
}

#[ignore = "not yet implemented"]
#[test]
fn test_glue_tables_system_view() {
    // TODO: Test SELECT * FROM PROVEN_TABLES returns table names and comments
}

#[ignore = "not yet implemented"]
#[test]
fn test_glue_table_columns_system_view() {
    // TODO: Test SELECT * FROM PROVEN_TABLE_COLUMNS returns column metadata:
    // TABLE_NAME, COLUMN_NAME, COLUMN_ID, NULLABLE, KEY, DEFAULT, COMMENT
}

#[ignore = "not yet implemented"]
#[test]
fn test_glue_tables_columns_bar_table() {
    // TODO: Test Bar table columns: id (UNIQUE, nullable), name (NOT NULL, DEFAULT 'NONE')
}

#[ignore = "not yet implemented"]
#[test]
fn test_glue_tables_columns_foo_table() {
    // TODO: Test Foo table columns: id, name, type (all nullable, no constraints)
}

#[ignore = "not yet implemented"]
#[test]
fn test_glue_tables_columns_zoo_table() {
    // TODO: Test Zoo table columns: id (PRIMARY KEY, not nullable, with comment 'hello')
}
