//! Tests for ALTER TABLE statements
//! Based on gluesql/test-suite/src/alter/alter_table.rs

use crate::common::setup_test;
use proven_value::Value;

#[test]
fn test_alter_table_rename_table() {
    let mut ctx = setup_test();

    // Create table and insert data
    ctx.exec("CREATE TABLE Foo (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO Foo VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    // Verify initial data
    let results = ctx.query("SELECT id FROM Foo ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));

    // Rename table
    ctx.exec("ALTER TABLE Foo RENAME TO Bar");

    // Verify data accessible with new name
    let results = ctx.query("SELECT id FROM Bar ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
#[should_panic(expected = "TableNotFound")]
fn test_alter_table_rename_nonexistent_table_error() {
    let mut ctx = setup_test();

    // Test that ALTER TABLE RENAME on non-existent table throws error
    ctx.exec("ALTER TABLE Foo2 RENAME TO Bar");
}

#[test]
fn test_alter_table_rename_column() {
    let mut ctx = setup_test();

    // Create table and insert data
    ctx.exec("CREATE TABLE Bar (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO Bar VALUES (1, 'a'), (2, 'b'), (3, 'c')");

    // Rename column
    ctx.exec("ALTER TABLE Bar RENAME COLUMN id TO new_id");

    // Verify data accessible with new column name
    let results = ctx.query("SELECT new_id FROM Bar ORDER BY new_id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("new_id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("new_id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("new_id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
#[should_panic(expected = "RenamingColumnNotFound")]
fn test_alter_table_rename_nonexistent_column_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Bar (id INTEGER, name TEXT)");

    // Test that renaming non-existent column throws error
    ctx.exec("ALTER TABLE Bar RENAME COLUMN hello TO idid");
}

#[test]
#[should_panic(expected = "AlreadyExistingColumn")]
fn test_alter_table_rename_to_duplicate_column_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Bar (new_id INTEGER, name TEXT)");

    // Test that renaming column to existing column name throws error
    ctx.exec("ALTER TABLE Bar RENAME COLUMN name TO new_id");
}

#[test]
#[should_panic(expected = "DefaultValueRequired")]
fn test_alter_table_add_column_not_null_no_default_error() {
    let mut ctx = setup_test();

    // Create table with existing data
    ctx.exec("CREATE TABLE Foo (id INTEGER)");
    ctx.exec("INSERT INTO Foo VALUES (1), (2)");

    // Test that ADD COLUMN NOT NULL without DEFAULT value fails
    ctx.exec("ALTER TABLE Foo ADD COLUMN amount INTEGER NOT NULL");
}

#[test]
#[should_panic(expected = "AlreadyExistingColumn")]
fn test_alter_table_add_existing_column_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that ADD COLUMN with existing column name throws error
    ctx.exec("ALTER TABLE Foo ADD COLUMN id INTEGER");
}

#[test]
fn test_alter_table_add_column_with_default() {
    let mut ctx = setup_test();

    // Create table with data
    ctx.exec("CREATE TABLE Foo (id INTEGER)");
    ctx.exec("INSERT INTO Foo VALUES (1), (2)");

    // Add column with default value
    ctx.exec("ALTER TABLE Foo ADD COLUMN amount INTEGER DEFAULT 10");

    // Verify default value applied to existing rows
    let results = ctx.query("SELECT * FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("amount").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("amount").unwrap(), &Value::I32(10));

    ctx.commit();
}

#[test]
fn test_alter_table_add_nullable_column() {
    let mut ctx = setup_test();

    // Create table with data
    ctx.exec("CREATE TABLE Foo (id INTEGER, amount INTEGER)");
    ctx.exec("INSERT INTO Foo VALUES (1, 10), (2, 10)");

    // Add nullable column
    ctx.exec("ALTER TABLE Foo ADD COLUMN opt BOOLEAN NULL");

    // Verify NULL values for existing rows
    let results = ctx.query("SELECT * FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("amount").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("opt").unwrap(), &Value::Null);
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("amount").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("opt").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_alter_table_add_nullable_column_with_default() {
    let mut ctx = setup_test();

    // Create table with data
    ctx.exec("CREATE TABLE Foo (id INTEGER, amount INTEGER, opt BOOLEAN NULL)");
    ctx.exec("INSERT INTO Foo VALUES (1, 10, NULL), (2, 10, NULL)");

    // Add nullable column with default
    ctx.exec("ALTER TABLE Foo ADD COLUMN opt2 BOOLEAN NULL DEFAULT true");

    // Verify default value applied to existing rows
    let results = ctx.query("SELECT * FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("amount").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("opt").unwrap(), &Value::Null);
    assert_eq!(results[0].get("opt2").unwrap(), &Value::Bool(true));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("amount").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("opt").unwrap(), &Value::Null);
    assert_eq!(results[1].get("opt2").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
#[should_panic(expected = "Expression type not supported")]
fn test_alter_table_add_column_subquery_default_error() {
    let mut ctx = setup_test();

    // Create tables
    ctx.exec("CREATE TABLE Foo (id INTEGER)");
    ctx.exec("CREATE TABLE Bar (id INTEGER)");

    // Test that ADD COLUMN with subquery in DEFAULT value fails
    ctx.exec("ALTER TABLE Foo ADD COLUMN something INTEGER DEFAULT (SELECT id FROM Bar LIMIT 1)");
}

#[test]
#[should_panic(expected = "Parse error")]
fn test_alter_table_add_column_unsupported_datatype_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that ADD COLUMN with unsupported data type fails
    ctx.exec("ALTER TABLE Foo ADD COLUMN something SOMEWHAT");
}

#[test]
#[should_panic(expected = "UnsupportedDataTypeForUniqueColumn")]
fn test_alter_table_add_column_unique_float_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that ADD COLUMN with UNIQUE FLOAT fails
    ctx.exec("ALTER TABLE Foo ADD COLUMN something FLOAT UNIQUE");
}

#[test]
fn test_alter_table_drop_column_if_exists() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER, amount INTEGER)");

    // Test DROP COLUMN IF EXISTS on non-existent column (should succeed silently)
    ctx.exec("ALTER TABLE Foo DROP COLUMN IF EXISTS something");

    // Verify table still exists
    ctx.exec("INSERT INTO Foo VALUES (1, 10)");
    let results = ctx.query("SELECT * FROM Foo");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
#[should_panic(expected = "DroppingColumnNotFound")]
fn test_alter_table_drop_nonexistent_column_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that DROP COLUMN on non-existent column throws error
    ctx.exec("ALTER TABLE Foo DROP COLUMN something");
}

#[test]
fn test_alter_table_drop_existing_column() {
    let mut ctx = setup_test();

    // Create table with data
    ctx.exec("CREATE TABLE Foo (id INTEGER, amount INTEGER, opt BOOLEAN NULL)");
    ctx.exec("INSERT INTO Foo VALUES (1, 10, NULL), (2, 20, NULL)");

    // Drop column
    ctx.exec("ALTER TABLE Foo DROP COLUMN amount");

    // Verify column is dropped
    let results = ctx.query("SELECT * FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("opt").unwrap(), &Value::Null);
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("opt").unwrap(), &Value::Null);

    // Also test DROP COLUMN IF EXISTS
    ctx.exec("ALTER TABLE Foo DROP COLUMN IF EXISTS opt");
    let results = ctx.query("SELECT * FROM Foo ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
#[should_panic(expected = "UnsupportedAlterTableOperation")]
fn test_alter_table_add_constraint_primary_key_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that ADD CONSTRAINT PRIMARY KEY is not supported
    ctx.exec(r#"ALTER TABLE Foo ADD CONSTRAINT "hey" PRIMARY KEY (asdf)"#);
}

#[test]
#[should_panic(expected = "UnsupportedAlterTableOperation")]
fn test_alter_table_add_constraint_unique_error() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Foo (id INTEGER)");

    // Test that ADD CONSTRAINT UNIQUE is not supported
    ctx.exec("ALTER TABLE Foo ADD CONSTRAINT hello UNIQUE (id)");
}

#[test]
#[should_panic(expected = "CannotAlterReferencedColumn")]
fn test_alter_table_cannot_drop_referenced_column() {
    let mut ctx = setup_test();

    // Create referenced table
    ctx.exec("CREATE TABLE Referenced (id INTEGER PRIMARY KEY)");

    // Create referencing table with foreign key
    ctx.exec(
        "CREATE TABLE Referencing (
            id INTEGER,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
        )",
    );

    // Test that dropping a column referenced by foreign key fails
    ctx.exec("ALTER TABLE Referenced DROP COLUMN id");
}

#[test]
#[should_panic(expected = "CannotAlterReferencedColumn")]
fn test_alter_table_cannot_rename_referenced_column() {
    let mut ctx = setup_test();

    // Create referenced table
    ctx.exec("CREATE TABLE Referenced (id INTEGER PRIMARY KEY)");

    // Create referencing table with foreign key
    ctx.exec(
        "CREATE TABLE Referencing (
            id INTEGER,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
        )",
    );

    // Test that renaming a column referenced by foreign key fails
    ctx.exec("ALTER TABLE Referenced RENAME COLUMN id to new_id");
}

#[test]
#[should_panic(expected = "CannotAlterReferencingColumn")]
fn test_alter_table_cannot_drop_referencing_column() {
    let mut ctx = setup_test();

    // Create referenced table
    ctx.exec("CREATE TABLE Referenced (id INTEGER PRIMARY KEY)");

    // Create referencing table with foreign key
    ctx.exec(
        "CREATE TABLE Referencing (
            id INTEGER,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
        )",
    );

    // Test that dropping a column that has foreign key reference fails
    ctx.exec("ALTER TABLE Referencing DROP COLUMN referenced_id");
}

#[test]
#[should_panic(expected = "CannotAlterReferencingColumn")]
fn test_alter_table_cannot_rename_referencing_column() {
    let mut ctx = setup_test();

    // Create referenced table
    ctx.exec("CREATE TABLE Referenced (id INTEGER PRIMARY KEY)");

    // Create referencing table with foreign key
    ctx.exec(
        "CREATE TABLE Referencing (
            id INTEGER,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES Referenced (id)
        )",
    );

    // Test that renaming a column that has foreign key reference fails
    ctx.exec("ALTER TABLE Referencing RENAME COLUMN referenced_id to new_id");
}
