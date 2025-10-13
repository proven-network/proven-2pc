//! Foreign key constraint tests
//! Based on gluesql/test-suite/src/foreign_key.rs

mod common;

use common::setup_test;
use proven_value::Value;

#[test]

fn test_create_table_referencing_table_without_primary_key() {
    let mut ctx = setup_test();

    // Create table without primary key
    ctx.exec("CREATE TABLE ReferencedTableWithoutPK (id INTEGER, name TEXT)");

    // Creating table with foreign key should fail if referenced table does not have primary key
    assert_error!(
        ctx,
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithoutPK (id)
        )",
        "primary key"
    );

    ctx.abort();
}

#[test]

fn test_create_table_referencing_table_with_only_unique_constraint() {
    let mut ctx = setup_test();

    // Create table with UNIQUE constraint but no PRIMARY KEY
    ctx.exec("CREATE TABLE ReferencedTableWithUnique (id INTEGER UNIQUE, name TEXT)");

    // Creating table with foreign key should fail if referenced table has only Unique constraint
    assert_error!(
        ctx,
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithUnique (id)
        )",
        "primary key"
    );

    ctx.abort();
}

#[test]

fn test_create_table_with_foreign_key_different_data_types() {
    let mut ctx = setup_test();

    // Create referenced table with INTEGER primary key
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // Creating table with foreign key on different data type should fail
    assert_error!(
        ctx,
        "CREATE TABLE ReferencingTable (
            id TEXT,
            name TEXT,
            referenced_id TEXT,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
        "type mismatch"
    );

    ctx.abort();
}

#[test]
fn test_foreign_key_on_delete_cascade() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // CASCADE option should now be supported
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE CASCADE
        )",
    );

    // Insert test data
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'parent1')");
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (2, 'parent2')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (10, 'child1', 1)");
    ctx.exec("INSERT INTO ReferencingTable VALUES (20, 'child2', 1)");
    ctx.exec("INSERT INTO ReferencingTable VALUES (30, 'child3', 2)");

    // Delete parent should cascade to children
    ctx.exec("DELETE FROM ReferencedTableWithPK WHERE id = 1");

    // Check that parent is deleted
    assert_rows!(ctx, "SELECT * FROM ReferencedTableWithPK", 1);
    // Check that children referencing parent 1 are also deleted
    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 1);
    assert_rows!(
        ctx,
        "SELECT * FROM ReferencingTable WHERE referenced_id = 2",
        1
    );

    ctx.commit();
}

#[test]
fn test_foreign_key_on_delete_set_default() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // SET DEFAULT option should now be supported
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER DEFAULT 999,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET DEFAULT
        )",
    );

    // Insert test data (including the default reference)
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'parent1')");
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (999, 'default')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (10, 'child1', 1)");

    // Delete parent should set child to default
    ctx.exec("DELETE FROM ReferencedTableWithPK WHERE id = 1");

    // Check that child now references default
    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 10",
        "referenced_id",
        Value::I32(999),
    );

    ctx.commit();
}

#[test]
fn test_foreign_key_on_delete_set_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // SET NULL option should now be supported
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE SET NULL
        )",
    );

    // Insert test data
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'parent1')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (10, 'child1', 1)");

    // Delete parent should set child to NULL
    ctx.exec("DELETE FROM ReferencedTableWithPK WHERE id = 1");

    // Check that child now has NULL reference
    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 10",
        "referenced_id",
        Value::Null,
    );

    ctx.commit();
}

#[test]

fn test_referencing_column_not_found() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // Referencing column not found in table
    assert_error!(
        ctx,
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (wrong_referencing_column) REFERENCES ReferencedTableWithPK (id)
        )",
        "column not found"
    );

    ctx.abort();
}

#[test]

fn test_referenced_column_not_found() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // Referenced column not found in referenced table
    assert_error!(
        ctx,
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (wrong_referenced_column)
        )",
        "column not found"
    );

    ctx.abort();
}

#[test]

fn test_create_table_with_foreign_key_success() {
    let mut ctx = setup_test();

    // Create referenced table with primary key
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");

    // Creating table with foreign key should succeed
    // NO ACTION(=RESTRICT) is default behavior
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            CONSTRAINT MyFkConstraint FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id) ON DELETE NO ACTION ON UPDATE RESTRICT
        )"
    );

    // Verify table was created
    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 0);

    ctx.commit();
}

#[test]

fn test_insert_without_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    // Insert should fail if there is no referenced value
    assert_error!(
        ctx,
        "INSERT INTO ReferencingTable VALUES (1, 'orphan', 1)",
        "referenced value"
    );

    ctx.abort();
}

#[test]

fn test_insert_null_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    // NULL should be allowed even if there is no referenced value
    ctx.exec("INSERT INTO ReferencingTable VALUES (1, 'Null is independent', NULL)");

    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 1);
    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 1",
        "referenced_id",
        Value::Null,
    );

    ctx.commit();
}

#[test]

fn test_insert_with_valid_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    // Insert referenced value first
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");

    // Insert with valid referenced value should succeed
    ctx.exec(
        "INSERT INTO ReferencingTable VALUES (2, 'referencing_table with referenced_table', 1)",
    );

    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 1);
    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 2",
        "referenced_id",
        Value::I32(1),
    );

    ctx.commit();
}

#[test]

fn test_update_without_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables with data
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (2, 'test', 1)");

    // Update should fail if there is no referenced value
    assert_error!(
        ctx,
        "UPDATE ReferencingTable SET referenced_id = 2 WHERE id = 2",
        "referenced value"
    );

    ctx.abort();
}

#[test]

fn test_update_to_null_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables with data
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (2, 'test', 1)");

    // Update to NULL should be allowed even if there is no referenced value
    ctx.exec("UPDATE ReferencingTable SET referenced_id = NULL WHERE id = 2");

    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 2",
        "referenced_id",
        Value::Null,
    );

    ctx.commit();
}

#[test]

fn test_update_with_valid_referenced_value() {
    let mut ctx = setup_test();

    // Setup tables with data
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (3, 'referenced_table3')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (2, 'test', 1)");

    // Update with valid referenced value should succeed
    ctx.exec("UPDATE ReferencingTable SET referenced_id = 3 WHERE id = 2");

    ctx.assert_query_value(
        "SELECT referenced_id FROM ReferencingTable WHERE id = 2",
        "referenced_id",
        Value::I32(3),
    );

    ctx.commit();
}

#[test]

fn test_delete_referenced_row_with_referencing_value() {
    let mut ctx = setup_test();

    // Setup tables with data
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (2, 'test', 1)");

    // Deleting referenced row should fail if referencing value exists (NO ACTION default behavior)
    assert_error!(
        ctx,
        "DELETE FROM ReferencedTableWithPK WHERE id = 1",
        "referencing"
    );

    ctx.abort();
}

#[test]

fn test_delete_referencing_table() {
    let mut ctx = setup_test();

    // Setup tables with data
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencingTable VALUES (2, 'test', 1)");

    // Deleting referencing table row does not care about referenced table
    ctx.exec("DELETE FROM ReferencingTable WHERE id = 2");

    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 0);
    assert_rows!(ctx, "SELECT * FROM ReferencedTableWithPK", 1);

    ctx.commit();
}

#[test]

fn test_table_with_two_foreign_keys() {
    let mut ctx = setup_test();

    // Create two referenced tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("CREATE TABLE ReferencedTableWithPK_2 (id INTEGER PRIMARY KEY, name TEXT)");

    // Create table with two foreign keys
    ctx.exec(
        "CREATE TABLE ReferencingWithTwoFK (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id_1 INTEGER,
            referenced_id_2 INTEGER,
            FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
            FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
        )",
    );

    // Insert referenced values
    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencedTableWithPK_2 VALUES (1, 'referenced_table2')");

    // Insert with valid referenced values should succeed
    ctx.exec("INSERT INTO ReferencingWithTwoFK VALUES (1, 'referencing_table with two referenced_table', 1, 1)");

    assert_rows!(ctx, "SELECT * FROM ReferencingWithTwoFK", 1);

    ctx.commit();
}

#[test]

fn test_update_with_multiple_foreign_keys() {
    let mut ctx = setup_test();

    // Setup tables from previous test
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec("CREATE TABLE ReferencedTableWithPK_2 (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingWithTwoFK (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id_1 INTEGER,
            referenced_id_2 INTEGER,
            FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id),
            FOREIGN KEY (referenced_id_2) REFERENCES ReferencedTableWithPK_2 (id)
        )",
    );

    ctx.exec("INSERT INTO ReferencedTableWithPK VALUES (1, 'referenced_table1')");
    ctx.exec("INSERT INTO ReferencedTableWithPK_2 VALUES (1, 'referenced_table2')");
    ctx.exec("INSERT INTO ReferencingWithTwoFK VALUES (1, 'test', 1, 1)");

    // Cannot update referenced_id_2 if there is no referenced value
    assert_error!(
        ctx,
        "UPDATE ReferencingWithTwoFK SET referenced_id_2 = 9 WHERE id = 1",
        "referenced value"
    );

    ctx.abort();
}

#[test]

fn test_drop_referenced_table_with_referencing_tables() {
    let mut ctx = setup_test();

    // Setup tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            CONSTRAINT MyFkConstraint FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )"
    );

    // Also create a second referencing table
    ctx.exec(
        "CREATE TABLE ReferencingWithTwoFK (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id_1 INTEGER,
            FOREIGN KEY (referenced_id_1) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    // Cannot drop referenced table if referencing table exists
    assert_error!(ctx, "DROP TABLE ReferencedTableWithPK", "referencing");

    ctx.abort();
}

#[test]

fn test_drop_table_with_cascade() {
    let mut ctx = setup_test();

    // Setup tables
    ctx.exec("CREATE TABLE ReferencedTableWithPK (id INTEGER PRIMARY KEY, name TEXT)");
    ctx.exec(
        "CREATE TABLE ReferencingTable (
            id INTEGER,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES ReferencedTableWithPK (id)
        )",
    );

    // Dropping table with CASCADE should drop both table and constraint
    ctx.exec("DROP TABLE ReferencedTableWithPK CASCADE");

    // Referenced table should be dropped
    assert_error!(ctx, "SELECT * FROM ReferencedTableWithPK", "TableNotFound");

    // Referencing table should still exist but constraint should be removed
    // (This would need to be verified with constraint inspection commands)
    assert_rows!(ctx, "SELECT * FROM ReferencingTable", 0);

    ctx.commit();
}

#[test]

fn test_create_self_referencing_table() {
    let mut ctx = setup_test();

    // Should create self referencing table
    ctx.exec(
        "CREATE TABLE SelfReferencingTable (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
        )",
    );

    // Verify table was created
    assert_rows!(ctx, "SELECT * FROM SelfReferencingTable", 0);

    // Insert root node (NULL reference)
    ctx.exec("INSERT INTO SelfReferencingTable VALUES (1, 'root', NULL)");

    // Insert child node
    ctx.exec("INSERT INTO SelfReferencingTable VALUES (2, 'child', 1)");

    assert_rows!(ctx, "SELECT * FROM SelfReferencingTable", 2);

    ctx.commit();
}

#[test]

fn test_drop_self_referencing_table() {
    let mut ctx = setup_test();

    // Create self referencing table
    ctx.exec(
        "CREATE TABLE SelfReferencingTable (
            id INTEGER PRIMARY KEY,
            name TEXT,
            referenced_id INTEGER,
            FOREIGN KEY (referenced_id) REFERENCES SelfReferencingTable (id)
        )",
    );

    // Dropping self referencing table should succeed
    ctx.exec("DROP TABLE SelfReferencingTable");

    // Verify table was dropped
    assert_error!(ctx, "SELECT * FROM SelfReferencingTable", "TableNotFound");

    ctx.commit();
}
