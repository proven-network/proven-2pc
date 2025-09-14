//! INSERT statement tests
//! Based on gluesql/test-suite/src/insert.rs

mod common;

use common::{TableBuilder, data, setup_test};

#[test]
fn test_basic_insert_single_item() {
    let mut ctx = setup_test();

    // Create test table using TableBuilder
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL")
        .insert_values("(1, 2, 'Hi boo')");

    // Verify the data
    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_contains("SELECT name FROM Test", "name", "Str(\"Hi boo\")");
    ctx.assert_query_contains("SELECT num FROM Test", "num", "I32(2)");

    ctx.commit();
}

#[test]
fn test_insert_multiple_rows() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL")
        .insert_values("(3, 9, 'Kitty!'), (2, 7, 'Monsters')");

    assert_rows!(ctx, "SELECT * FROM Test", 2);
    ctx.assert_query_contains(
        "SELECT name FROM Test WHERE id = 2",
        "name",
        "Str(\"Monsters\")",
    );
    ctx.assert_query_contains(
        "SELECT name FROM Test WHERE id = 3",
        "name",
        "Str(\"Kitty!\")",
    );

    ctx.commit();
}

#[test]
fn test_insert_without_column_list() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL");

    // Insert without specifying columns (uses all columns in order)
    ctx.exec("INSERT INTO Test VALUES(17, 30, 'Sullivan')");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_contains("SELECT * FROM Test", "id", "I32(17)");
    ctx.assert_query_contains("SELECT * FROM Test", "num", "I32(30)");
    ctx.assert_query_contains("SELECT * FROM Test", "name", "Str(\"Sullivan\")");

    ctx.commit();
}

#[test]
#[ignore = "DEFAULT values not yet fully implemented"]
fn test_insert_with_default_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL");

    // Insert with id using DEFAULT value
    ctx.exec("INSERT INTO Test (num, name) VALUES (28, 'Wazowski')");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_contains("SELECT * FROM Test", "id", "I32(1)"); // Should use DEFAULT value of 1
    ctx.assert_query_contains("SELECT * FROM Test", "num", "I32(28)");
    ctx.assert_query_contains("SELECT * FROM Test", "name", "Str(\"Wazowski\")");

    ctx.commit();
}

#[test]
fn test_insert_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL");

    // Insert with num as NULL (not specified, and it's nullable)
    ctx.exec("INSERT INTO Test (id, name) VALUES (1, 'The end')");

    assert_rows!(ctx, "SELECT * FROM Test", 1);
    ctx.assert_query_contains("SELECT * FROM Test", "id", "I32(1)");
    ctx.assert_query_contains("SELECT * FROM Test", "num", "Null");
    ctx.assert_query_contains("SELECT * FROM Test", "name", "Str(\"The end\")");

    ctx.commit();
}

#[test]
#[ignore = "NOT NULL constraint validation not yet implemented"]
fn test_insert_missing_not_null_column() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL");

    // Try to insert without providing required NOT NULL column
    assert_error!(ctx, "INSERT INTO Test (id, num) VALUES (1, 10)", "name");

    ctx.abort();
}

#[test]
fn test_verify_inserted_data() {
    let mut ctx = setup_test();

    // Use TableBuilder to create and populate table
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER DEFAULT 1, num INTEGER NULL, name TEXT NOT NULL");

    // Insert multiple rows with different patterns using helper and manual inserts
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hi boo')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, 9, 'Kitty!'), (2, 7, 'Monsters')");
    ctx.exec("INSERT INTO Test VALUES(17, 30, 'Sullivan')");
    ctx.exec("INSERT INTO Test (id, name) VALUES (5, 'The end')"); // num will be NULL

    // Verify all data
    assert_rows!(ctx, "SELECT * FROM Test", 5);

    // Check specific values using assert_query_contains
    ctx.assert_query_contains(
        "SELECT name FROM Test WHERE id = 1",
        "name",
        "Str(\"Hi boo\")",
    );
    ctx.assert_query_contains(
        "SELECT name FROM Test WHERE id = 17",
        "name",
        "Str(\"Sullivan\")",
    );
    ctx.assert_query_contains("SELECT num FROM Test WHERE id = 5", "num", "Null");

    ctx.commit();
}

#[test]
#[ignore = "CREATE TABLE AS SELECT not yet implemented"]
fn test_create_table_as_select() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 10, 'test')");

    // Create new table from SELECT (empty due to WHERE 1 = 0)
    ctx.exec("CREATE TABLE Target AS SELECT * FROM Test WHERE 1 = 0");

    // Target should exist but be empty
    assert_rows!(ctx, "SELECT * FROM Target", 0);

    ctx.commit();
}

#[test]
fn test_insert_from_select() {
    let mut ctx = setup_test();

    // Create source table with data using TableBuilder
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 10, 'Alice'), (2, 20, 'Bob'), (3, 30, 'Charlie')");

    // Create target table with same schema
    TableBuilder::new(&mut ctx, "Target").create_simple("id INTEGER, num INTEGER, name TEXT");

    // Insert from SELECT
    ctx.exec("INSERT INTO Target SELECT * FROM Test");

    // Verify data was copied
    assert_rows!(ctx, "SELECT * FROM Target", 3);
    ctx.assert_query_contains(
        "SELECT name FROM Target WHERE id = 1",
        "name",
        "Str(\"Alice\")",
    );
    ctx.assert_query_contains(
        "SELECT name FROM Target WHERE id = 3",
        "name",
        "Str(\"Charlie\")",
    );

    ctx.commit();
}

#[test]
fn test_verify_insert_from_select() {
    let mut ctx = setup_test();

    // Setup source table with TableBuilder
    TableBuilder::new(&mut ctx, "Test")
        .create_simple("id INTEGER, num INTEGER, name TEXT")
        .insert_values("(1, 10, 'First'), (2, 20, 'Second')");

    // Setup target and copy data
    TableBuilder::new(&mut ctx, "Target").create_simple("id INTEGER, num INTEGER, name TEXT");

    ctx.exec("INSERT INTO Target SELECT * FROM Test");

    // Verify both tables have same row count
    assert_rows!(ctx, "SELECT * FROM Test", 2);
    assert_rows!(ctx, "SELECT * FROM Target", 2);

    // Verify specific data matches
    ctx.assert_query_contains(
        "SELECT name FROM Target WHERE id = 1",
        "name",
        "Str(\"First\")",
    );
    ctx.assert_query_contains(
        "SELECT name FROM Target WHERE id = 2",
        "name",
        "Str(\"Second\")",
    );

    ctx.commit();
}

#[test]
fn test_insert_with_generated_data() {
    let mut ctx = setup_test();

    // Create a standard table using the builder's predefined schema
    TableBuilder::new(&mut ctx, "Generated")
        .create_standard() // Creates: id INTEGER PRIMARY KEY, name TEXT, value INTEGER
        .insert_rows(3); // Inserts 3 rows with generated data

    assert_rows!(ctx, "SELECT * FROM Generated", 3);

    // Verify generated data pattern
    ctx.assert_query_contains(
        "SELECT name FROM Generated WHERE id = 1",
        "name",
        "Str(\"item_1\")",
    );
    ctx.assert_query_contains(
        "SELECT value FROM Generated WHERE id = 2",
        "value",
        "I32(20)",
    ); // 2 * 10
    ctx.assert_query_contains(
        "SELECT value FROM Generated WHERE id = 3",
        "value",
        "I32(30)",
    ); // 3 * 10

    ctx.commit();
}

#[test]
fn test_insert_using_data_generators() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "DataTest").create_simple("id INTEGER, name TEXT, value INTEGER");

    // Use data generator helper to create insert values
    let insert_data = data::rows(5); // Generates 5 rows of test data
    ctx.exec(&format!("INSERT INTO DataTest VALUES {}", insert_data));

    assert_rows!(ctx, "SELECT * FROM DataTest", 5);

    // Verify the pattern of generated data
    ctx.assert_query_contains(
        "SELECT name FROM DataTest WHERE id = 5",
        "name",
        "Str(\"item_5\")",
    );
    ctx.assert_query_contains(
        "SELECT value FROM DataTest WHERE id = 5",
        "value",
        "I32(50)",
    );

    ctx.commit();
}
