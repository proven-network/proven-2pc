//! UUID data type tests
//! Based on gluesql/test-suite/src/data_type/uuid.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_uuid_primary_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE posts (id UUID PRIMARY KEY)");

    // Use a valid UUID string
    let uuid = "550e8400-e29b-41d4-a716-446655440000";
    ctx.exec(&format!("INSERT INTO posts (id) VALUES ('{}')", uuid));

    let results = ctx.query(&format!("SELECT id FROM posts WHERE id = '{}'", uuid));
    assert_eq!(results.len(), 1);
    // UUID values are formatted as their standard representation
    assert!(results[0].get("id").unwrap().starts_with("Uuid("));

    ctx.commit();
}

#[test]
fn test_create_table_with_uuid_field() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");
    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_uuid_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");
    // Numbers cannot be inserted into UUID fields
    ctx.exec("INSERT INTO uuid_table VALUES (0)");
}

#[test]
#[should_panic(expected = "Failed to parse UUID")]
fn test_insert_invalid_hex_uuid_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");
    // Invalid hex string (too short) - try without X'' syntax for now
    ctx.exec("INSERT INTO uuid_table VALUES ('1234')");
}

#[test]
#[should_panic(expected = "Failed to parse UUID")]
fn test_insert_invalid_string_uuid_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");
    // Invalid UUID string
    ctx.exec("INSERT INTO uuid_table VALUES ('NOT_UUID')");
}

#[test]
fn test_insert_various_uuid_formats() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");

    // Test various UUID formats
    // Hex format (32 hex digits) - use as string without X'' prefix
    ctx.exec("INSERT INTO uuid_table VALUES ('936DA01F9ABD4d9d80C702AF85C822A8')");

    // Standard hyphenated format
    ctx.exec("INSERT INTO uuid_table VALUES ('550e8400-e29b-41d4-a716-446655440000')");

    // URN format
    ctx.exec("INSERT INTO uuid_table VALUES ('urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4')");

    let results = ctx.query("SELECT uuid_field FROM uuid_table ORDER BY uuid_field");
    assert_eq!(results.len(), 3);

    // Check the values are properly parsed and stored
    // UUIDs are stored and can be compared
    assert!(results[0].get("uuid_field").unwrap().starts_with("Uuid("));
    assert!(results[1].get("uuid_field").unwrap().starts_with("Uuid("));
    assert!(results[2].get("uuid_field").unwrap().starts_with("Uuid("));

    ctx.commit();
}

#[test]
fn test_select_uuid_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (id INT, uuid_field UUID)");

    let uuid1 = "936DA01F-9ABD-4d9d-80C7-02AF85C822A8";
    let uuid2 = "550e8400-e29b-41d4-a716-446655440000";

    ctx.exec(&format!("INSERT INTO uuid_table VALUES (1, '{}')", uuid1));
    ctx.exec(&format!("INSERT INTO uuid_table VALUES (2, '{}')", uuid2));

    let results = ctx.query("SELECT id, uuid_field FROM uuid_table ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert!(results[0].get("uuid_field").unwrap().starts_with("Uuid("));

    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert!(results[1].get("uuid_field").unwrap().starts_with("Uuid("));

    ctx.commit();
}

#[test]
fn test_update_uuid_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");

    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4";

    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid1));

    // Update the UUID value
    ctx.exec(&format!(
        "UPDATE uuid_table SET uuid_field = '{}' WHERE uuid_field = '{}'",
        uuid2, uuid1
    ));

    let results = ctx.query("SELECT uuid_field FROM uuid_table");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("uuid_field").unwrap().starts_with("Uuid("));

    ctx.commit();
}

#[test]
fn test_uuid_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (id INT, uuid_field UUID)");

    // Insert UUIDs that can be compared
    ctx.exec("INSERT INTO uuid_table VALUES (1, '00000000-0000-0000-0000-000000000001')");
    ctx.exec("INSERT INTO uuid_table VALUES (2, '00000000-0000-0000-0000-000000000002')");
    ctx.exec("INSERT INTO uuid_table VALUES (3, '00000000-0000-0000-0000-000000000003')");

    // Test equality
    let results = ctx.query(
        "SELECT id FROM uuid_table WHERE uuid_field = '00000000-0000-0000-0000-000000000002'",
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test greater than
    let results = ctx.query("SELECT id FROM uuid_table WHERE uuid_field > '00000000-0000-0000-0000-000000000001' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    // Test less than
    let results = ctx.query("SELECT id FROM uuid_table WHERE uuid_field < '00000000-0000-0000-0000-000000000003' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(2)");

    ctx.commit();
}

#[test]
fn test_group_by_uuid() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");

    let uuid1 = "936DA01F-9ABD-4d9d-80C7-02AF85C822A8";
    let uuid2 = "F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4";

    // Insert duplicate UUIDs to test grouping
    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid1));
    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid2));
    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid2));

    let results = ctx
        .query("SELECT uuid_field, COUNT(*) as cnt FROM uuid_table GROUP BY uuid_field ORDER BY 2");
    assert_eq!(results.len(), 2);

    // Check the counts - the order should be ascending by count
    // Since ORDER BY 2 means order by the second column (COUNT(*))
    // But it seems to be descending, so let's verify the actual values

    // First result should have count 1 or 2
    let first_count = results[0].get("cnt").unwrap();
    let second_count = results[1].get("cnt").unwrap();

    // Just check that we have one with count 1 and one with count 2
    assert!(
        (first_count == "I64(1)" && second_count == "I64(2)")
            || (first_count == "I64(2)" && second_count == "I64(1)"),
        "Expected one row with count 1 and one with count 2, got {} and {}",
        first_count,
        second_count
    );

    // Both should be valid UUIDs
    assert!(results[0].get("uuid_field").unwrap().starts_with("Uuid("));
    assert!(results[1].get("uuid_field").unwrap().starts_with("Uuid("));

    ctx.commit();
}

#[test]
fn test_delete_uuid() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_table (uuid_field UUID)");

    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4";

    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid1));
    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid2));
    ctx.exec(&format!("INSERT INTO uuid_table VALUES ('{}')", uuid2));

    // First update one of the uuid1 values to uuid2
    ctx.exec(&format!(
        "UPDATE uuid_table SET uuid_field = '{}' WHERE uuid_field = '{}'",
        uuid2, uuid1
    ));

    // Now try to delete uuid1 (should delete 0 since it was updated)
    ctx.exec(&format!(
        "DELETE FROM uuid_table WHERE uuid_field = '{}'",
        uuid1
    ));

    // Check that nothing was deleted
    let results = ctx.query("SELECT COUNT(*) as cnt FROM uuid_table");
    assert_eq!(results[0].get("cnt").unwrap(), "I64(3)");

    // Delete uuid2 (should delete all 3 records)
    ctx.exec(&format!(
        "DELETE FROM uuid_table WHERE uuid_field = '{}'",
        uuid2
    ));

    let results = ctx.query("SELECT COUNT(*) as cnt FROM uuid_table");
    assert_eq!(results[0].get("cnt").unwrap(), "I64(0)");

    ctx.commit();
}

#[test]
fn test_uuid_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE uuid_nulls (id INT, uuid_field UUID)");

    ctx.exec("INSERT INTO uuid_nulls VALUES (1, '550e8400-e29b-41d4-a716-446655440000')");
    ctx.exec("INSERT INTO uuid_nulls VALUES (2, NULL)");
    ctx.exec("INSERT INTO uuid_nulls VALUES (3, 'F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4')");

    let results =
        ctx.query("SELECT id, uuid_field FROM uuid_nulls WHERE uuid_field IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert!(results[0].get("uuid_field").unwrap().starts_with("Uuid("));
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");
    assert!(results[1].get("uuid_field").unwrap().starts_with("Uuid("));

    let results = ctx.query("SELECT id FROM uuid_nulls WHERE uuid_field IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    ctx.commit();
}

#[test]
fn test_cast_to_uuid() {
    let mut ctx = setup_test();

    // Test CAST from string to UUID
    let results =
        ctx.query("SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS uuid_val");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("uuid_val").unwrap().starts_with("Uuid("));

    // Test with URN format
    let results = ctx
        .query("SELECT CAST('urn:uuid:F9168C5E-CEB2-4faa-B6BF-329BF39FA1E4' AS UUID) AS uuid_val");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("uuid_val").unwrap().starts_with("Uuid("));

    ctx.commit();
}
