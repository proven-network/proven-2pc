//! Identity data type tests
//!
//! Identity represents authenticated users and should NOT be creatable from
//! string literals. It can only come from:
//! 1. CURRENT_IDENTITY() function (future implementation)
//! 2. Parameters/bindings from application layer
//! 3. Database reads (existing Identity values)

mod common;

use common::setup_test;
use proven_value::{Identity, Value};
use uuid::Uuid;

#[test]
fn test_create_table_with_identity_field() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (id INT, owner_identity IDENTITY)");
    ctx.commit();
}

#[test]
fn test_create_table_with_identity_primary_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE users (identity IDENTITY PRIMARY KEY, name TEXT)");
    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_string_into_identity_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (owner_identity IDENTITY)");

    // Strings cannot be inserted into Identity fields
    // This should fail because Identity can't be created from strings
    ctx.exec("INSERT INTO documents VALUES ('550e8400-e29b-41d4-a716-446655440000')");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_identity_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (owner_identity IDENTITY)");

    // Numbers cannot be inserted into Identity fields
    ctx.exec("INSERT INTO documents VALUES (123)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_string_to_identity_should_error() {
    let mut ctx = setup_test();

    // CAST from string to Identity should fail
    // Identity can only come from authentication or parameters
    ctx.exec("SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS IDENTITY)");
}

#[test]
fn test_identity_display_is_redacted() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let identity = Identity::new(uuid);

    // Display should be redacted
    let display_str = format!("{}", identity);
    assert_eq!(display_str, "[REDACTED IDENTITY]");

    // Should not contain any part of the UUID
    assert!(!display_str.contains("550e"));
    assert!(!display_str.contains("8400"));
}

#[test]
fn test_identity_debug_is_redacted() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let identity = Identity::new(uuid);

    // Debug should be redacted
    let debug_str = format!("{:?}", identity);
    assert_eq!(debug_str, "Identity([REDACTED])");

    // Should not contain any part of the UUID
    assert!(!debug_str.contains("550e"));
    assert!(!debug_str.contains("8400"));
}

#[test]
fn test_identity_equality() {
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();

    let identity1a = Identity::new(uuid1);
    let identity1b = Identity::new(uuid1);
    let identity2 = Identity::new(uuid2);

    // Same UUID should be equal
    assert_eq!(identity1a, identity1b);

    // Different UUIDs should not be equal
    assert_ne!(identity1a, identity2);
}

#[test]
fn test_identity_ordering() {
    let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

    let identity1 = Identity::new(uuid1);
    let identity2 = Identity::new(uuid2);

    // Should be orderable for database operations
    assert!(identity1 < identity2);
    assert!(identity2 > identity1);
}

#[test]
fn test_identity_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (id INT, owner_identity IDENTITY)");

    // NULL should be allowed
    ctx.exec("INSERT INTO documents VALUES (1, NULL)");
    ctx.exec("INSERT INTO documents VALUES (2, NULL)");

    let results = ctx.query("SELECT id FROM documents WHERE owner_identity IS NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_identity_value_is_redacted_in_query_results() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let identity = Identity::new(uuid);

    // When converted to string (as would happen in query results),
    // it should show redacted text
    let value = Value::Identity(identity);
    let display_str = format!("{}", value);

    // Should be redacted
    assert_eq!(display_str, "[REDACTED IDENTITY]");

    // Should not leak the UUID
    assert!(!display_str.contains("550e"));
    assert!(!display_str.contains("8400"));
}

// Tests for parameterized inserts - the proper way to insert Identity values
// NOTE: These tests are currently disabled due to parameter binding issues
// The error "expected Identity, got identity" suggests a case sensitivity issue
// in the type matching code that needs to be fixed.
//
// CONFIRMED: Parameter binding works for UUID (see data_type_uuid.rs::test_insert_uuid_via_parameters)
// This is specifically an Identity type name matching issue, not a general parameter problem.

#[test]
fn test_insert_identity_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (id INT, owner_identity IDENTITY, content TEXT)");

    // Create identity values
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let identity1 = Identity::new(uuid1);
    let identity2 = Identity::new(uuid2);

    // Insert using parameters (the proper way)
    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::Identity(identity1.clone()),
            Value::Str("doc1".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::Identity(identity2.clone()),
            Value::Str("doc2".to_string()),
        ],
    );

    // Query all documents
    let results = ctx.query("SELECT id, content FROM documents ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("content").unwrap(),
        &Value::Str("doc1".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("content").unwrap(),
        &Value::Str("doc2".to_string())
    );

    ctx.commit();
}

#[test]
fn test_query_identity_with_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (id INT, owner_identity IDENTITY, content TEXT)");

    // Create and insert identities
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let identity1 = Identity::new(uuid1);
    let identity2 = Identity::new(uuid2);

    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::Identity(identity1.clone()),
            Value::Str("alice's doc".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::Identity(identity2.clone()),
            Value::Str("bob's doc".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?, ?)",
        vec![
            Value::I32(3),
            Value::Identity(identity1.clone()),
            Value::Str("alice's doc 2".to_string()),
        ],
    );

    // First, check all inserted documents
    let all_results = ctx.query("SELECT id, owner_identity, content FROM documents ORDER BY id");
    println!("All documents: {:?}", all_results);
    assert_eq!(all_results.len(), 3, "Should have 3 documents");

    // Query by specific identity using parameters
    let results = ctx.query_with_params(
        "SELECT id, content FROM documents WHERE owner_identity = ? ORDER BY id",
        vec![Value::Identity(identity1.clone())],
    );

    println!("Filtered results: {:?}", results);
    println!("Searching for identity: {:?}", identity1);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("content").unwrap(),
        &Value::Str("alice's doc".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(
        results[1].get("content").unwrap(),
        &Value::Str("alice's doc 2".to_string())
    );

    ctx.commit();
}

#[test]
fn test_identity_equality_in_queries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE documents (id INT, owner_identity IDENTITY)");

    // Create two identical identity values
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let identity1 = Identity::new(uuid);
    let identity2 = Identity::new(uuid); // Same UUID

    ctx.exec_with_params(
        "INSERT INTO documents VALUES (?, ?)",
        vec![Value::I32(1), Value::Identity(identity1)],
    );

    // Query with the "different" identity object (but same UUID)
    let results = ctx.query_with_params(
        "SELECT id FROM documents WHERE owner_identity = ?",
        vec![Value::Identity(identity2)],
    );

    // Should find the document because UUIDs match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

// Debug test to isolate the parameter comparison issue
#[test]
fn test_identity_parameter_simple_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE test_table (id INT, identity_col IDENTITY)");

    // Create identity
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let identity = Identity::new(uuid);

    // Insert via parameter
    ctx.exec_with_params(
        "INSERT INTO test_table VALUES (?, ?)",
        vec![Value::I32(1), Value::Identity(identity.clone())],
    );

    // Check it was inserted
    let all = ctx.query("SELECT * FROM test_table");
    println!("Inserted row: {:?}", all);
    assert_eq!(all.len(), 1);

    // Get the actual identity value from the database
    let stored_identity = all[0].get("identity_col").unwrap();
    println!("Stored identity: {:?}", stored_identity);
    println!("Original identity: {:?}", Value::Identity(identity.clone()));
    println!(
        "Are they equal? {}",
        stored_identity == &Value::Identity(identity.clone())
    );

    // Try to query with the exact same parameter
    let results = ctx.query_with_params(
        "SELECT * FROM test_table WHERE identity_col = ?",
        vec![Value::Identity(identity.clone())],
    );
    println!("Query results: {:?}", results);
    assert_eq!(
        results.len(),
        1,
        "Should find the row with matching identity"
    );

    ctx.commit();
}

// NOTE: Future tests to add when CURRENT_IDENTITY() is implemented:
// - test_current_identity_function() - SELECT CURRENT_IDENTITY()
// - test_row_level_security() - WHERE owner_identity = CURRENT_IDENTITY()
// - test_identity_in_joins() - JOIN on identity fields
