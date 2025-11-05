//! Vault data type tests
//!
//! Vault represents resource accounts (vaults that hold resources).
//! Like Identity, Vault should NOT be creatable from string literals.
//! It can only come from:
//! 1. CREATE_VAULT() function (future implementation)
//! 2. Parameters/bindings from application layer
//! 3. Database reads (existing Vault values)

use crate::common::setup_test;
use proven_value::{Value, Vault};
use uuid::Uuid;

#[test]
fn test_create_table_with_vault_field() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT)");
    ctx.commit();
}

#[test]
fn test_create_table_with_vault_primary_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE vaults (account_vault VAULT PRIMARY KEY, name TEXT)");
    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_string_into_vault_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (account_vault VAULT)");

    // Strings cannot be inserted into Vault fields
    // This should fail because Vault can't be created from strings
    ctx.exec("INSERT INTO accounts VALUES ('550e8400-e29b-41d4-a716-446655440000')");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_vault_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (account_vault VAULT)");

    // Numbers cannot be inserted into Vault fields
    ctx.exec("INSERT INTO accounts VALUES (123)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_string_to_vault_should_error() {
    let mut ctx = setup_test();

    // CAST from string to Vault should fail
    // Vault can only come from application or parameters
    ctx.exec("SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS VAULT)");
}

#[test]
fn test_vault_display_is_redacted() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let vault = Vault::new(uuid);

    // Display should be redacted
    let display_str = format!("{}", vault);
    assert_eq!(display_str, "[REDACTED VAULT]");

    // Should not contain any part of the UUID
    assert!(!display_str.contains("550e"));
    assert!(!display_str.contains("8400"));
}

#[test]
fn test_vault_debug_is_redacted() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let vault = Vault::new(uuid);

    // Debug should be redacted
    let debug_str = format!("{:?}", vault);
    assert_eq!(debug_str, "Vault([REDACTED])");

    // Should not contain any part of the UUID
    assert!(!debug_str.contains("550e"));
    assert!(!debug_str.contains("8400"));
}

#[test]
fn test_vault_equality() {
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();

    let vault1a = Vault::new(uuid1);
    let vault1b = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Same UUID should be equal
    assert_eq!(vault1a, vault1b);

    // Different UUIDs should not be equal
    assert_ne!(vault1a, vault2);
}

#[test]
fn test_vault_ordering() {
    let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Should be orderable for database operations
    assert!(vault1 < vault2);
    assert!(vault2 > vault1);
}

#[test]
fn test_vault_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT)");

    // NULL should be allowed
    ctx.exec("INSERT INTO accounts VALUES (1, NULL)");
    ctx.exec("INSERT INTO accounts VALUES (2, NULL)");

    let results = ctx.query("SELECT id FROM accounts WHERE account_vault IS NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_vault_value_is_redacted_in_query_results() {
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let vault = Vault::new(uuid);

    // When converted to string (as would happen in query results),
    // it should show redacted text
    let value = Value::Vault(vault);
    let display_str = format!("{}", value);

    // Should be redacted
    assert_eq!(display_str, "[REDACTED VAULT]");

    // Should not leak the UUID
    assert!(!display_str.contains("550e"));
    assert!(!display_str.contains("8400"));
}

// Tests for parameterized inserts - the proper way to insert Vault values

#[test]
fn test_insert_vault_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT, name TEXT)");

    // Create vault values
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Insert using parameters (the proper way)
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::Vault(vault1.clone()),
            Value::Str("alice".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::Vault(vault2.clone()),
            Value::Str("bob".to_string()),
        ],
    );

    // Query all accounts
    let results = ctx.query("SELECT id, name FROM accounts ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("alice".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("bob".to_string())
    );

    ctx.commit();
}

#[test]
fn test_query_vault_with_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT, name TEXT)");

    // Create and insert vaults
    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::Vault(vault1.clone()),
            Value::Str("alice".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::Vault(vault2.clone()),
            Value::Str("bob".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(3),
            Value::Vault(vault1.clone()),
            Value::Str("alice_secondary".to_string()),
        ],
    );

    // First, check all inserted accounts
    let all_results = ctx.query("SELECT id, account_vault, name FROM accounts ORDER BY id");
    println!("All accounts: {:?}", all_results);
    assert_eq!(all_results.len(), 3, "Should have 3 accounts");

    // Query by specific vault using parameters
    let results = ctx.query_with_params(
        "SELECT id, name FROM accounts WHERE account_vault = ? ORDER BY id",
        vec![Value::Vault(vault1.clone())],
    );

    println!("Filtered results: {:?}", results);
    println!("Searching for vault: {:?}", vault1);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("alice".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("alice_secondary".to_string())
    );

    ctx.commit();
}

#[test]
fn test_vault_equality_in_queries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT)");

    // Create two identical vault values
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let vault1 = Vault::new(uuid);
    let vault2 = Vault::new(uuid); // Same UUID

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(1), Value::Vault(vault1)],
    );

    // Query with the "different" vault object (but same UUID)
    let results = ctx.query_with_params(
        "SELECT id FROM accounts WHERE account_vault = ?",
        vec![Value::Vault(vault2)],
    );

    // Should find the account because UUIDs match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_vault_parameter_simple_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE test_table (id INT, account_vault VAULT)");

    // Create vault
    let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let vault = Vault::new(uuid);

    // Insert via parameter
    ctx.exec_with_params(
        "INSERT INTO test_table VALUES (?, ?)",
        vec![Value::I32(1), Value::Vault(vault.clone())],
    );

    // Check it was inserted
    let all = ctx.query("SELECT * FROM test_table");
    println!("Inserted row: {:?}", all);
    assert_eq!(all.len(), 1);

    // Get the actual vault value from the database
    let stored_vault = all[0].get("account_vault").unwrap();
    println!("Stored vault: {:?}", stored_vault);
    println!("Original vault: {:?}", Value::Vault(vault.clone()));
    println!(
        "Are they equal? {}",
        stored_vault == &Value::Vault(vault.clone())
    );

    // Try to query with the exact same parameter
    let results = ctx.query_with_params(
        "SELECT * FROM test_table WHERE account_vault = ?",
        vec![Value::Vault(vault.clone())],
    );
    println!("Query results: {:?}", results);
    assert_eq!(results.len(), 1, "Should find the row with matching vault");

    ctx.commit();
}

#[test]
fn test_vault_in_joins() {
    let mut ctx = setup_test();

    // Create two tables that reference the same vault
    ctx.exec("CREATE TABLE accounts (account_vault VAULT PRIMARY KEY, name TEXT)");
    ctx.exec("CREATE TABLE balances (account_vault VAULT, amount INT)");

    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Insert accounts
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![
            Value::Vault(vault1.clone()),
            Value::Str("alice".to_string()),
        ],
    );
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::Vault(vault2.clone()), Value::Str("bob".to_string())],
    );

    // Insert balances
    ctx.exec_with_params(
        "INSERT INTO balances VALUES (?, ?)",
        vec![Value::Vault(vault1.clone()), Value::I32(1000)],
    );
    ctx.exec_with_params(
        "INSERT INTO balances VALUES (?, ?)",
        vec![Value::Vault(vault2.clone()), Value::I32(500)],
    );

    // Join on vault column
    let results = ctx.query(
        "SELECT a.name, b.amount
         FROM accounts a
         JOIN balances b ON a.account_vault = b.account_vault
         ORDER BY b.amount DESC",
    );

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("alice".to_string())
    );
    assert_eq!(results[0].get("amount").unwrap(), &Value::I32(1000));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("bob".to_string())
    );
    assert_eq!(results[1].get("amount").unwrap(), &Value::I32(500));

    ctx.commit();
}

#[test]
fn test_vault_in_group_by() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE transactions (account_vault VAULT, amount INT)");

    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Insert multiple transactions per vault
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::Vault(vault1.clone()), Value::I32(100)],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::Vault(vault1.clone()), Value::I32(200)],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::Vault(vault2.clone()), Value::I32(50)],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::Vault(vault2.clone()), Value::I32(75)],
    );

    // Group by vault and sum amounts
    let results = ctx.query(
        "SELECT account_vault, SUM(amount) as total FROM transactions GROUP BY account_vault",
    );

    assert_eq!(results.len(), 2);

    // Check totals (order may vary)
    let totals: Vec<i32> = results
        .iter()
        .map(|r| match r.get("total").unwrap() {
            Value::I32(v) => *v,
            _ => panic!("Expected I32"),
        })
        .collect();

    assert!(totals.contains(&300)); // vault1: 100 + 200
    assert!(totals.contains(&125)); // vault2: 50 + 75

    ctx.commit();
}

#[test]
fn test_vault_distinct() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE transactions (id INT, account_vault VAULT)");

    let uuid1 = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap();
    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);

    // Insert multiple transactions with duplicate vaults
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(1), Value::Vault(vault1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(2), Value::Vault(vault1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(3), Value::Vault(vault2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(4), Value::Vault(vault2.clone())],
    );

    // Get distinct vaults
    let results = ctx.query("SELECT DISTINCT account_vault FROM transactions");

    assert_eq!(results.len(), 2, "Should have 2 distinct vaults");

    ctx.commit();
}

#[test]
fn test_vault_ordering_in_queries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, account_vault VAULT)");

    let uuid1 = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let uuid2 = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
    let uuid3 = Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap();

    let vault1 = Vault::new(uuid1);
    let vault2 = Vault::new(uuid2);
    let vault3 = Vault::new(uuid3);

    // Insert in random order
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(2), Value::Vault(vault2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(1), Value::Vault(vault1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(3), Value::Vault(vault3.clone())],
    );

    // Order by vault
    let results = ctx.query("SELECT id FROM accounts ORDER BY account_vault");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}
