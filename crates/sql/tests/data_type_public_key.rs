//! Public key data type tests
//!
//! PublicKey represents cryptographic public keys and should NOT be creatable from
//! string literals or casts. It can only come from:
//! 1. Parameters/bindings from application layer (the secure way)
//! 2. Database reads (existing PublicKey values)
//!
//! Unlike PrivateKey, PublicKey values are NOT redacted in display/debug since they
//! are meant to be shared. However, they still cannot be created from literals to
//! maintain type safety and prevent confusion.

mod common;

use common::setup_test;
use proven_value::{PublicKey, Value};

#[test]
fn test_create_table_with_public_key_field() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, owner_pubkey PUBLIC KEY)");
    ctx.commit();
}

#[test]
fn test_create_table_with_public_key_primary_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE users (pubkey PUBLIC KEY PRIMARY KEY, username TEXT)");
    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_string_into_public_key_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (owner_pubkey PUBLIC KEY)");

    // Strings cannot be inserted into PublicKey fields
    // This should fail because PublicKey can't be created from strings
    ctx.exec("INSERT INTO accounts VALUES ('0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_public_key_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (owner_pubkey PUBLIC KEY)");

    // Numbers cannot be inserted into PublicKey fields
    ctx.exec("INSERT INTO accounts VALUES (123)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_string_to_public_key_should_error() {
    let mut ctx = setup_test();

    // CAST from string to PublicKey should fail
    // PublicKey can only come from secure parameters
    ctx.exec("SELECT CAST('0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' AS PUBLIC KEY)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_bytea_to_public_key_should_error() {
    let mut ctx = setup_test();

    // CAST from bytea to PublicKey should also fail
    // Only parameter binding is allowed
    ctx.exec("SELECT CAST(X'0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' AS PUBLIC KEY)");
}

#[test]
fn test_public_key_display_shows_value() {
    let key_bytes = [42u8; 32];
    let public_key = PublicKey::ed25519(key_bytes);

    // Display should show the actual key (public keys are meant to be shared)
    let display_str = format!("{}", public_key);

    // Public keys should be visible (not redacted like private keys)
    assert!(!display_str.is_empty());
    // Should contain some representation of the key
    assert!(!display_str.contains("[REDACTED]"));
}

#[test]
fn test_public_key_debug_shows_type() {
    let key_bytes = [99u8; 33];
    let public_key = PublicKey::secp256k1_compressed(key_bytes);

    // Debug should show type information
    let debug_str = format!("{:?}", public_key);

    // Should indicate it's a public key
    assert!(debug_str.contains("PublicKey") || debug_str.contains("Secp256k1"));
    // Public keys should show their data (unlike private keys)
    assert!(!debug_str.contains("[REDACTED]"));
}

#[test]
fn test_public_key_equality() {
    let key_bytes1 = [1u8; 32];
    let key_bytes2 = [2u8; 32];

    let key1a = PublicKey::ed25519(key_bytes1);
    let key1b = PublicKey::ed25519(key_bytes1);
    let key2 = PublicKey::ed25519(key_bytes2);

    // Same bytes should be equal
    assert_eq!(key1a, key1b);

    // Different bytes should not be equal
    assert_ne!(key1a, key2);
}

#[test]
fn test_public_key_different_types_not_equal() {
    let key_bytes = [42u8; 32];

    let ed25519_key = PublicKey::ed25519(key_bytes);
    let secp256k1_key = PublicKey::secp256k1_compressed([42u8; 33]);

    // Different key types should not be equal even with same bytes
    assert_ne!(ed25519_key, secp256k1_key);
}

#[test]
fn test_public_key_ordering() {
    let bytes1 = [1u8; 32];
    let bytes2 = [2u8; 32];

    let ed_key1 = PublicKey::ed25519(bytes1);
    let ed_key2 = PublicKey::ed25519(bytes2);
    let secp_key = PublicKey::secp256k1_compressed([1u8; 33]);

    // Should be orderable for database operations
    assert!(ed_key1 < ed_key2);
    assert!(ed_key2 > ed_key1);

    // Ed25519 sorts before Secp256k1
    assert!(ed_key1 < secp_key);
}

#[test]
fn test_public_key_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, owner_pubkey PUBLIC KEY)");

    // NULL should be allowed
    ctx.exec("INSERT INTO accounts VALUES (1, NULL)");
    ctx.exec("INSERT INTO accounts VALUES (2, NULL)");

    let results = ctx.query("SELECT id FROM accounts WHERE owner_pubkey IS NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_public_key_value_in_query_results() {
    let key_bytes = [123u8; 32];
    let public_key = PublicKey::ed25519(key_bytes);

    // When converted to string (as would happen in query results),
    // public keys should display their actual value (not redacted)
    let value = Value::PublicKey(public_key);
    let display_str = format!("{}", value);

    // Should NOT be redacted (public keys are meant to be shared)
    assert!(!display_str.contains("[REDACTED]"));
}

// Tests for parameterized inserts - the ONLY proper way to insert PublicKey values

#[test]
fn test_insert_ed25519_public_key_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, owner_pubkey PUBLIC KEY, label TEXT)");

    // Create Ed25519 public keys
    let key1 = PublicKey::ed25519([1u8; 32]);
    let key2 = PublicKey::ed25519([2u8; 32]);

    // Insert using parameters (the proper and only way)
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PublicKey(key1.clone()),
            Value::Str("alice-pubkey".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PublicKey(key2.clone()),
            Value::Str("bob-pubkey".to_string()),
        ],
    );

    // Query all accounts
    let results = ctx.query("SELECT id, label FROM accounts ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("label").unwrap(),
        &Value::Str("alice-pubkey".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("label").unwrap(),
        &Value::Str("bob-pubkey".to_string())
    );

    ctx.commit();
}

#[test]
fn test_insert_secp256k1_public_key_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE blockchain_accounts (id INT, eth_pubkey PUBLIC KEY)");

    // Create Secp256k1 public keys (used in Ethereum, Bitcoin, etc.)
    let key1 = PublicKey::secp256k1_compressed([42u8; 33]);
    let key2 = PublicKey::secp256k1_compressed([99u8; 33]);

    // Insert using parameters
    ctx.exec_with_params(
        "INSERT INTO blockchain_accounts VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1)],
    );

    ctx.exec_with_params(
        "INSERT INTO blockchain_accounts VALUES (?, ?)",
        vec![Value::I32(2), Value::PublicKey(key2)],
    );

    // Query to verify insertion
    let results = ctx.query("SELECT id FROM blockchain_accounts ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_query_public_key_with_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, owner_pubkey PUBLIC KEY, balance INT)");

    // Create and insert public keys
    let key1 = PublicKey::ed25519([10u8; 32]);
    let key2 = PublicKey::ed25519([20u8; 32]);

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PublicKey(key1.clone()),
            Value::I32(1000),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PublicKey(key2.clone()),
            Value::I32(2000),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?, ?)",
        vec![
            Value::I32(3),
            Value::PublicKey(key1.clone()),
            Value::I32(1500),
        ],
    );

    // Query by specific public key using parameters
    let results = ctx.query_with_params(
        "SELECT id, balance FROM accounts WHERE owner_pubkey = ? ORDER BY id",
        vec![Value::PublicKey(key1.clone())],
    );

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("balance").unwrap(), &Value::I32(1000));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[1].get("balance").unwrap(), &Value::I32(1500));

    ctx.commit();
}

#[test]
fn test_public_key_equality_in_queries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_registry (id INT, pubkey PUBLIC KEY)");

    // Create two identical public key values (same bytes, same type)
    let key_bytes = [77u8; 32];
    let key1 = PublicKey::ed25519(key_bytes);
    let key2 = PublicKey::ed25519(key_bytes); // Same bytes, same type

    ctx.exec_with_params(
        "INSERT INTO key_registry VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1)],
    );

    // Query with the "different" key object (but same bytes and type)
    let results = ctx.query_with_params(
        "SELECT id FROM key_registry WHERE pubkey = ?",
        vec![Value::PublicKey(key2)],
    );

    // Should find the key because bytes and type match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_update_public_key_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT PRIMARY KEY, owner_pubkey PUBLIC KEY)");

    let key1 = PublicKey::ed25519([1u8; 32]);
    let key2 = PublicKey::ed25519([2u8; 32]);

    // Insert initial key
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1.clone())],
    );

    // Update the public key value
    ctx.exec_with_params(
        "UPDATE accounts SET owner_pubkey = ? WHERE id = ?",
        vec![Value::PublicKey(key2.clone()), Value::I32(1)],
    );

    // Verify the key was updated by querying with the new key
    let results = ctx.query_with_params(
        "SELECT id FROM accounts WHERE owner_pubkey = ?",
        vec![Value::PublicKey(key2)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    // Old key should not match
    let results = ctx.query_with_params(
        "SELECT id FROM accounts WHERE owner_pubkey = ?",
        vec![Value::PublicKey(key1)],
    );
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_delete_public_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE accounts (id INT, owner_pubkey PUBLIC KEY)");

    let key1 = PublicKey::ed25519([10u8; 32]);
    let key2 = PublicKey::secp256k1_compressed([20u8; 33]);

    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(2), Value::PublicKey(key2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO accounts VALUES (?, ?)",
        vec![Value::I32(3), Value::PublicKey(key1.clone())],
    );

    // Delete all accounts with key1
    ctx.exec_with_params(
        "DELETE FROM accounts WHERE owner_pubkey = ?",
        vec![Value::PublicKey(key1)],
    );

    // Should only have the key2 account left
    let results = ctx.query("SELECT id FROM accounts");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_public_key_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_registry (id INT, pubkey PUBLIC KEY)");

    // Insert keys that can be compared (all Ed25519 for consistent ordering)
    let key1 = PublicKey::ed25519([1u8; 32]);
    let key2 = PublicKey::ed25519([2u8; 32]);
    let key3 = PublicKey::ed25519([3u8; 32]);

    ctx.exec_with_params(
        "INSERT INTO key_registry VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO key_registry VALUES (?, ?)",
        vec![Value::I32(2), Value::PublicKey(key2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO key_registry VALUES (?, ?)",
        vec![Value::I32(3), Value::PublicKey(key3.clone())],
    );

    // Test equality
    let results = ctx.query_with_params(
        "SELECT id FROM key_registry WHERE pubkey = ?",
        vec![Value::PublicKey(key2.clone())],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // Test greater than
    let results = ctx.query_with_params(
        "SELECT id FROM key_registry WHERE pubkey > ? ORDER BY id",
        vec![Value::PublicKey(key1)],
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    // Test less than
    let results = ctx.query_with_params(
        "SELECT id FROM key_registry WHERE pubkey < ? ORDER BY id",
        vec![Value::PublicKey(key3)],
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_group_by_public_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_usage (pubkey PUBLIC KEY, usage_count INT)");

    let key1 = PublicKey::ed25519([10u8; 32]);
    let key2 = PublicKey::ed25519([20u8; 32]);

    // Insert multiple usages
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PublicKey(key1.clone()), Value::I32(1)],
    );
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PublicKey(key2.clone()), Value::I32(1)],
    );
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PublicKey(key1.clone()), Value::I32(1)],
    );

    let results = ctx.query("SELECT COUNT(*) as cnt FROM key_usage GROUP BY pubkey ORDER BY 1");
    assert_eq!(results.len(), 2);

    // Check the counts
    let first_count = results[0].get("cnt").unwrap();
    let second_count = results[1].get("cnt").unwrap();

    // One key used once, one key used twice
    assert!(
        (first_count == &Value::I64(1) && second_count == &Value::I64(2))
            || (first_count == &Value::I64(2) && second_count == &Value::I64(1)),
        "Expected one group with count 1 and one with count 2, got {} and {}",
        first_count,
        second_count
    );

    ctx.commit();
}

#[test]
fn test_mixed_public_key_types() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE multi_chain_account (id INT, account_pubkey PUBLIC KEY, chain TEXT)");

    let ed25519_key = PublicKey::ed25519([1u8; 32]);
    let secp256k1_key = PublicKey::secp256k1_compressed([2u8; 33]);

    // Insert both key types
    ctx.exec_with_params(
        "INSERT INTO multi_chain_account VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PublicKey(ed25519_key.clone()),
            Value::Str("solana".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO multi_chain_account VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PublicKey(secp256k1_key.clone()),
            Value::Str("ethereum".to_string()),
        ],
    );

    // Query each type
    let results = ctx.query_with_params(
        "SELECT id, chain FROM multi_chain_account WHERE account_pubkey = ?",
        vec![Value::PublicKey(ed25519_key)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("chain").unwrap(),
        &Value::Str("solana".to_string())
    );

    let results = ctx.query_with_params(
        "SELECT id, chain FROM multi_chain_account WHERE account_pubkey = ?",
        vec![Value::PublicKey(secp256k1_key)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[0].get("chain").unwrap(),
        &Value::Str("ethereum".to_string())
    );

    ctx.commit();
}

#[test]
fn test_public_key_ordering_by_type() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE keys (id INT, pubkey PUBLIC KEY)");

    // Ed25519 keys should sort before Secp256k1 keys
    let ed_key = PublicKey::ed25519([255u8; 32]); // High byte value
    let secp_key = PublicKey::secp256k1_compressed([0u8; 33]); // Low byte value

    ctx.exec_with_params(
        "INSERT INTO keys VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(secp_key)],
    );
    ctx.exec_with_params(
        "INSERT INTO keys VALUES (?, ?)",
        vec![Value::I32(2), Value::PublicKey(ed_key)],
    );

    // When ordered by pubkey, Ed25519 should come first despite higher byte values
    let results = ctx.query("SELECT id FROM keys ORDER BY pubkey");
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("id").unwrap(),
        &Value::I32(2),
        "Ed25519 key should sort first"
    );
    assert_eq!(
        results[1].get("id").unwrap(),
        &Value::I32(1),
        "Secp256k1 key should sort second"
    );

    ctx.commit();
}

#[test]
fn test_public_key_as_foreign_reference() {
    let mut ctx = setup_test();

    // Use case: store public keys and reference them from other tables
    ctx.exec("CREATE TABLE users (pubkey PUBLIC KEY PRIMARY KEY, username TEXT)");
    ctx.exec("CREATE TABLE messages (id INT, sender_pubkey PUBLIC KEY, message TEXT)");

    let alice_key = PublicKey::ed25519([1u8; 32]);
    let bob_key = PublicKey::ed25519([2u8; 32]);

    // Insert users
    ctx.exec_with_params(
        "INSERT INTO users VALUES (?, ?)",
        vec![
            Value::PublicKey(alice_key.clone()),
            Value::Str("alice".to_string()),
        ],
    );
    ctx.exec_with_params(
        "INSERT INTO users VALUES (?, ?)",
        vec![
            Value::PublicKey(bob_key.clone()),
            Value::Str("bob".to_string()),
        ],
    );

    // Insert messages
    ctx.exec_with_params(
        "INSERT INTO messages VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PublicKey(alice_key.clone()),
            Value::Str("Hello from Alice".to_string()),
        ],
    );
    ctx.exec_with_params(
        "INSERT INTO messages VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PublicKey(bob_key.clone()),
            Value::Str("Hello from Bob".to_string()),
        ],
    );

    // Query messages from a specific user
    let results = ctx.query_with_params(
        "SELECT message FROM messages WHERE sender_pubkey = ?",
        vec![Value::PublicKey(alice_key)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("message").unwrap(),
        &Value::Str("Hello from Alice".to_string())
    );

    ctx.commit();
}

#[test]
fn test_public_key_distinct() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE transactions (id INT, signer PUBLIC KEY)");

    let key1 = PublicKey::ed25519([1u8; 32]);
    let key2 = PublicKey::ed25519([2u8; 32]);

    // Insert multiple transactions with duplicate signers
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(1), Value::PublicKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(2), Value::PublicKey(key2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(3), Value::PublicKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO transactions VALUES (?, ?)",
        vec![Value::I32(4), Value::PublicKey(key2.clone())],
    );

    // Get distinct signers
    let results = ctx.query("SELECT DISTINCT signer FROM transactions ORDER BY signer");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

// NOTE: Future tests to consider:
// - test_public_key_in_index() - Test creating indexes on public key columns
// - test_public_key_join() - Test joining tables on public key fields
// - test_public_key_aggregates() - Test aggregate functions with public keys
