//! Private key data type tests
//!
//! PrivateKey represents cryptographic private keys and should NOT be creatable from
//! string literals or casts. It can only come from:
//! 1. Parameters/bindings from application layer (the secure way)
//! 2. Database reads (existing PrivateKey values)
//!
//! This ensures keys cannot be accidentally hardcoded or logged.

use crate::common::setup_test;
use proven_value::{PrivateKey, Value};

#[test]
fn test_create_table_with_private_key_field() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT, signing_key PRIVATE KEY)");
    ctx.commit();
}

#[test]
fn test_create_table_with_private_key_primary_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_store (key_id PRIVATE KEY PRIMARY KEY, label TEXT)");
    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_string_into_private_key_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (signing_key PRIVATE KEY)");

    // Strings cannot be inserted into PrivateKey fields
    // This should fail because PrivateKey can't be created from strings
    ctx.exec("INSERT INTO wallets VALUES ('0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef')");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_private_key_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (signing_key PRIVATE KEY)");

    // Numbers cannot be inserted into PrivateKey fields
    ctx.exec("INSERT INTO wallets VALUES (123)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_string_to_private_key_should_error() {
    let mut ctx = setup_test();

    // CAST from string to PrivateKey should fail
    // PrivateKey can only come from secure parameters
    ctx.exec("SELECT CAST('0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' AS PRIVATE KEY)");
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_cast_bytea_to_private_key_should_error() {
    let mut ctx = setup_test();

    // CAST from bytea to PrivateKey should also fail
    // Only parameter binding is allowed
    ctx.exec("SELECT CAST(X'0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef' AS PRIVATE KEY)");
}

#[test]
fn test_private_key_display_is_redacted() {
    let key_bytes = [42u8; 32];
    let private_key = PrivateKey::ed25519(key_bytes);

    // Display should be redacted
    let display_str = format!("{}", private_key);
    assert_eq!(display_str, "[REDACTED ed25519 private key]");

    // Should not contain any part of the key bytes
    assert!(!display_str.contains("42"));
    assert!(!display_str.to_lowercase().contains("2a")); // hex representation
}

#[test]
fn test_private_key_debug_is_redacted() {
    let key_bytes = [99u8; 32];
    let private_key = PrivateKey::secp256k1(key_bytes);

    // Debug should be redacted
    let debug_str = format!("{:?}", private_key);
    assert_eq!(debug_str, "PrivateKey::Secp256k1([REDACTED])");

    // Should not contain any part of the key bytes
    assert!(!debug_str.contains("99"));
    assert!(!debug_str.to_lowercase().contains("63")); // hex representation
}

#[test]
fn test_private_key_equality() {
    let key_bytes1 = [1u8; 32];
    let key_bytes2 = [2u8; 32];

    let key1a = PrivateKey::ed25519(key_bytes1);
    let key1b = PrivateKey::ed25519(key_bytes1);
    let key2 = PrivateKey::ed25519(key_bytes2);

    // Same bytes should be equal
    assert_eq!(key1a, key1b);

    // Different bytes should not be equal
    assert_ne!(key1a, key2);
}

#[test]
fn test_private_key_different_types_not_equal() {
    let key_bytes = [42u8; 32];

    let ed25519_key = PrivateKey::ed25519(key_bytes);
    let secp256k1_key = PrivateKey::secp256k1(key_bytes);

    // Different key types should not be equal even with same bytes
    assert_ne!(ed25519_key, secp256k1_key);
}

#[test]
fn test_private_key_ordering() {
    let bytes1 = [1u8; 32];
    let bytes2 = [2u8; 32];

    let ed_key1 = PrivateKey::ed25519(bytes1);
    let ed_key2 = PrivateKey::ed25519(bytes2);
    let secp_key = PrivateKey::secp256k1(bytes1);

    // Should be orderable for database operations
    assert!(ed_key1 < ed_key2);
    assert!(ed_key2 > ed_key1);

    // Ed25519 sorts before Secp256k1
    assert!(ed_key1 < secp_key);
}

#[test]
fn test_private_key_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT, signing_key PRIVATE KEY)");

    // NULL should be allowed
    ctx.exec("INSERT INTO wallets VALUES (1, NULL)");
    ctx.exec("INSERT INTO wallets VALUES (2, NULL)");

    let results = ctx.query("SELECT id FROM wallets WHERE signing_key IS NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_private_key_value_is_redacted_in_query_results() {
    let key_bytes = [123u8; 32];
    let private_key = PrivateKey::ed25519(key_bytes);

    // When converted to string (as would happen in query results),
    // it should show redacted text
    let value = Value::PrivateKey(private_key);
    let display_str = format!("{}", value);

    // Should be redacted
    assert_eq!(display_str, "[REDACTED ed25519 private key]");

    // Should not leak the key bytes
    assert!(!display_str.contains("123"));
    assert!(!display_str.to_lowercase().contains("7b")); // hex representation
}

// Tests for parameterized inserts - the ONLY proper way to insert PrivateKey values

#[test]
fn test_insert_ed25519_private_key_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT, signing_key PRIVATE KEY, label TEXT)");

    // Create Ed25519 private keys
    let key1 = PrivateKey::ed25519([1u8; 32]);
    let key2 = PrivateKey::ed25519([2u8; 32]);

    // Insert using parameters (the proper and only way)
    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PrivateKey(key1.clone()),
            Value::Str("alice-key".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PrivateKey(key2.clone()),
            Value::Str("bob-key".to_string()),
        ],
    );

    // Query all wallets (keys should be redacted in output)
    let results = ctx.query("SELECT id, label FROM wallets ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("label").unwrap(),
        &Value::Str("alice-key".to_string())
    );
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(
        results[1].get("label").unwrap(),
        &Value::Str("bob-key".to_string())
    );

    ctx.commit();
}

#[test]
fn test_insert_secp256k1_private_key_via_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE crypto_wallets (id INT, eth_key PRIVATE KEY)");

    // Create Secp256k1 private keys (used in Ethereum, Bitcoin, etc.)
    let key1 = PrivateKey::secp256k1([42u8; 32]);
    let key2 = PrivateKey::secp256k1([99u8; 32]);

    // Insert using parameters
    ctx.exec_with_params(
        "INSERT INTO crypto_wallets VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(key1)],
    );

    ctx.exec_with_params(
        "INSERT INTO crypto_wallets VALUES (?, ?)",
        vec![Value::I32(2), Value::PrivateKey(key2)],
    );

    // Query to verify insertion
    let results = ctx.query("SELECT id FROM crypto_wallets ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_query_private_key_with_parameters() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT, signing_key PRIVATE KEY, balance INT)");

    // Create and insert private keys
    let key1 = PrivateKey::ed25519([10u8; 32]);
    let key2 = PrivateKey::ed25519([20u8; 32]);

    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PrivateKey(key1.clone()),
            Value::I32(1000),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PrivateKey(key2.clone()),
            Value::I32(2000),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?, ?)",
        vec![
            Value::I32(3),
            Value::PrivateKey(key1.clone()),
            Value::I32(1500),
        ],
    );

    // Query by specific private key using parameters
    let results = ctx.query_with_params(
        "SELECT id, balance FROM wallets WHERE signing_key = ? ORDER BY id",
        vec![Value::PrivateKey(key1.clone())],
    );

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("balance").unwrap(), &Value::I32(1000));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[1].get("balance").unwrap(), &Value::I32(1500));

    ctx.commit();
}

#[test]
fn test_private_key_equality_in_queries() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_store (id INT, private_key PRIVATE KEY)");

    // Create two identical private key values (same bytes, same type)
    let key_bytes = [77u8; 32];
    let key1 = PrivateKey::ed25519(key_bytes);
    let key2 = PrivateKey::ed25519(key_bytes); // Same bytes, same type

    ctx.exec_with_params(
        "INSERT INTO key_store VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(key1)],
    );

    // Query with the "different" key object (but same bytes and type)
    let results = ctx.query_with_params(
        "SELECT id FROM key_store WHERE private_key = ?",
        vec![Value::PrivateKey(key2)],
    );

    // Should find the key because bytes and type match
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    ctx.commit();
}

#[test]
fn test_update_private_key_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT PRIMARY KEY, signing_key PRIVATE KEY)");

    let key1 = PrivateKey::ed25519([1u8; 32]);
    let key2 = PrivateKey::ed25519([2u8; 32]);

    // Insert initial key
    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(key1.clone())],
    );

    // Update the private key value
    ctx.exec_with_params(
        "UPDATE wallets SET signing_key = ? WHERE id = ?",
        vec![Value::PrivateKey(key2.clone()), Value::I32(1)],
    );

    // Verify the key was updated by querying with the new key
    let results = ctx.query_with_params(
        "SELECT id FROM wallets WHERE signing_key = ?",
        vec![Value::PrivateKey(key2)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    // Old key should not match
    let results = ctx.query_with_params(
        "SELECT id FROM wallets WHERE signing_key = ?",
        vec![Value::PrivateKey(key1)],
    );
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_delete_private_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE wallets (id INT, signing_key PRIVATE KEY)");

    let key1 = PrivateKey::ed25519([10u8; 32]);
    let key2 = PrivateKey::secp256k1([20u8; 32]);

    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?)",
        vec![Value::I32(2), Value::PrivateKey(key2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO wallets VALUES (?, ?)",
        vec![Value::I32(3), Value::PrivateKey(key1.clone())],
    );

    // Delete all wallets with key1
    ctx.exec_with_params(
        "DELETE FROM wallets WHERE signing_key = ?",
        vec![Value::PrivateKey(key1)],
    );

    // Should only have the key2 wallet left
    let results = ctx.query("SELECT id FROM wallets");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_private_key_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_store (id INT, private_key PRIVATE KEY)");

    // Insert keys that can be compared (all Ed25519 for consistent ordering)
    let key1 = PrivateKey::ed25519([1u8; 32]);
    let key2 = PrivateKey::ed25519([2u8; 32]);
    let key3 = PrivateKey::ed25519([3u8; 32]);

    ctx.exec_with_params(
        "INSERT INTO key_store VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(key1.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO key_store VALUES (?, ?)",
        vec![Value::I32(2), Value::PrivateKey(key2.clone())],
    );
    ctx.exec_with_params(
        "INSERT INTO key_store VALUES (?, ?)",
        vec![Value::I32(3), Value::PrivateKey(key3.clone())],
    );

    // Test equality
    let results = ctx.query_with_params(
        "SELECT id FROM key_store WHERE private_key = ?",
        vec![Value::PrivateKey(key2.clone())],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // Test greater than
    let results = ctx.query_with_params(
        "SELECT id FROM key_store WHERE private_key > ? ORDER BY id",
        vec![Value::PrivateKey(key1)],
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    // Test less than
    let results = ctx.query_with_params(
        "SELECT id FROM key_store WHERE private_key < ? ORDER BY id",
        vec![Value::PrivateKey(key3)],
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_group_by_private_key() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE key_usage (private_key PRIVATE KEY, usage_count INT)");

    let key1 = PrivateKey::ed25519([10u8; 32]);
    let key2 = PrivateKey::ed25519([20u8; 32]);

    // Insert multiple usages
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PrivateKey(key1.clone()), Value::I32(1)],
    );
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PrivateKey(key2.clone()), Value::I32(1)],
    );
    ctx.exec_with_params(
        "INSERT INTO key_usage VALUES (?, ?)",
        vec![Value::PrivateKey(key1.clone()), Value::I32(1)],
    );

    let results =
        ctx.query("SELECT COUNT(*) as cnt FROM key_usage GROUP BY private_key ORDER BY 1");
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
fn test_mixed_private_key_types() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE multi_chain_wallet (id INT, wallet_key PRIVATE KEY, chain TEXT)");

    let ed25519_key = PrivateKey::ed25519([1u8; 32]);
    let secp256k1_key = PrivateKey::secp256k1([2u8; 32]);

    // Insert both key types
    ctx.exec_with_params(
        "INSERT INTO multi_chain_wallet VALUES (?, ?, ?)",
        vec![
            Value::I32(1),
            Value::PrivateKey(ed25519_key.clone()),
            Value::Str("solana".to_string()),
        ],
    );

    ctx.exec_with_params(
        "INSERT INTO multi_chain_wallet VALUES (?, ?, ?)",
        vec![
            Value::I32(2),
            Value::PrivateKey(secp256k1_key.clone()),
            Value::Str("ethereum".to_string()),
        ],
    );

    // Query each type
    let results = ctx.query_with_params(
        "SELECT id, chain FROM multi_chain_wallet WHERE wallet_key = ?",
        vec![Value::PrivateKey(ed25519_key)],
    );
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(
        results[0].get("chain").unwrap(),
        &Value::Str("solana".to_string())
    );

    let results = ctx.query_with_params(
        "SELECT id, chain FROM multi_chain_wallet WHERE wallet_key = ?",
        vec![Value::PrivateKey(secp256k1_key)],
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
fn test_private_key_ordering_by_type() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE keys (id INT, private_key PRIVATE KEY)");

    // Ed25519 keys should sort before Secp256k1 keys
    let ed_key = PrivateKey::ed25519([255u8; 32]); // High byte value
    let secp_key = PrivateKey::secp256k1([0u8; 32]); // Low byte value

    ctx.exec_with_params(
        "INSERT INTO keys VALUES (?, ?)",
        vec![Value::I32(1), Value::PrivateKey(secp_key)],
    );
    ctx.exec_with_params(
        "INSERT INTO keys VALUES (?, ?)",
        vec![Value::I32(2), Value::PrivateKey(ed_key)],
    );

    // When ordered by private_key, Ed25519 should come first despite higher byte values
    let results = ctx.query("SELECT id FROM keys ORDER BY private_key");
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

// NOTE: Future security tests to consider:
// - test_private_key_not_in_error_messages() - Ensure keys don't leak in error messages
// - test_private_key_not_in_logs() - Verify logging doesn't expose keys
// - test_private_key_secure_delete() - Test that deleted keys are properly cleared
