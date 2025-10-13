//! BYTEA data type tests
//! Based on gluesql/test-suite/src/data_type/bytea.rs

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_bytea_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BinaryData (id INT, data BYTEA)");
    ctx.commit();
}

#[test]
fn test_insert_bytea_hex_format() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Bytea (bytes BYTEA)");

    // Insert hex literals using X'...' format
    ctx.exec("INSERT INTO Bytea VALUES (X'123456')");
    ctx.exec("INSERT INTO Bytea VALUES (X'ab0123')");
    ctx.exec("INSERT INTO Bytea VALUES (X'936DA0')");

    let results = ctx.query("SELECT bytes FROM Bytea");
    assert_eq!(results.len(), 3);

    // Check that the values are properly stored as Bytea
    assert!(results[0].get("bytes").unwrap().is_bytes());
    assert!(results[1].get("bytes").unwrap().is_bytes());
    assert!(results[2].get("bytes").unwrap().is_bytes());

    ctx.commit();
}

#[test]
fn test_insert_uppercase_hex() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Bytea (bytes BYTEA)");

    // Uppercase hex should work
    ctx.exec("INSERT INTO Bytea VALUES (X'DEADBEEF')");
    ctx.exec("INSERT INTO Bytea VALUES (X'CAFEBABE')");

    let results = ctx.query("SELECT bytes FROM Bytea");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("bytes").unwrap().is_bytes());
    assert!(results[1].get("bytes").unwrap().is_bytes());

    ctx.commit();
}

#[test]
#[should_panic(expected = "TypeMismatch")]
fn test_insert_number_into_bytea_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Bytea (bytes BYTEA)");
    // Numbers cannot be inserted into BYTEA fields
    ctx.exec("INSERT INTO Bytea VALUES (0)");
}

#[test]
#[should_panic(expected = "ParseError")]
fn test_insert_invalid_hex_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Bytea (bytes BYTEA)");
    // Hex strings must have even number of digits (2 per byte)
    // For now, this produces a parse error
    ctx.exec("INSERT INTO Bytea VALUES (X'123')");
}

#[test]
fn test_select_bytea_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BinaryData (id INT, data BYTEA)");

    ctx.exec("INSERT INTO BinaryData VALUES (1, X'48656C6C6F')"); // "Hello" in hex
    ctx.exec("INSERT INTO BinaryData VALUES (2, X'576F726C64')"); // "World" in hex
    ctx.exec("INSERT INTO BinaryData VALUES (3, X'00FF00FF')"); // Binary data

    let results = ctx.query("SELECT id, data FROM BinaryData ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert!(results[0].get("data").unwrap().is_bytes());

    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert!(results[1].get("data").unwrap().is_bytes());

    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert!(results[2].get("data").unwrap().is_bytes());

    ctx.commit();
}

#[test]
fn test_bytea_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BinaryData (id INT, data BYTEA)");

    ctx.exec("INSERT INTO BinaryData VALUES (1, X'01')");
    ctx.exec("INSERT INTO BinaryData VALUES (2, X'02')");
    ctx.exec("INSERT INTO BinaryData VALUES (3, X'03')");
    ctx.exec("INSERT INTO BinaryData VALUES (4, X'02')"); // Duplicate value

    // Test equality
    let results = ctx.query("SELECT id FROM BinaryData WHERE data = X'02' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(4));

    // Test greater than
    let results = ctx.query("SELECT id FROM BinaryData WHERE data > X'01' ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(4));

    // Test less than
    let results = ctx.query("SELECT id FROM BinaryData WHERE data < X'03' ORDER BY id");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(4));

    ctx.commit();
}

#[test]
fn test_bytea_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BinaryData (id INT, data BYTEA)");

    ctx.exec("INSERT INTO BinaryData VALUES (1, X'ABCD')");
    ctx.exec("INSERT INTO BinaryData VALUES (2, NULL)");
    ctx.exec("INSERT INTO BinaryData VALUES (3, X'1234')");

    let results = ctx.query("SELECT id, data FROM BinaryData WHERE data IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert!(results[0].get("data").unwrap().is_bytes());
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    assert!(results[1].get("data").unwrap().is_bytes());

    let results = ctx.query("SELECT id FROM BinaryData WHERE data IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_bytea_update_and_delete() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE BinaryData (id INT PRIMARY KEY, data BYTEA)");

    ctx.exec("INSERT INTO BinaryData VALUES (1, X'0102')");
    ctx.exec("INSERT INTO BinaryData VALUES (2, X'0304')");
    ctx.exec("INSERT INTO BinaryData VALUES (3, X'0506')");

    // Update a BYTEA value
    ctx.exec("UPDATE BinaryData SET data = X'FFFF' WHERE id = 2");

    let results = ctx.query("SELECT data FROM BinaryData WHERE id = 2");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("data").unwrap().is_bytes());

    // Delete by BYTEA value
    ctx.exec("DELETE FROM BinaryData WHERE data = X'0506'");

    let results = ctx.query("SELECT COUNT(*) as cnt FROM BinaryData");
    assert_eq!(results[0].get("cnt").unwrap(), &Value::I64(2));

    ctx.commit();
}
