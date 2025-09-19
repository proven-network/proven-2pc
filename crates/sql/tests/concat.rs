//! String concatenation tests
//! Based on gluesql/test-suite/src/concat.rs

mod common;
use common::{TableBuilder, setup_test};

#[test]
fn test_basic_string_concatenation() {
    let mut ctx = setup_test();

    // Create and populate test table
    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, rate FLOAT, flag BOOLEAN, name TEXT, null_value TEXT NULL")
        .insert_values("(1, 2.3, TRUE, 'Foo', NULL)");

    // Test value || value concatenation
    let results = ctx.query("SELECT name || name AS value_value FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("value_value").unwrap().contains("FooFoo"));

    // Test value || literal concatenation
    let results = ctx.query("SELECT name || 'Bar' AS value_literal FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("value_literal").unwrap().contains("FooBar"));

    // Test literal || value concatenation
    let results = ctx.query("SELECT 'Bar' || name AS literal_value FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("literal_value").unwrap().contains("BarFoo"));

    // Test literal || literal concatenation
    let results = ctx.query("SELECT 'Foo' || 'Bar' AS literal_literal FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("literal_literal")
            .unwrap()
            .contains("FooBar")
    );

    ctx.commit();
}

#[test]
fn test_concatenation_with_null_values() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, rate FLOAT, flag BOOLEAN, name TEXT, null_value TEXT NULL")
        .insert_values("(1, 2.3, TRUE, 'Foo', NULL)");

    // Test concatenation with NULL values - all should return NULL
    let results = ctx.query("SELECT id || null_value AS id_n FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id_n").unwrap().contains("Null"));

    let results = ctx.query("SELECT rate || null_value AS rate_n FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("rate_n").unwrap().contains("Null"));

    let results = ctx.query("SELECT flag || null_value AS flag_n FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("flag_n").unwrap().contains("Null"));

    let results = ctx.query("SELECT name || null_value AS text_n FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("text_n").unwrap().contains("Null"));

    let results = ctx.query("SELECT null_value || id AS n_id FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("n_id").unwrap().contains("Null"));

    let results = ctx.query("SELECT null_value || name AS n_text FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("n_text").unwrap().contains("Null"));

    ctx.commit();
}

#[test]
fn test_concatenation_with_different_data_types() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, rate FLOAT, flag BOOLEAN, name TEXT, null_value TEXT NULL")
        .insert_values("(1, 2.3, TRUE, 'Foo', NULL)");

    // Test INTEGER || TEXT
    let results = ctx.query("SELECT id || name AS int_text FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("int_text").unwrap().contains("1Foo"));

    // Test FLOAT || TEXT
    let results = ctx.query("SELECT rate || name AS float_text FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("float_text").unwrap().contains("2.3Foo"));

    // Test BOOLEAN || TEXT
    let results = ctx.query("SELECT flag || name AS bool_text FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("bool_text").unwrap().contains("trueFoo"));

    // Test INTEGER || FLOAT
    let results = ctx.query("SELECT id || rate AS int_float FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("int_float").unwrap().contains("12.3"));

    // Test FLOAT || BOOLEAN
    let results = ctx.query("SELECT rate || flag AS float_bool FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("float_bool").unwrap().contains("2.3true"));

    // Test BOOLEAN || INTEGER
    let results = ctx.query("SELECT flag || id AS bool_int FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("bool_int").unwrap().contains("true1"));

    ctx.commit();
}

#[test]
fn test_multiple_concatenation() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, rate FLOAT, flag BOOLEAN, name TEXT, null_value TEXT NULL")
        .insert_values("(1, 2.3, TRUE, 'Foo', NULL)");

    // Test multiple concatenations
    let results = ctx.query("SELECT name || name || name AS triple FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("triple").unwrap().contains("FooFooFoo"));

    // Test mixed multiple concatenations
    let results = ctx.query("SELECT id || name || flag AS mixed FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("mixed").unwrap().contains("1Footrue"));

    // Test with literals
    let results = ctx.query("SELECT 'A' || name || 'B' || name || 'C' AS complex FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("complex").unwrap().contains("AFooBFooC"));

    // Test multiple with NULL (should make entire result NULL)
    let results = ctx.query("SELECT name || null_value || name AS with_null FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("with_null").unwrap().contains("Null"));

    ctx.commit();
}

#[test]
fn test_concatenation_in_where_clause() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Foo'), (2, 'Bar'), (3, 'Baz')");

    // Test concatenation in WHERE clause
    let results = ctx.query("SELECT * FROM Concat WHERE name || 'Bar' = 'FooBar'");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id").unwrap().contains("1"));
    assert!(results[0].get("name").unwrap().contains("Foo"));

    // Test with literal || column
    let results = ctx.query("SELECT * FROM Concat WHERE 'Test' || name = 'TestBar'");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("id").unwrap().contains("2"));

    // Test multiple rows matching
    ctx.exec("INSERT INTO Concat VALUES (4, 'Foo')");
    let results = ctx.query("SELECT * FROM Concat WHERE name || 'Bar' = 'FooBar' ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_concatenation_with_cast() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, rate FLOAT, flag BOOLEAN, name TEXT")
        .insert_values("(1, 2.5, TRUE, 'Foo')"); // 2.5 has exact representation in binary

    // Test concatenation with CAST
    // 2.5 * 10 = 25.0 (exact in binary), so id=1 || 25 = "125"
    let results = ctx.query("SELECT id || CAST(rate * 10 AS INTEGER) AS with_cast FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("with_cast").unwrap().contains("125"));

    // Test CAST result concatenated with text
    let results = ctx.query("SELECT CAST(rate * 10 AS INTEGER) || name AS cast_text FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("cast_text").unwrap().contains("25Foo"));

    ctx.commit();
}

#[test]
fn test_literal_only_concatenation() {
    let mut ctx = setup_test();

    // Test literal-only concatenation (no table needed)
    ctx.exec("CREATE TABLE dummy (id INTEGER)");
    ctx.exec("INSERT INTO dummy VALUES (1)");

    let results = ctx.query("SELECT 'Hello' || ' ' || 'World' AS greeting FROM dummy");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("greeting").unwrap().contains("Hello World"));

    // Test with numbers as literals
    let results = ctx.query("SELECT 1 || 2 || 3 AS numbers FROM dummy");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("numbers").unwrap().contains("123"));

    // Test with boolean literals
    let results = ctx.query("SELECT TRUE || FALSE AS bools FROM dummy");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("bools").unwrap().contains("truefalse"));

    ctx.commit();
}

#[test]
fn test_concatenation_with_string_functions() {
    let mut ctx = setup_test();

    TableBuilder::new(&mut ctx, "Concat")
        .create_simple("id INTEGER, name TEXT")
        .insert_values("(1, 'Hello')");

    // Test with UPPER function
    let results = ctx.query("SELECT UPPER(name) || ' WORLD' AS upper_concat FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("upper_concat")
            .unwrap()
            .contains("HELLO WORLD")
    );

    // Test with LOWER function
    let results = ctx.query("SELECT LOWER('HELLO ') || name AS lower_concat FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("lower_concat")
            .unwrap()
            .contains("hello Hello")
    );

    // Test with SUBSTR function
    let results = ctx.query("SELECT SUBSTR(name, 1, 3) || 'LO' AS substr_concat FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("substr_concat").unwrap().contains("HelLO"));

    // Test SUBSTR on both sides
    let results =
        ctx.query("SELECT SUBSTR('Testing', 1, 4) || SUBSTR(name, 3) AS double_substr FROM Concat");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("double_substr").unwrap().contains("Testllo"));

    ctx.commit();
}
