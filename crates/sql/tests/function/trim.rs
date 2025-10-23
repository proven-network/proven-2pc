//! TRIM function tests
//! Based on gluesql/test-suite/src/function/trim.rs

use crate::common::{TableBuilder, TestContext, setup_test};
use proven_value::Value;

/// Setup test table with strings containing whitespace
fn setup_trim_table(ctx: &mut TestContext) {
    TableBuilder::new(ctx, "Item")
        .create_simple("name TEXT DEFAULT TRIM(LEADING 'a' FROM 'aabc') || TRIM('   good  ')")
        .insert_values(
            "('      Left blank'), ('Right blank     '), ('     Blank!     '), ('Not Blank')",
        );
}

#[test]
fn test_create_table_with_trim_default() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Item (name TEXT DEFAULT TRIM(LEADING 'a' FROM 'aabc') || TRIM('   good  '))",
    );

    ctx.commit();
}

#[test]
fn test_insert_test_data_with_whitespace() {
    let mut ctx = setup_test();
    setup_trim_table(&mut ctx);

    ctx.assert_row_count("SELECT * FROM Item", 4);

    ctx.commit();
}

#[test]
fn test_basic_trim_function() {
    let mut ctx = setup_test();
    setup_trim_table(&mut ctx);

    let results = ctx.query("SELECT TRIM(name) as trimmed FROM Item");
    assert_eq!(results.len(), 4);

    assert_eq!(
        results[0].get("trimmed").unwrap(),
        &Value::Str("Left blank".to_string())
    );
    assert_eq!(
        results[1].get("trimmed").unwrap(),
        &Value::Str("Right blank".to_string())
    );
    assert_eq!(
        results[2].get("trimmed").unwrap(),
        &Value::Str("Blank!".to_string())
    );
    assert_eq!(
        results[3].get("trimmed").unwrap(),
        &Value::Str("Not Blank".to_string())
    );

    ctx.commit();
}

#[test]
fn test_trim_function_type_error() {
    let mut ctx = setup_test();
    setup_trim_table(&mut ctx);

    ctx.assert_error_contains("SELECT TRIM(1) FROM Item", "TypeMismatch");

    ctx.commit();
}

#[test]
fn test_create_null_table() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    ctx.assert_row_count("SELECT * FROM NullName", 1);

    ctx.commit();
}

#[test]
fn test_trim_with_null_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    let results = ctx.query("SELECT TRIM(name) AS test FROM NullName");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_trim_both_null_from_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    let results = ctx.query("SELECT TRIM(BOTH NULL FROM name) as trimmed FROM NullName");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("trimmed").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_trim_both_null_from_string() {
    let mut ctx = setup_test();

    let results = ctx.query("SELECT TRIM(BOTH NULL FROM 'name') AS test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_trim_trailing_and_leading_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE NullName (name TEXT)");
    ctx.exec("INSERT INTO NullName VALUES (NULL)");

    let results = ctx.query("SELECT TRIM(TRAILING NULL FROM name) as t1 FROM NullName");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("t1").unwrap(), &Value::Null);

    let results = ctx.query("SELECT TRIM(LEADING NULL FROM name) as t2 FROM NullName");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("t2").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_create_test_table_with_patterns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (name TEXT)");
    ctx.exec(
        "INSERT INTO Test VALUES
            ('     blank     '),
            ('xxxyzblankxyzxx'),
            ('xxxyzblank     '),
            ('     blankxyzxx'),
            ('  xyzblankxyzxx'),
            ('xxxyzblankxyz  ')",
    );

    ctx.assert_row_count("SELECT * FROM Test", 6);

    ctx.commit();
}

#[test]
fn test_trim_both_with_character_pattern() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (name TEXT)");
    ctx.exec(
        "INSERT INTO Test VALUES
            ('     blank     '),
            ('xxxyzblankxyzxx'),
            ('xxxyzblank     '),
            ('     blankxyzxx'),
            ('  xyzblankxyzxx'),
            ('xxxyzblankxyz  ')",
    );

    let results = ctx.query("SELECT TRIM(BOTH 'xyz' FROM name) as trimmed FROM Test");
    assert_eq!(results.len(), 6);

    assert_eq!(
        results[0].get("trimmed").unwrap(),
        &Value::Str("     blank     ".to_string())
    );
    assert_eq!(
        results[1].get("trimmed").unwrap(),
        &Value::Str("blank".to_string())
    );
    assert_eq!(
        results[2].get("trimmed").unwrap(),
        &Value::Str("blank     ".to_string())
    );
    assert_eq!(
        results[3].get("trimmed").unwrap(),
        &Value::Str("     blank".to_string())
    );
    assert_eq!(
        results[4].get("trimmed").unwrap(),
        &Value::Str("  xyzblank".to_string())
    );
    assert_eq!(
        results[5].get("trimmed").unwrap(),
        &Value::Str("blankxyz  ".to_string())
    );

    ctx.commit();
}

#[test]
fn test_trim_leading_with_character_pattern() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (name TEXT)");
    ctx.exec(
        "INSERT INTO Test VALUES
            ('     blank     '),
            ('xxxyzblankxyzxx'),
            ('xxxyzblank     '),
            ('     blankxyzxx'),
            ('  xyzblankxyzxx'),
            ('xxxyzblankxyz  ')",
    );

    let results = ctx.query("SELECT TRIM(LEADING 'xyz' FROM name) as trimmed FROM Test");
    assert_eq!(results.len(), 6);

    assert_eq!(
        results[0].get("trimmed").unwrap(),
        &Value::Str("     blank     ".to_string())
    );
    assert_eq!(
        results[1].get("trimmed").unwrap(),
        &Value::Str("blankxyzxx".to_string())
    );
    assert_eq!(
        results[2].get("trimmed").unwrap(),
        &Value::Str("blank     ".to_string())
    );
    assert_eq!(
        results[3].get("trimmed").unwrap(),
        &Value::Str("     blankxyzxx".to_string())
    );
    assert_eq!(
        results[4].get("trimmed").unwrap(),
        &Value::Str("  xyzblankxyzxx".to_string())
    );
    assert_eq!(
        results[5].get("trimmed").unwrap(),
        &Value::Str("blankxyz  ".to_string())
    );

    ctx.commit();
}

#[test]
fn test_trim_trailing_with_character_pattern() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Test (name TEXT)");
    ctx.exec(
        "INSERT INTO Test VALUES
            ('     blank     '),
            ('xxxyzblankxyzxx'),
            ('xxxyzblank     '),
            ('     blankxyzxx'),
            ('  xyzblankxyzxx'),
            ('xxxyzblankxyz  ')",
    );

    let results = ctx.query("SELECT TRIM(TRAILING 'xyz' FROM name) as trimmed FROM Test");
    assert_eq!(results.len(), 6);

    assert_eq!(
        results[0].get("trimmed").unwrap(),
        &Value::Str("     blank     ".to_string())
    );
    assert_eq!(
        results[1].get("trimmed").unwrap(),
        &Value::Str("xxxyzblank".to_string())
    );
    assert_eq!(
        results[2].get("trimmed").unwrap(),
        &Value::Str("xxxyzblank     ".to_string())
    );
    assert_eq!(
        results[3].get("trimmed").unwrap(),
        &Value::Str("     blank".to_string())
    );
    assert_eq!(
        results[4].get("trimmed").unwrap(),
        &Value::Str("  xyzblank".to_string())
    );
    assert_eq!(
        results[5].get("trimmed").unwrap(),
        &Value::Str("xxxyzblankxyz  ".to_string())
    );

    ctx.commit();
}

#[test]
fn test_trim_basic_whitespace_variations() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT
            TRIM(BOTH '  hello  ') as t1,
            TRIM(LEADING '  hello  ') as t2,
            TRIM(TRAILING '  hello  ') as t3",
    );
    assert_eq!(results.len(), 1);

    assert_eq!(
        results[0].get("t1").unwrap(),
        &Value::Str("hello".to_string())
    );
    assert_eq!(
        results[0].get("t2").unwrap(),
        &Value::Str("hello  ".to_string())
    );
    assert_eq!(
        results[0].get("t3").unwrap(),
        &Value::Str("  hello".to_string())
    );

    ctx.commit();
}

#[test]
fn test_trim_nested_and_edge_cases() {
    let mut ctx = setup_test();

    let results = ctx.query(
        "SELECT
            TRIM(BOTH TRIM(BOTH ' potato ')) as t1,
            TRIM('xyz' FROM 'x') as t2,
            TRIM(TRAILING 'xyz' FROM 'xx') as t3",
    );
    assert_eq!(results.len(), 1);

    assert_eq!(
        results[0].get("t1").unwrap(),
        &Value::Str("potato".to_string())
    );
    // TRIM('xyz' FROM 'x') removes all 'x', 'y', 'z' characters, leaving empty string
    assert_eq!(results[0].get("t2").unwrap(), &Value::Str("".to_string()));
    // TRIM(TRAILING 'xyz' FROM 'xx') removes trailing 'x', 'y', 'z' characters, leaving empty string
    assert_eq!(results[0].get("t3").unwrap(), &Value::Str("".to_string()));

    ctx.commit();
}

#[test]
fn test_trim_type_errors() {
    let mut ctx = setup_test();

    ctx.assert_error_contains("SELECT TRIM('1' FROM 1)", "TypeMismatch");

    ctx.assert_error_contains(
        "SELECT TRIM(1 FROM TRIM('t' FROM 'tartare'))",
        "TypeMismatch",
    );

    ctx.commit();
}
