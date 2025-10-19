//! Tests for DEFAULT values in column definitions
//! Based on gluesql/test-suite/src/default.rs

mod common;
use common::setup_test;
use proven_value::Value;
#[test]
fn test_default_values_basic() {
    let mut ctx = setup_test();

    // Create table with DEFAULT values
    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    );

    // Insert with all values specified
    ctx.exec("INSERT INTO Test VALUES (8, 80, true)");

    // Insert with only num specified, others use defaults
    ctx.exec("INSERT INTO Test (num) VALUES (10)");

    // Insert with num and id specified in different order
    ctx.exec("INSERT INTO Test (num, id) VALUES (20, 2)");

    // Insert multiple rows, some with NULL
    ctx.exec("INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true)");

    // Verify all data with defaults applied
    let results = ctx.query("SELECT id, num, flag FROM Test ORDER BY num");
    assert_eq!(results.len(), 5);

    // Row 1: INSERT INTO Test (num) VALUES (10) - uses default id=1, default flag=false
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(false));

    // Row 2: INSERT INTO Test (num, id) VALUES (20, 2) - uses default flag=false
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(20));
    assert_eq!(results[1].get("flag").unwrap(), &Value::Bool(false));

    // Row 3: INSERT INTO Test (num, flag) VALUES (30, NULL) - uses default id=1, explicit NULL
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[2].get("num").unwrap(), &Value::I32(30));
    assert_eq!(results[2].get("flag").unwrap(), &Value::Null);

    // Row 4: INSERT INTO Test (num, flag) VALUES (40, true) - uses default id=1
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[3].get("num").unwrap(), &Value::I32(40));
    assert_eq!(results[3].get("flag").unwrap(), &Value::Bool(true));

    // Row 5: INSERT INTO Test VALUES (8, 80, true) - all values explicit
    assert_eq!(results[4].get("id").unwrap(), &Value::I32(8));
    assert_eq!(results[4].get("num").unwrap(), &Value::I32(80));
    assert_eq!(results[4].get("flag").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_insert_with_all_values() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    );

    // Insert providing all values (overriding defaults)
    ctx.exec("INSERT INTO Test VALUES (8, 80, true)");

    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(8));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(80));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_insert_partial_values_with_defaults() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    );

    // Insert with missing columns using default values
    ctx.exec("INSERT INTO Test (num) VALUES (10)");

    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(10));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_insert_partial_values_mixed_order() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    );

    // Insert with columns in different order
    ctx.exec("INSERT INTO Test (num, id) VALUES (20, 2)");

    let results = ctx.query("SELECT * FROM Test");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(20));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_insert_multiple_rows_with_defaults() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 1,
            num INTEGER,
            flag BOOLEAN NULL DEFAULT false
        )",
    );

    // Insert multiple rows with some using defaults
    ctx.exec("INSERT INTO Test (num, flag) VALUES (30, NULL), (40, true)");

    let results = ctx.query("SELECT * FROM Test ORDER BY num");
    assert_eq!(results.len(), 2);

    // First row: NULL explicitly overrides default
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(30));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Null);

    // Second row: true overrides default false
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("num").unwrap(), &Value::I32(40));
    assert_eq!(results[1].get("flag").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_select_all_with_default_values() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Test (
            id INTEGER DEFAULT 5,
            name TEXT DEFAULT 'unnamed',
            active BOOLEAN DEFAULT true
        )",
    );

    // Insert with no values specified
    ctx.exec("INSERT INTO Test DEFAULT VALUES");

    // Insert with partial values
    ctx.exec("INSERT INTO Test (name) VALUES ('Alice')");

    let results = ctx.query("SELECT * FROM Test ORDER BY name");
    assert_eq!(results.len(), 2);

    // First row: name='Alice', others use defaults
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(5));
    assert_eq!(
        results[0].get("name").unwrap(),
        &Value::Str("Alice".to_string())
    );
    assert_eq!(results[0].get("active").unwrap(), &Value::Bool(true));

    // Second row: all defaults
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(5));
    assert_eq!(
        results[1].get("name").unwrap(),
        &Value::Str("unnamed".to_string())
    );
    assert_eq!(results[1].get("active").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_default_with_stateless_functions() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE FunctionTest (
            id UUID DEFAULT GENERATE_UUID(),
            num FLOAT
        )",
    );

    ctx.exec("INSERT INTO FunctionTest (num) VALUES (1.0)");
    ctx.exec("INSERT INTO FunctionTest (num) VALUES (2.0)");

    let results = ctx.query("SELECT * FROM FunctionTest");
    assert_eq!(results.len(), 2);

    // UUIDs should be generated and different
    let uuid1 = results[0].get("id").unwrap();
    let uuid2 = results[1].get("id").unwrap();
    assert_ne!(uuid1, uuid2);

    ctx.commit();
}

#[test]
fn test_subquery_in_default_clause_prohibited() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Foo (id INTEGER)");
    ctx.exec("INSERT INTO Foo VALUES (1)");

    // Subqueries in DEFAULT clauses should fail (non-deterministic)
    let error = ctx.exec_error(
        "CREATE TABLE FunctionTest (
            id UUID DEFAULT GENERATE_UUID(),
            num FLOAT DEFAULT (SELECT id FROM Foo)
        )",
    );
    assert!(
        error.contains("subquery") || error.contains("not allowed") || error.contains("DEFAULT"),
        "Expected error for subquery in DEFAULT clause, got: {}",
        error
    );

    ctx.commit();
}

#[test]
fn test_default_with_complex_expressions() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE TestExpr (
            id INTEGER,
            date DATE DEFAULT DATE '2020-01-01',
            num INTEGER DEFAULT 2,
            flag BOOLEAN DEFAULT true,
            flag2 BOOLEAN DEFAULT true,
            flag3 BOOLEAN DEFAULT false,
            flag4 BOOLEAN DEFAULT false
        )",
    );

    ctx.exec("INSERT INTO TestExpr (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM TestExpr");
    assert_eq!(results.len(), 1);

    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    // FIXME: Need proper Date value comparison
    // assert_eq!(results[0].get("date").unwrap(), ...);
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(true));
    assert_eq!(results[0].get("flag2").unwrap(), &Value::Bool(true));
    assert_eq!(results[0].get("flag3").unwrap(), &Value::Bool(false));
    assert_eq!(results[0].get("flag4").unwrap(), &Value::Bool(false));

    ctx.commit();
}

#[test]
fn test_default_date_literal() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE DateTest (
            id INTEGER,
            created DATE DEFAULT DATE '2020-01-01'
        )",
    );

    ctx.exec("INSERT INTO DateTest (id) VALUES (1)");
    ctx.exec("INSERT INTO DateTest (id, created) VALUES (2, DATE '2021-06-15')");

    let results = ctx.query("SELECT * FROM DateTest ORDER BY id");
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    // FIXME: Need proper Date value comparison

    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    // FIXME: Need proper Date value comparison

    ctx.commit();
}

#[test]
fn test_default_arithmetic_expression() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE ArithTest (
            id INTEGER,
            num INTEGER DEFAULT -(-1 * +2)
        )",
    );

    ctx.exec("INSERT INTO ArithTest (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM ArithTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("num").unwrap(), &Value::I32(2)); // -(-1 * +2) = 2

    ctx.commit();
}

#[test]
fn test_default_cast_expression() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE CastTest (
            id INTEGER,
            flag BOOLEAN DEFAULT CAST('TRUE' AS BOOLEAN)
        )",
    );

    ctx.exec("INSERT INTO CastTest (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM CastTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(true));

    ctx.commit();
}

#[test]
fn test_default_in_expression() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE InTest (
            id INTEGER,
            flag BOOLEAN DEFAULT 1 IN (1, 2, 3)
        )",
    );

    ctx.exec("INSERT INTO InTest (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM InTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(true)); // 1 IN (1,2,3) = true

    ctx.commit();
}

#[test]
fn test_default_between_expression() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE BetweenTest (
            id INTEGER,
            flag BOOLEAN DEFAULT 10 BETWEEN 1 AND 2
        )",
    );

    ctx.exec("INSERT INTO BetweenTest (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM BetweenTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(false)); // 10 BETWEEN 1 AND 2 = false

    ctx.commit();
}

#[test]
fn test_default_boolean_logic_expression() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE BoolTest (
            id INTEGER,
            flag BOOLEAN DEFAULT (1 IS NULL OR NULL IS NOT NULL)
        )",
    );

    ctx.exec("INSERT INTO BoolTest (id) VALUES (1)");

    let results = ctx.query("SELECT * FROM BoolTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[0].get("flag").unwrap(), &Value::Bool(false)); // (1 IS NULL OR NULL IS NOT NULL) = false

    ctx.commit();
}

#[test]
fn test_default_with_null_override() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE NullTest (
            id INTEGER DEFAULT 10,
            name TEXT DEFAULT 'default_name'
        )",
    );

    // NULL explicitly provided should override default
    ctx.exec("INSERT INTO NullTest VALUES (NULL, NULL)");

    let results = ctx.query("SELECT * FROM NullTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::Null);
    assert_eq!(results[0].get("name").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
fn test_default_values_different_types() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE TypeTest (
            int_col INTEGER DEFAULT 42,
            text_col TEXT DEFAULT 'hello',
            bool_col BOOLEAN DEFAULT false,
            date_col DATE DEFAULT DATE '2023-01-01',
            time_col TIME DEFAULT TIME '12:00:00',
            timestamp_col TIMESTAMP DEFAULT TIMESTAMP '2023-01-01 12:00:00'
        )",
    );

    ctx.exec("INSERT INTO TypeTest DEFAULT VALUES");

    let results = ctx.query("SELECT * FROM TypeTest");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("int_col").unwrap(), &Value::I32(42));
    assert_eq!(
        results[0].get("text_col").unwrap(),
        &Value::Str("hello".to_string())
    );
    assert_eq!(results[0].get("bool_col").unwrap(), &Value::Bool(false));
    // FIXME: Need proper Date value comparison
    // FIXME: Need proper Time value comparison
    assert!(
        results[0]
            .get("timestamp_col")
            .unwrap()
            .to_string()
            .contains("2023-01-01")
    );

    ctx.commit();
}
