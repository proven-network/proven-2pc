//! DECIMAL data type tests
//! Based on gluesql/test-suite/src/data_type/decimal.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_decimal_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1)");

    let results = ctx.query("SELECT v FROM DECIMAL_ITEM");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("v").unwrap(), "Decimal(1)");

    ctx.commit();
}

#[test]
fn test_insert_decimal_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1), (1.5), (2.0), (25.12)");

    let results = ctx.query("SELECT v FROM DECIMAL_ITEM ORDER BY v");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("v").unwrap(), "Decimal(1)");
    assert_eq!(results[1].get("v").unwrap(), "Decimal(1.5)");
    assert_eq!(results[2].get("v").unwrap(), "Decimal(2)"); // Decimal normalizes 2.0 to 2
    assert_eq!(results[3].get("v").unwrap(), "Decimal(25.12)");

    ctx.commit();
}

#[test]
fn test_select_decimal_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1.5), (2.0), (25.12)");

    let results = ctx.query("SELECT v FROM DECIMAL_ITEM WHERE v > 1.5 AND v <= 25.12");
    assert_eq!(results.len(), 2);

    // Results should be 2.0 and 25.12 (in some order)
    let values: Vec<String> = results
        .iter()
        .map(|r| r.get("v").unwrap().clone())
        .collect();
    assert!(values.contains(&"Decimal(2)".to_string())); // Decimal normalizes 2.0 to 2
    assert!(values.contains(&"Decimal(25.12)".to_string()));

    ctx.commit();
}

#[test]
fn test_decimal_arithmetic_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1)");

    // Test addition - Decimal + Integer always returns Decimal
    let results = ctx.query("SELECT v + 1 AS result FROM DECIMAL_ITEM");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), "Decimal(2)");

    // Test subtraction - Decimal - Integer always returns Decimal
    let results = ctx.query("SELECT v - 1 AS result FROM DECIMAL_ITEM");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), "Decimal(0)");

    // Test multiplication - Decimal * Integer always returns Decimal
    let results = ctx.query("SELECT v * 2 AS result FROM DECIMAL_ITEM");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("result").unwrap(), "Decimal(2)");

    // Test division - Decimal / Integer always returns Decimal
    let results = ctx.query("SELECT v / 2 AS result FROM DECIMAL_ITEM");
    assert_eq!(results.len(), 1);
    // Note: rust_decimal may format as 0.5 or 0.50
    let result = results[0].get("result").unwrap();
    assert!(result == "Decimal(0.5)" || result == "Decimal(0.50)");

    ctx.commit();
}

#[test]
fn test_decimal_comparison_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (id INT, v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1, 10.50), (2, 100.00), (3, 50.00)");

    // Test equality
    let results = ctx.query("SELECT id FROM DECIMAL_ITEM WHERE v = 10.50");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    // Test greater than
    let results = ctx.query("SELECT id FROM DECIMAL_ITEM WHERE v > 50.00 ORDER BY id");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test less than
    let results = ctx.query("SELECT id FROM DECIMAL_ITEM WHERE v < 100.00 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
fn test_decimal_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (100.50), (10.25), (50.75), (1.00)");

    // Test ascending order
    let results = ctx.query("SELECT v FROM DECIMAL_ITEM ORDER BY v ASC");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("v").unwrap(), "Decimal(1)"); // Decimal normalizes 1.00 to 1
    assert_eq!(results[1].get("v").unwrap(), "Decimal(10.25)");
    assert_eq!(results[2].get("v").unwrap(), "Decimal(50.75)");
    assert_eq!(results[3].get("v").unwrap(), "Decimal(100.5)"); // Decimal normalizes 100.50 to 100.5

    // Test descending order
    let results = ctx.query("SELECT v FROM DECIMAL_ITEM ORDER BY v DESC");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("v").unwrap(), "Decimal(100.5)"); // Decimal normalizes 100.50 to 100.5
    assert_eq!(results[1].get("v").unwrap(), "Decimal(50.75)");
    assert_eq!(results[2].get("v").unwrap(), "Decimal(10.25)");
    assert_eq!(results[3].get("v").unwrap(), "Decimal(1)");

    ctx.commit();
}

#[test]
fn test_mixed_numeric_type_operations() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1)");

    // Unlike gluesql, our implementation always returns Decimal when one operand is Decimal,
    // regardless of operand order. This is more consistent behavior.
    let results = ctx.query(
        "
        SELECT
            v AS a,
            v + 1 AS b,
            1 + v AS c,
            v - 1 AS d,
            1 - v AS e,
            v * 2 AS f,
            2 * v AS g
        FROM DECIMAL_ITEM
    ",
    );

    assert_eq!(results.len(), 1);
    let row = &results[0];

    assert_eq!(row.get("a").unwrap(), "Decimal(1)");
    assert_eq!(row.get("b").unwrap(), "Decimal(2)"); // v + 1
    assert_eq!(row.get("c").unwrap(), "Decimal(2)"); // 1 + v (also Decimal, unlike gluesql)
    assert_eq!(row.get("d").unwrap(), "Decimal(0)"); // v - 1
    assert_eq!(row.get("e").unwrap(), "Decimal(0)"); // 1 - v (also Decimal, unlike gluesql)
    assert_eq!(row.get("f").unwrap(), "Decimal(2)"); // v * 2
    assert_eq!(row.get("g").unwrap(), "Decimal(2)"); // 2 * v (also Decimal, unlike gluesql)

    ctx.commit();
}

#[test]
fn test_decimal_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DECIMAL_ITEM (v DECIMAL)");
    ctx.exec("INSERT INTO DECIMAL_ITEM VALUES (1.5), (NULL), (2.5)");

    let results = ctx.query("SELECT v FROM DECIMAL_ITEM WHERE v IS NOT NULL ORDER BY v");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("v").unwrap(), "Decimal(1.5)");
    assert_eq!(results[1].get("v").unwrap(), "Decimal(2.5)");

    // Test NULL propagation in arithmetic
    let results = ctx.query("SELECT v + 1 as result FROM DECIMAL_ITEM ORDER BY v");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].get("result").unwrap(), "Null"); // NULL + 1 = NULL

    ctx.commit();
}
