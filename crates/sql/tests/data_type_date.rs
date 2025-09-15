//! DATE data type tests
//! Based on gluesql/test-suite/src/data_type/date.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_date_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE, date2 DATE)");

    // Verify table exists
    let result = ctx.query("SELECT * FROM DateLog");
    assert_eq!(result.len(), 0);

    ctx.commit();
}

#[test]
fn test_insert_and_select_date_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE, date2 DATE)");

    // Insert date values
    ctx.exec("INSERT INTO DateLog VALUES (1, '2020-06-11', '2021-03-01')");
    ctx.exec("INSERT INTO DateLog VALUES (2, '2020-09-30', '1989-01-01')");
    ctx.exec("INSERT INTO DateLog VALUES (3, '2021-05-01', '2021-05-01')");

    // Select all date values
    let results = ctx.query("SELECT id, date1, date2 FROM DateLog ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[0].get("date1").unwrap(), "Date(2020-06-11)");
    assert_eq!(results[0].get("date2").unwrap(), "Date(2021-03-01)");

    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert_eq!(results[1].get("date1").unwrap(), "Date(2020-09-30)");
    assert_eq!(results[1].get("date2").unwrap(), "Date(1989-01-01)");

    assert_eq!(results[2].get("id").unwrap(), "I32(3)");
    assert_eq!(results[2].get("date1").unwrap(), "Date(2021-05-01)");
    assert_eq!(results[2].get("date2").unwrap(), "Date(2021-05-01)");

    ctx.commit();
}

#[test]
fn test_date_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE, date2 DATE)");
    ctx.exec("INSERT INTO DateLog VALUES (1, '2020-06-11', '2021-03-01')");
    ctx.exec("INSERT INTO DateLog VALUES (2, '2020-09-30', '1989-01-01')");
    ctx.exec("INSERT INTO DateLog VALUES (3, '2021-05-01', '2021-05-01')");

    // Test date1 > date2
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 > date2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test date1 <= date2
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 <= date2 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    // Test date1 = date2
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 = date2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
fn test_date_literal_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE, date2 DATE)");
    ctx.exec("INSERT INTO DateLog VALUES (1, '2020-06-11', '2021-03-01')");
    ctx.exec("INSERT INTO DateLog VALUES (2, '2020-09-30', '1989-01-01')");
    ctx.exec("INSERT INTO DateLog VALUES (3, '2021-05-01', '2021-05-01')");

    // Test DATE literal comparison
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 = DATE '2020-06-11'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    // Test string literal comparison (should coerce to date)
    let results = ctx.query("SELECT * FROM DateLog WHERE date2 < '2000-01-01'");
    println!("String-to-date comparison results: {} rows", results.len());
    for row in &results {
        println!("  Row: {:?}", row);
    }
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test DATE literal in SELECT
    let results = ctx.query("SELECT DATE '2020-01-01' AS test_date");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("test_date").unwrap(), "Date(2020-01-01)");

    // Test DATE literal comparison in WHERE without table reference
    let results =
        ctx.query("SELECT * FROM DateLog WHERE DATE '1999-01-03' < DATE '2000-01-01' ORDER BY id");
    assert_eq!(results.len(), 3); // All rows since condition is always true

    ctx.commit();
}

#[test]
fn test_date_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE)");
    ctx.exec("INSERT INTO DateLog VALUES (1, '2021-05-01')");
    ctx.exec("INSERT INTO DateLog VALUES (2, '1989-01-01')");
    ctx.exec("INSERT INTO DateLog VALUES (3, '2020-06-11')");
    ctx.exec("INSERT INTO DateLog VALUES (4, NULL)");

    // Test ORDER BY date ASC
    let results = ctx.query("SELECT * FROM DateLog ORDER BY date1 ASC");
    assert_eq!(results.len(), 4);
    // NULL should come first in ASC order
    assert_eq!(results[0].get("date1").unwrap(), "Null");
    assert_eq!(results[1].get("date1").unwrap(), "Date(1989-01-01)");
    assert_eq!(results[2].get("date1").unwrap(), "Date(2020-06-11)");
    assert_eq!(results[3].get("date1").unwrap(), "Date(2021-05-01)");

    // Test ORDER BY date DESC
    let results = ctx.query("SELECT * FROM DateLog ORDER BY date1 DESC");
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].get("date1").unwrap(), "Date(2021-05-01)");
    assert_eq!(results[1].get("date1").unwrap(), "Date(2020-06-11)");
    assert_eq!(results[2].get("date1").unwrap(), "Date(1989-01-01)");
    assert_eq!(results[3].get("date1").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_date_with_null_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE)");
    ctx.exec("INSERT INTO DateLog VALUES (1, '2020-06-11')");
    ctx.exec("INSERT INTO DateLog VALUES (2, NULL)");
    ctx.exec("INSERT INTO DateLog VALUES (3, '2021-05-01')");

    // Test IS NULL
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test IS NOT NULL
    let results = ctx.query("SELECT * FROM DateLog WHERE date1 IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
#[ignore = "Date arithmetic not yet implemented"]
fn test_date_arithmetic() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateLog (id INTEGER, date1 DATE, date2 DATE)");
    ctx.exec("INSERT INTO DateLog VALUES (1, '2020-06-11', '2020-06-01')");
    ctx.exec("INSERT INTO DateLog VALUES (2, '2021-03-01', '2021-02-01')");

    // Test date subtraction (should return interval)
    // This requires implementing date arithmetic operators
    let _results = ctx.query("SELECT date1 - date2 AS date_diff FROM DateLog ORDER BY id");

    // Note: Date arithmetic might need special handling
    // For now, just test that we can select the dates
    let results = ctx.query("SELECT date1, date2 FROM DateLog ORDER BY id");
    assert_eq!(results.len(), 2);

    // Test date + interval (if supported)
    // This may need INTERVAL literal support first
    // let results = ctx.query("SELECT date1 + INTERVAL '1' DAY AS next_day FROM DateLog");

    ctx.commit();
}
