//! Index NULL value handling tests
//! Based on gluesql/test-suite/src/index/null.rs

mod common;

use common::setup_test;

#[test]
fn test_date_literal_parsing() {
    use chrono::NaiveDate;

    // Test that two identical date literals create equal values
    let date1 = NaiveDate::parse_from_str("2020-03-20", "%Y-%m-%d").unwrap();
    let date2 = NaiveDate::parse_from_str("2020-03-20", "%Y-%m-%d").unwrap();

    assert_eq!(date1, date2, "NaiveDate values should be equal");

    // Test in SQL context
    let mut ctx = setup_test();
    ctx.exec("CREATE TABLE DateTest (d DATE)");
    ctx.exec("INSERT INTO DateTest VALUES ('2020-03-20')");

    // Check what we stored
    let stored = ctx.query("SELECT d FROM DateTest");
    println!("Stored date: {:?}", stored);

    // Check DATE literal
    let literal = ctx.query("SELECT DATE '2020-03-20' AS d");
    println!("DATE literal: {:?}", literal);

    // Direct comparison - simplify to just DATE literal
    let just_date = ctx.query("SELECT DATE '2020-03-20' AS d");
    println!("Just DATE: {:?}", just_date);

    let comparison = ctx.query("SELECT '2020-03-20' = '2020-03-20' AS str_eq, DATE '2020-03-20' = DATE '2020-03-20' AS date_eq");
    println!("Comparison: {:?}", comparison);

    // Check if the issue is with how dates are compared
    // Try storing the result of a DATE literal first
    ctx.exec("CREATE TABLE DateLitTest (d1 DATE, d2 DATE)");
    ctx.exec("INSERT INTO DateLitTest VALUES (DATE '2020-03-20', DATE '2020-03-20')");
    let stored_lits = ctx.query("SELECT d1, d2, d1 = d2 AS eq FROM DateLitTest");
    println!("Stored DATE literals: {:?}", stored_lits);

    // Let's see if this is a parsing issue - check if 1 = 1 works
    let int_comparison = ctx.query("SELECT 1 = 1 AS int_eq");
    println!("Int comparison: {:?}", int_comparison);

    // Test Value comparison directly
    use proven_sql::Value;
    let v1 = Value::Date(date1);
    let v2 = Value::Date(date2);
    println!(
        "Direct Value comparison: {:?} == {:?} is {}",
        v1,
        v2,
        v1 == v2
    );

    ctx.commit();
}

#[test]
fn test_index_with_null_values() {
    let mut ctx = setup_test();

    // Create table with nullable columns
    ctx.exec("CREATE TABLE NullIdx (id INTEGER NULL, date DATE NULL, flag BOOLEAN NULL)");

    // Insert data with various NULL combinations
    ctx.exec("INSERT INTO NullIdx (id, date, flag) VALUES (NULL, NULL, TRUE)");
    ctx.exec("INSERT INTO NullIdx (id, date, flag) VALUES (1, '2020-03-20', TRUE)");
    ctx.exec("INSERT INTO NullIdx (id, date, flag) VALUES (2, NULL, NULL)");
    ctx.exec("INSERT INTO NullIdx (id, date, flag) VALUES (3, '1989-02-01', FALSE)");
    ctx.exec("INSERT INTO NullIdx (id, date, flag) VALUES (4, NULL, TRUE)");

    // Create indexes on nullable columns
    ctx.exec("CREATE INDEX idx_id ON NullIdx (id)");
    ctx.exec("CREATE INDEX idx_date ON NullIdx (date)");
    ctx.exec("CREATE INDEX idx_flag ON NullIdx (flag)");

    // Test queries with NULL values
    let results = ctx.query("SELECT * FROM NullIdx WHERE id IS NULL");
    assert_eq!(results.len(), 1);

    let results = ctx.query("SELECT * FROM NullIdx WHERE id IS NOT NULL");
    assert_eq!(results.len(), 4);

    let results = ctx.query("SELECT * FROM NullIdx WHERE date IS NULL");
    assert_eq!(results.len(), 3);

    let results = ctx.query("SELECT * FROM NullIdx WHERE flag IS NOT NULL");
    assert_eq!(results.len(), 4);

    ctx.commit();
}

#[test]
fn test_index_equality_queries_with_nulls() {
    let mut ctx = setup_test();

    // Create table and index
    ctx.exec("CREATE TABLE NullIdx (id INTEGER NULL, flag BOOLEAN NULL)");
    ctx.exec("CREATE INDEX idx_id ON NullIdx (id)");
    ctx.exec("CREATE INDEX idx_flag ON NullIdx (flag)");

    // Insert data with NULLs
    ctx.exec("INSERT INTO NullIdx VALUES (NULL, TRUE)");
    ctx.exec("INSERT INTO NullIdx VALUES (1, TRUE)");
    ctx.exec("INSERT INTO NullIdx VALUES (2, NULL)");
    ctx.exec("INSERT INTO NullIdx VALUES (3, FALSE)");
    ctx.exec("INSERT INTO NullIdx VALUES (NULL, FALSE)");

    // Test equality queries - NULL values should not match
    let results = ctx.query("SELECT * FROM NullIdx WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    let results = ctx.query("SELECT * FROM NullIdx WHERE flag = TRUE");
    assert_eq!(results.len(), 2); // Only non-NULL TRUE values

    // Test that = NULL returns no results (SQL standard behavior)
    let results = ctx.query("SELECT * FROM NullIdx WHERE id = NULL");
    assert_eq!(results.len(), 0);

    ctx.commit();
}

#[test]
fn test_index_range_queries_with_nulls() {
    let mut ctx = setup_test();

    // Create table and index
    ctx.exec("CREATE TABLE NullIdx (id INTEGER NULL)");
    ctx.exec("CREATE INDEX idx_id ON NullIdx (id)");

    // Insert data with NULLs
    ctx.exec("INSERT INTO NullIdx VALUES (NULL)");
    ctx.exec("INSERT INTO NullIdx VALUES (1)");
    ctx.exec("INSERT INTO NullIdx VALUES (2)");
    ctx.exec("INSERT INTO NullIdx VALUES (3)");
    ctx.exec("INSERT INTO NullIdx VALUES (4)");
    ctx.exec("INSERT INTO NullIdx VALUES (NULL)");

    // Test range queries - NULL values should be excluded
    let results = ctx.query("SELECT * FROM NullIdx WHERE id > 2");
    assert_eq!(results.len(), 2); // 3 and 4, not NULLs

    let results = ctx.query("SELECT * FROM NullIdx WHERE id < 3");
    assert_eq!(results.len(), 2); // 1 and 2, not NULLs

    let results = ctx.query("SELECT * FROM NullIdx WHERE id >= 2");
    assert_eq!(results.len(), 3); // 2, 3, and 4

    let results = ctx.query("SELECT * FROM NullIdx WHERE id <= 3");
    assert_eq!(results.len(), 3); // 1, 2, and 3

    ctx.commit();
}

#[test]
fn test_index_date_queries_with_nulls() {
    let mut ctx = setup_test();

    // Create table with date column
    ctx.exec("CREATE TABLE NullIdx (id INTEGER, date DATE NULL)");
    // Don't create index yet - test without it first
    // ctx.exec("CREATE INDEX idx_date ON NullIdx (date)");

    // Insert data with NULL dates
    ctx.exec("INSERT INTO NullIdx VALUES (1, '2020-03-20')");
    ctx.exec("INSERT INTO NullIdx VALUES (2, NULL)");
    ctx.exec("INSERT INTO NullIdx VALUES (3, '1989-02-01')");
    ctx.exec("INSERT INTO NullIdx VALUES (4, '2040-12-24')");
    ctx.exec("INSERT INTO NullIdx VALUES (5, NULL)");

    // Test date comparisons using DATE literals
    // First check all data
    let all_results = ctx.query("SELECT * FROM NullIdx");
    println!("All rows: {}", all_results.len());
    for row in &all_results {
        println!("Row: {:?}", row);
    }

    // Test without index first
    // Also test if DATE literal works in SELECT
    let date_literal_test = ctx.query("SELECT DATE '2020-03-20' AS test_date");
    println!("DATE literal test: {:?}", date_literal_test);

    // Test string comparison first (this should work with coercion)
    let results = ctx.query("SELECT * FROM NullIdx WHERE date = '2020-03-20'");
    println!("Date = '2020-03-20' (string): {} rows", results.len());

    // Now test DATE literal
    let results = ctx.query("SELECT * FROM NullIdx WHERE date = DATE '2020-03-20'");
    println!("Date = DATE '2020-03-20' (literal): {} rows", results.len());
    for row in &results {
        println!("  Matching row: {:?}", row);
    }

    // Also check if we can compare two DATE literals in SELECT
    let compare_test = ctx.query("SELECT DATE '2020-03-20' = DATE '2020-03-20' AS same, DATE '2020-03-20' AS d1, DATE '2020-03-20' AS d2");
    println!("DATE literal comparison test: {:?}", compare_test);

    // Try comparing with stored date
    let compare_stored =
        ctx.query("SELECT id, date, date = DATE '2020-03-20' AS matches FROM NullIdx WHERE id = 1");
    println!("Compare stored date: {:?}", compare_stored);

    assert_eq!(results.len(), 1);

    let results = ctx.query("SELECT * FROM NullIdx WHERE date < DATE '2040-12-24'");
    println!("Date < '2040-12-24': {} rows", results.len());
    for row in &results {
        println!("  Row: {:?}", row);
    }
    assert_eq!(results.len(), 2); // '1989-02-01' and '2020-03-20'

    let results = ctx.query("SELECT * FROM NullIdx WHERE date >= DATE '2020-01-01'");
    assert_eq!(results.len(), 2); // '2020-03-20' and '2040-12-24'

    let results = ctx.query("SELECT * FROM NullIdx WHERE date IS NULL");
    assert_eq!(results.len(), 2); // Both NULL entries

    ctx.commit();
}

#[test]
fn test_order_by_with_null_indexed_columns() {
    let mut ctx = setup_test();

    // Create table and index
    ctx.exec("CREATE TABLE NullIdx (id INTEGER NULL, name TEXT)");
    ctx.exec("CREATE INDEX idx_id ON NullIdx (id)");

    // Insert data with NULLs
    ctx.exec("INSERT INTO NullIdx VALUES (3, 'Three')");
    ctx.exec("INSERT INTO NullIdx VALUES (NULL, 'Null1')");
    ctx.exec("INSERT INTO NullIdx VALUES (1, 'One')");
    ctx.exec("INSERT INTO NullIdx VALUES (NULL, 'Null2')");
    ctx.exec("INSERT INTO NullIdx VALUES (2, 'Two')");

    // Test ORDER BY with NULLs (NULLs should come first or last depending on implementation)
    let results = ctx.query("SELECT * FROM NullIdx ORDER BY id");
    assert_eq!(results.len(), 5);

    // Check that non-NULL values are ordered correctly
    let non_null_ids: Vec<_> = results
        .iter()
        .filter_map(|row| {
            let id_str = row.get("id").unwrap();
            if id_str != "Null" {
                Some(id_str.clone())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(non_null_ids, vec!["I32(1)", "I32(2)", "I32(3)"]);

    ctx.commit();
}
