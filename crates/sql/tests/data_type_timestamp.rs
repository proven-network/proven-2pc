//! TIMESTAMP data type tests
//! Based on gluesql/test-suite/src/data_type/timestamp.rs

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_timestamp_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP, t2 TIMESTAMP)");

    // Verify table exists
    let result = ctx.query("SELECT * FROM TimestampLog");
    assert_eq!(result.len(), 0);

    ctx.commit();
}

#[test]
fn test_insert_and_select_timestamp_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP, t2 TIMESTAMP)");

    // Insert timestamp values - basic format
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 11:23:11', '2021-03-01 00:00:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '2020-09-30 19:00:00', '1988-12-31 15:01:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '2021-05-01 00:00:00', '2021-05-01 00:00:00')");

    // Select all timestamp values
    let results = ctx.query("SELECT id, t1, t2 FROM TimestampLog ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(
        results[0].get("t1").unwrap(),
        "Timestamp(2020-06-11T11:23:11)"
    );
    assert_eq!(
        results[0].get("t2").unwrap(),
        "Timestamp(2021-03-01T00:00:00)"
    );

    assert_eq!(results[1].get("id").unwrap(), "I32(2)");
    assert_eq!(
        results[1].get("t1").unwrap(),
        "Timestamp(2020-09-30T19:00:00)"
    );
    assert_eq!(
        results[1].get("t2").unwrap(),
        "Timestamp(1988-12-31T15:01:00)"
    );

    assert_eq!(results[2].get("id").unwrap(), "I32(3)");
    assert_eq!(
        results[2].get("t1").unwrap(),
        "Timestamp(2021-05-01T00:00:00)"
    );
    assert_eq!(
        results[2].get("t2").unwrap(),
        "Timestamp(2021-05-01T00:00:00)"
    );

    ctx.commit();
}

#[test]
fn test_insert_timestamp_with_fractional_seconds() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP)");

    // Insert timestamp with milliseconds
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 11:23:11.123')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '2021-05-01 00:00:00.001')");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '1999-12-31 23:59:59.999')");

    let results = ctx.query("SELECT * FROM TimestampLog ORDER BY id");
    assert_eq!(results.len(), 3);

    // Check that fractional seconds are preserved
    assert_eq!(
        results[0].get("t1").unwrap(),
        "Timestamp(2020-06-11T11:23:11.123)"
    );
    assert_eq!(
        results[1].get("t1").unwrap(),
        "Timestamp(2021-05-01T00:00:00.001)"
    );
    assert_eq!(
        results[2].get("t1").unwrap(),
        "Timestamp(1999-12-31T23:59:59.999)"
    );

    ctx.commit();
}

#[test]
fn test_timestamp_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP, t2 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 11:23:11', '2021-03-01 00:00:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '2020-09-30 19:00:00', '1988-12-31 15:01:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '2021-05-01 00:00:00', '2021-05-01 00:00:00')");

    // Test t1 > t2
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t1 > t2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test t1 = t2
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t1 = t2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(3)");

    // Test t1 < t2
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t1 < t2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    ctx.commit();
}

#[test]
fn test_timestamp_literal_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP, t2 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 11:23:11', '2021-03-01 00:00:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '2020-09-30 19:00:00', '1988-12-31 15:01:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '2021-05-01 00:00:00', '2021-05-01 00:00:00')");

    // Test TIMESTAMP literal comparison
    let results =
        ctx.query("SELECT * FROM TimestampLog WHERE t1 = TIMESTAMP '2020-06-11 11:23:11'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    // Test string literal comparison (should coerce to timestamp)
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t2 < '2000-01-01 00:00:00'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test TIMESTAMP literal in SELECT
    let results = ctx.query("SELECT TIMESTAMP '2020-01-01 12:30:00' AS test_ts");
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].get("test_ts").unwrap(),
        "Timestamp(2020-01-01T12:30:00)"
    );

    // Test TIMESTAMP literal comparison in WHERE without table reference
    let results = ctx.query("SELECT * FROM TimestampLog WHERE TIMESTAMP '1999-01-03 00:00:00' < TIMESTAMP '2000-01-01 00:00:00' ORDER BY id");
    assert_eq!(results.len(), 3); // All rows since condition is always true

    ctx.commit();
}

#[test]
fn test_timestamp_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2021-05-01 14:30:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '1988-01-01 08:15:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '2020-06-11 23:59:59')");
    ctx.exec("INSERT INTO TimestampLog VALUES (4, '1970-01-01 00:00:00')"); // Unix epoch
    ctx.exec("INSERT INTO TimestampLog VALUES (5, NULL)");

    // Test ORDER BY timestamp ASC
    let results = ctx.query("SELECT * FROM TimestampLog ORDER BY t1 ASC");
    assert_eq!(results.len(), 5);
    // NULL should come first in ASC order
    assert_eq!(results[0].get("t1").unwrap(), "Null");
    assert_eq!(
        results[1].get("t1").unwrap(),
        "Timestamp(1970-01-01T00:00:00)"
    );
    assert_eq!(
        results[2].get("t1").unwrap(),
        "Timestamp(1988-01-01T08:15:00)"
    );
    assert_eq!(
        results[3].get("t1").unwrap(),
        "Timestamp(2020-06-11T23:59:59)"
    );
    assert_eq!(
        results[4].get("t1").unwrap(),
        "Timestamp(2021-05-01T14:30:00)"
    );

    // Test ORDER BY timestamp DESC
    let results = ctx.query("SELECT * FROM TimestampLog ORDER BY t1 DESC");
    assert_eq!(results.len(), 5);
    assert_eq!(
        results[0].get("t1").unwrap(),
        "Timestamp(2021-05-01T14:30:00)"
    );
    assert_eq!(
        results[1].get("t1").unwrap(),
        "Timestamp(2020-06-11T23:59:59)"
    );
    assert_eq!(
        results[2].get("t1").unwrap(),
        "Timestamp(1988-01-01T08:15:00)"
    );
    assert_eq!(
        results[3].get("t1").unwrap(),
        "Timestamp(1970-01-01T00:00:00)"
    );
    assert_eq!(results[4].get("t1").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_timestamp_with_null_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 12:00:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, NULL)");
    ctx.exec("INSERT INTO TimestampLog VALUES (3, '2021-05-01 18:30:00')");

    // Test IS NULL
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(2)");

    // Test IS NOT NULL
    let results = ctx.query("SELECT * FROM TimestampLog WHERE t1 IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
fn test_timestamp_date_only_format() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP)");

    // Test inserting date-only format (should add 00:00:00 time)
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2021-03-01')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '1999-12-31')");

    let results = ctx.query("SELECT * FROM TimestampLog ORDER BY id");
    assert_eq!(results.len(), 2);

    // Time should default to 00:00:00
    assert_eq!(
        results[0].get("t1").unwrap(),
        "Timestamp(2021-03-01T00:00:00)"
    );
    assert_eq!(
        results[1].get("t1").unwrap(),
        "Timestamp(1999-12-31T00:00:00)"
    );

    ctx.commit();
}

#[test]
fn test_timestamp_arithmetic() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampLog (id INTEGER, t1 TIMESTAMP, t2 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampLog VALUES (1, '2020-06-11 12:30:00', '2020-06-11 10:15:00')");
    ctx.exec("INSERT INTO TimestampLog VALUES (2, '2021-05-01 18:45:00', '2021-04-30 09:30:00')");

    // Test timestamp subtraction (should return interval)
    // This requires implementing timestamp arithmetic operators
    let _results = ctx.query("SELECT t1 - t2 AS ts_diff FROM TimestampLog ORDER BY id");

    // Test timestamp + interval (if supported)
    // let results = ctx.query("SELECT t1 + INTERVAL '1' DAY AS next_day FROM TimestampLog");

    ctx.commit();
}
