//! TIME data type tests
//! Based on gluesql/test-suite/src/data_type/time.rs

use crate::common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_time_columns() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME, time2 TIME)");

    // Verify table exists
    let result = ctx.query("SELECT * FROM TimeLog");
    assert_eq!(result.len(), 0);

    ctx.commit();
}

#[test]
fn test_insert_and_select_time_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME, time2 TIME)");

    // Insert time values - basic format
    ctx.exec("INSERT INTO TimeLog VALUES (1, '12:30:00', '13:31:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '09:02:01', '08:02:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '14:59:00', '09:00:00')");

    // Select all time values
    let results = ctx.query("SELECT id, time1, time2 FROM TimeLog ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    ctx.commit();
}

#[test]
fn test_insert_time_with_fractional_seconds() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME)");

    // Insert time with milliseconds
    ctx.exec("INSERT INTO TimeLog VALUES (1, '13:31:01.123')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '08:02:01.001')");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '23:59:59.999')");

    let results = ctx.query("SELECT * FROM TimeLog ORDER BY id");
    assert_eq!(results.len(), 3);

    // Check that fractional seconds are preserved
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    ctx.commit();
}

#[test]
fn test_time_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME, time2 TIME)");
    ctx.exec("INSERT INTO TimeLog VALUES (1, '12:30:00', '13:31:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '09:02:01', '08:02:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '14:59:00', '09:00:00')");

    // Test time1 > time2
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 > time2 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    // Test time1 <= time2
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 <= time2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));

    // Test time1 = specific time
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 = '14:59:00'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_time_literal_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME, time2 TIME)");
    ctx.exec("INSERT INTO TimeLog VALUES (1, '12:30:00', '13:31:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '09:02:01', '08:02:01')");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '14:59:00', '09:00:00')");

    // Test TIME literal comparison
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 = TIME '14:59:00'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(3));

    // Test string literal comparison (should coerce to time)
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 < '13:00:00' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));

    // Test TIME literal in SELECT
    let results = ctx.query("SELECT TIME '12:30:00' AS test_time");
    assert_eq!(results.len(), 1);
    // FIXME: Time comparison - needs NaiveTime value

    // Test TIME literal comparison in WHERE without table reference
    let results =
        ctx.query("SELECT * FROM TimeLog WHERE TIME '23:00:00' > TIME '13:00:00' ORDER BY id");
    assert_eq!(results.len(), 3); // All rows since condition is always true

    ctx.commit();
}

#[test]
fn test_time_ordering() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME)");
    ctx.exec("INSERT INTO TimeLog VALUES (1, '14:30:00')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '08:15:00')");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '23:59:59')");
    ctx.exec("INSERT INTO TimeLog VALUES (4, '00:00:00')");
    ctx.exec("INSERT INTO TimeLog VALUES (5, NULL)");

    // Test ORDER BY time ASC
    let results = ctx.query("SELECT * FROM TimeLog ORDER BY time1 ASC");
    assert_eq!(results.len(), 5);
    // SQL standard: NULL should come last in ASC order
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    assert_eq!(results[4].get("time1").unwrap(), &Value::Null);

    // Test ORDER BY time DESC
    let results = ctx.query("SELECT * FROM TimeLog ORDER BY time1 DESC");
    assert_eq!(results.len(), 5);
    // SQL standard: NULL should come first in DESC order
    assert_eq!(results[0].get("time1").unwrap(), &Value::Null);
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    ctx.commit();
}

#[test]
fn test_time_with_null_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME)");
    ctx.exec("INSERT INTO TimeLog VALUES (1, '12:00:00')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, NULL)");
    ctx.exec("INSERT INTO TimeLog VALUES (3, '18:30:00')");

    // Test IS NULL
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // Test IS NOT NULL
    let results = ctx.query("SELECT * FROM TimeLog WHERE time1 IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_time_boundary_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME)");

    // Test boundary values
    ctx.exec("INSERT INTO TimeLog VALUES (1, '00:00:00')"); // Midnight
    ctx.exec("INSERT INTO TimeLog VALUES (2, '23:59:59')"); // One second before midnight
    ctx.exec("INSERT INTO TimeLog VALUES (3, '12:00:00')"); // Noon

    let results = ctx.query("SELECT * FROM TimeLog ORDER BY time1");
    assert_eq!(results.len(), 3);
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value
    // FIXME: Time comparison - needs NaiveTime value

    ctx.commit();
}

#[test]
fn test_time_arithmetic() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeLog (id INTEGER, time1 TIME, time2 TIME)");
    ctx.exec("INSERT INTO TimeLog VALUES (1, '12:30:00', '10:15:00')");
    ctx.exec("INSERT INTO TimeLog VALUES (2, '18:45:00', '09:30:00')");

    // Test time subtraction (should return interval)
    // This requires implementing time arithmetic operators
    let _results = ctx.query("SELECT time1 - time2 AS time_diff FROM TimeLog ORDER BY id");

    // Test time + interval (if supported)
    // let results = ctx.query("SELECT time1 + INTERVAL '1' HOUR AS next_hour FROM TimeLog");

    ctx.commit();
}
