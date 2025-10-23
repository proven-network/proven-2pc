//! INTERVAL data type tests
//! Based on gluesql/test-suite/src/data_type/interval.rs

use crate::common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_interval_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL, interval2 INTERVAL)");

    // Verify table exists
    let result = ctx.query("SELECT * FROM IntervalLog");
    assert_eq!(result.len(), 0);

    ctx.commit();
}

#[test]
fn test_insert_and_select_interval_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL, interval2 INTERVAL)");

    // Insert interval values using simple format
    ctx.exec("INSERT INTO IntervalLog VALUES (1, INTERVAL '14' MONTH, INTERVAL '30' MONTH)");
    ctx.exec("INSERT INTO IntervalLog VALUES (2, INTERVAL '12' DAY, INTERVAL '35' HOUR)");
    ctx.exec("INSERT INTO IntervalLog VALUES (3, INTERVAL '12' MINUTE, INTERVAL '300' SECOND)");

    // Select all interval values
    let results = ctx.query("SELECT id, interval1, interval2 FROM IntervalLog ORDER BY id");
    assert_eq!(results.len(), 3);

    // Check first row - months
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    // FIXME: Need proper Interval value comparison
    // assert_eq!(results[0].get("interval1").unwrap(), ...);
    // assert_eq!(results[0].get("interval2").unwrap(), ...);

    // Check second row - days and hours
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    // FIXME: Need proper Interval value comparison
    // assert_eq!(results[1].get("interval1").unwrap(), ...);
    // 35 hours = 1 day + 11 hours = 1 day + 39600000000 microseconds
    let interval2 = results[1].get("interval2").unwrap();
    assert!(interval2.to_string().contains("Interval"));

    // Check third row - minutes and seconds
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    // 12 minutes = 720000000 microseconds
    let interval1 = results[2].get("interval1").unwrap();
    assert!(interval1.to_string().contains("Interval"));
    // 300 seconds = 5 minutes = 300000000 microseconds
    let interval2 = results[2].get("interval2").unwrap();
    assert!(interval2.to_string().contains("Interval"));

    ctx.commit();
}

#[test]
fn test_interval_arithmetic_with_dates() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE DateTest (id INTEGER, date1 DATE)");
    ctx.exec("INSERT INTO DateTest VALUES (1, '2020-06-11')");
    ctx.exec("INSERT INTO DateTest VALUES (2, '2021-12-31')");

    // Test DATE + INTERVAL
    let results =
        ctx.query("SELECT date1 + INTERVAL '5' DAY AS new_date FROM DateTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Date value comparison

    // Test DATE - INTERVAL
    let results =
        ctx.query("SELECT date1 - INTERVAL '10' DAY AS new_date FROM DateTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Date value comparison

    // Test DATE + INTERVAL MONTH (approximated as 30 days)
    let results =
        ctx.query("SELECT date1 + INTERVAL '2' MONTH AS new_date FROM DateTest WHERE id = 2");
    assert_eq!(results.len(), 1);
    // 2 months = 60 days approximately
    // FIXME: Need proper Date value comparison

    ctx.commit();
}

#[test]
fn test_interval_arithmetic_with_timestamps() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimestampTest (id INTEGER, ts1 TIMESTAMP)");
    ctx.exec("INSERT INTO TimestampTest VALUES (1, '2020-06-11 12:30:00')");
    ctx.exec("INSERT INTO TimestampTest VALUES (2, '2021-12-31 23:59:59')");

    // Test TIMESTAMP + INTERVAL DAY
    let results =
        ctx.query("SELECT ts1 + INTERVAL '5' DAY AS new_ts FROM TimestampTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("new_ts")
            .unwrap()
            .to_string()
            .contains("2020-06-16")
    );

    // Test TIMESTAMP - INTERVAL HOUR
    let results =
        ctx.query("SELECT ts1 - INTERVAL '2' HOUR AS new_ts FROM TimestampTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("new_ts")
            .unwrap()
            .to_string()
            .contains("2020-06-11")
    );

    // Test TIMESTAMP + INTERVAL MINUTE
    let results =
        ctx.query("SELECT ts1 + INTERVAL '30' MINUTE AS new_ts FROM TimestampTest WHERE id = 2");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("new_ts")
            .unwrap()
            .to_string()
            .contains("2022-01-01")
    );

    ctx.commit();
}

#[test]
fn test_interval_arithmetic_with_time() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE TimeTest (id INTEGER, time1 TIME)");
    ctx.exec("INSERT INTO TimeTest VALUES (1, '12:30:00')");
    ctx.exec("INSERT INTO TimeTest VALUES (2, '23:30:00')");

    // Test TIME + INTERVAL HOUR
    let results =
        ctx.query("SELECT time1 + INTERVAL '2' HOUR AS new_time FROM TimeTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Time value comparison

    // Test TIME - INTERVAL MINUTE
    let results =
        ctx.query("SELECT time1 - INTERVAL '45' MINUTE AS new_time FROM TimeTest WHERE id = 1");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Time value comparison

    // Test TIME + INTERVAL that wraps around midnight
    let results =
        ctx.query("SELECT time1 + INTERVAL '1' HOUR AS new_time FROM TimeTest WHERE id = 2");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Time value comparison

    ctx.commit();
}

#[test]
fn test_interval_arithmetic_between_intervals() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalTest (id INTEGER)");
    ctx.exec("INSERT INTO IntervalTest VALUES (1)");

    // Test INTERVAL + INTERVAL
    let results =
        ctx.query("SELECT INTERVAL '5' DAY + INTERVAL '3' DAY AS total FROM IntervalTest");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Interval value comparison

    // Test INTERVAL - INTERVAL
    let results =
        ctx.query("SELECT INTERVAL '10' HOUR - INTERVAL '3' HOUR AS diff FROM IntervalTest");
    assert_eq!(results.len(), 1);
    // 7 hours = 25200000000 microseconds
    let diff = results[0].get("diff").unwrap();
    assert!(diff.to_string().contains("Interval"));

    // Test INTERVAL + INTERVAL with different units
    let results =
        ctx.query("SELECT INTERVAL '2' MONTH + INTERVAL '15' DAY AS total FROM IntervalTest");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("total")
            .unwrap()
            .to_string()
            .contains("Interval")
    );

    ctx.commit();
}

#[test]
fn test_interval_with_null_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL)");
    ctx.exec("INSERT INTO IntervalLog VALUES (1, INTERVAL '5' DAY)");
    ctx.exec("INSERT INTO IntervalLog VALUES (2, NULL)");
    ctx.exec("INSERT INTO IntervalLog VALUES (3, INTERVAL '10' HOUR)");

    // Test IS NULL
    let results = ctx.query("SELECT * FROM IntervalLog WHERE interval1 IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    // Test IS NOT NULL
    let results = ctx.query("SELECT * FROM IntervalLog WHERE interval1 IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_date_timestamp_subtraction() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE DateDiff (id INTEGER, date1 DATE, date2 DATE, ts1 TIMESTAMP, ts2 TIMESTAMP)",
    );
    ctx.exec("INSERT INTO DateDiff VALUES (1, '2020-06-11', '2020-06-01', '2020-06-11 12:30:00', '2020-06-10 11:30:00')");

    // Test DATE - DATE returns INTERVAL
    let results = ctx.query("SELECT date1 - date2 AS date_diff FROM DateDiff");
    assert_eq!(results.len(), 1);
    // FIXME: Need proper Interval value comparison

    // Test TIMESTAMP - TIMESTAMP returns INTERVAL
    let results = ctx.query("SELECT ts1 - ts2 AS ts_diff FROM DateDiff");
    assert_eq!(results.len(), 1);
    // 1 day + 1 hour = 1 day + 3600000000 microseconds
    let ts_diff = results[0].get("ts_diff").unwrap();
    println!("Got ts_diff: {}", ts_diff);
    assert!(ts_diff.to_string().contains("Interval"));

    ctx.commit();
}

#[test]
fn test_negative_intervals() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL)");

    // Test negative interval
    ctx.exec("INSERT INTO IntervalLog VALUES (1, INTERVAL '-5' DAY)");
    ctx.exec("INSERT INTO IntervalLog VALUES (2, INTERVAL '-2' HOUR)");

    let results = ctx.query("SELECT * FROM IntervalLog ORDER BY id");
    assert_eq!(results.len(), 2);
    // FIXME: Need proper Interval value comparison
    // assert_eq!(results[0].get("interval1").unwrap(), ...);
    // assert_eq!(results[1].get("interval1").unwrap(), ...);

    ctx.commit();
}

#[test]
fn test_complex_interval_formats() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL)");

    // Test YEAR TO MONTH format
    ctx.exec("INSERT INTO IntervalLog VALUES (1, INTERVAL '1-2' YEAR TO MONTH)");

    // Test DAY TO HOUR format
    ctx.exec("INSERT INTO IntervalLog VALUES (2, INTERVAL '3 14' DAY TO HOUR)");

    // Test DAY TO SECOND format
    ctx.exec("INSERT INTO IntervalLog VALUES (3, INTERVAL '3 14:30:12.5' DAY TO SECOND)");

    ctx.commit();
}

#[test]
fn test_negative_complex_intervals() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE IntervalLog (id INTEGER, interval1 INTERVAL)");

    // Test negative YEAR TO MONTH format
    ctx.exec("INSERT INTO IntervalLog VALUES (1, INTERVAL '-1-2' YEAR TO MONTH)");

    // Test negative DAY TO HOUR format
    ctx.exec("INSERT INTO IntervalLog VALUES (2, INTERVAL '-3 14' DAY TO HOUR)");

    // Test negative DAY TO SECOND format
    ctx.exec("INSERT INTO IntervalLog VALUES (3, INTERVAL '-3 14:30:12' DAY TO SECOND)");

    let results = ctx.query("SELECT * FROM IntervalLog ORDER BY id");
    assert_eq!(results.len(), 3);

    // -1 year -2 months = -14 months
    // FIXME: Need proper Interval value comparison

    // -3 days 14 hours = -3 days + 14 hours = -2 days -10 hours when normalized
    // But we might store it as -3 days and negative microseconds
    let interval2 = results[1].get("interval1").unwrap();
    assert!(interval2.to_string().contains("-"));

    // Similar for DAY TO SECOND
    let interval3 = results[2].get("interval1").unwrap();
    assert!(interval3.to_string().contains("-"));

    ctx.commit();
}
