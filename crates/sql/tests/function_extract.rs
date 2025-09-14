//! EXTRACT function tests
//! Based on gluesql/test-suite/src/function/extract.rs

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_timestamp() {
    // TODO: Test SELECT EXTRACT(HOUR FROM TIMESTAMP '2016-12-31 13:30:15'), EXTRACT(YEAR FROM TIMESTAMP), etc.
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_year_from_timestamp() {
    // TODO: Test SELECT EXTRACT(YEAR FROM TIMESTAMP '2016-12-31 13:30:15') should return 2016
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_month_from_timestamp() {
    // TODO: Test SELECT EXTRACT(MONTH FROM TIMESTAMP '2016-12-31 13:30:15') should return 12
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_day_from_timestamp() {
    // TODO: Test SELECT EXTRACT(DAY FROM TIMESTAMP '2016-12-31 13:30:15') should return 31
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_minute_from_timestamp() {
    // TODO: Test SELECT EXTRACT(MINUTE FROM TIMESTAMP '2016-12-31 13:30:15') should return 30
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_second_from_timestamp() {
    // TODO: Test SELECT EXTRACT(SECOND FROM TIMESTAMP '2016-12-31 13:30:15') should return 15
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_time() {
    // TODO: Test SELECT EXTRACT(SECOND FROM TIME '17:12:28') should return 28
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_date() {
    // TODO: Test SELECT EXTRACT(DAY FROM DATE '2021-10-06') should return 6
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_year() {
    // TODO: Test SELECT EXTRACT(YEAR FROM INTERVAL '3' YEAR) should return 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_month() {
    // TODO: Test SELECT EXTRACT(MONTH FROM INTERVAL '4' MONTH) should return 4
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_day() {
    // TODO: Test SELECT EXTRACT(DAY FROM INTERVAL '5' DAY) should return 5
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_hour() {
    // TODO: Test SELECT EXTRACT(HOUR FROM INTERVAL '6' HOUR) should return 6
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_minute() {
    // TODO: Test SELECT EXTRACT(MINUTE FROM INTERVAL '7' MINUTE) should return 7
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_from_interval_second() {
    // TODO: Test SELECT EXTRACT(SECOND FROM INTERVAL '8' SECOND) should return 8
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_format_not_matched_from_string() {
    // TODO: Test SELECT EXTRACT(HOUR FROM number) from table should fail with ExtractFormatNotMatched
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_failed_from_incompatible_interval() {
    // TODO: Test SELECT EXTRACT(HOUR FROM INTERVAL '7' YEAR) should fail with FailedToExtract
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_format_not_matched_from_integer() {
    // TODO: Test SELECT EXTRACT(HOUR FROM 100) should fail with ExtractFormatNotMatched
}

#[ignore = "not yet implemented"]
#[test]
fn test_extract_unsupported_datetime_field() {
    // TODO: Test SELECT EXTRACT(microseconds FROM '2011-01-1') should fail with UnsupportedDateTimeField
}
