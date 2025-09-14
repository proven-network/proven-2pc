//! CAST function tests
//! Based on gluesql/test-suite/src/function/cast.rs

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_boolean() {
    // TODO: Test CAST('TRUE' AS BOOLEAN) and SELECT 1::BOOLEAN - casting various values to boolean
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_boolean_invalid_values() {
    // TODO: Test CAST('asdf' AS BOOLEAN) and CAST(3 AS BOOLEAN) should fail with LiteralCastToBooleanFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_null_to_boolean() {
    // TODO: Test CAST(NULL AS BOOLEAN) should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_integer() {
    // TODO: Test CAST('1' AS INTEGER) and CAST(SUBSTR('123', 2, 3) AS INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_integer_invalid() {
    // TODO: Test CAST('foo' AS INTEGER) and CAST(1.1 AS INTEGER) should fail
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_boolean_to_integer() {
    // TODO: Test CAST(TRUE AS INTEGER) should return 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_int8_uint8() {
    // TODO: Test CAST(255 AS INT8) should fail, test CAST(-1 AS UINT8) should fail
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_uint16() {
    // TODO: Test CAST('foo' AS UINT16) and CAST(-1 AS UINT16) should fail
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_float() {
    // TODO: Test CAST('1.1' AS FLOAT), CAST(1 AS FLOAT), CAST(TRUE AS FLOAT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_float_invalid() {
    // TODO: Test CAST('foo' AS FLOAT) should fail with LiteralCastFromTextToFloatFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_decimal() {
    // TODO: Test CAST(true AS Decimal), CAST('1.1' AS Decimal), CAST(1 AS Decimal), CAST(-1 AS Decimal)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_decimal_invalid() {
    // TODO: Test CAST('foo' AS Decimal) should fail with LiteralCastFromTextToDecimalFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_table_values_to_decimal() {
    // TODO: Test CAST(mytext AS Decimal), CAST(myint8 AS Decimal), etc. from table columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_text() {
    // TODO: Test CAST(1 AS TEXT), CAST(1.1 AS TEXT), CAST(TRUE AS TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_interval() {
    // TODO: Test CAST(NULL AS INTERVAL) should fail with UnimplementedLiteralCast
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_interval_strings() {
    // TODO: Test CAST('''1-2'' YEAR TO MONTH' AS INTERVAL) and other interval formats
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_date() {
    // TODO: Test CAST('2021-08-25' AS DATE), CAST('08-25-2021' AS DATE)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_date_invalid() {
    // TODO: Test CAST('2021-08-025' AS DATE) should fail with LiteralCastToDateFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_time() {
    // TODO: Test CAST('AM 8:05' AS TIME), CAST('8:05:30.9 AM' AS TIME)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_time_invalid() {
    // TODO: Test CAST('25:08:05' AS TIME) should fail with LiteralCastToTimeFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_timestamp() {
    // TODO: Test CAST('2021-08-25 08:05:30' AS TIMESTAMP), CAST('2021-08-25 08:05:30.9' AS TIMESTAMP)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_to_timestamp_invalid() {
    // TODO: Test CAST('2021-13-25 08:05:30' AS TIMESTAMP) should fail with LiteralCastToTimestampFailed
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_value_from_expression() {
    // TODO: Test CAST(LOWER(number) AS INTEGER), CAST(id AS BOOLEAN), CAST(flag AS TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_value_null_handling() {
    // TODO: Test CAST(ratio AS INTEGER) where ratio is NULL should return NULL
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_value_conversion_errors() {
    // TODO: Test CAST(number AS BOOLEAN) should fail with ConvertError
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_interval_from_table() {
    // TODO: Test CAST(interval_str_1 AS INTERVAL) from table with various interval strings
}

#[ignore = "not yet implemented"]
#[test]
fn test_cast_unsupported_format() {
    // TODO: Test CAST(1 AS STRING FORMAT 'ASCII') should fail with UnsupportedCastFormat
}
