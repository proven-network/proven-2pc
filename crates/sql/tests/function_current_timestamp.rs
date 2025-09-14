//! CURRENT_TIMESTAMP function tests
//! Based on gluesql/test-suite/src/function/current_timestamp.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_current_timestamp_default() {
    // TODO: Test CREATE TABLE Item (timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_timestamp_values() {
    // TODO: Test INSERT INTO Item VALUES ('2021-10-13T06:42:40.364832862'), ('9999-12-31T23:59:40.364832862')
}

#[ignore = "not yet implemented"]
#[test]
fn test_filter_by_current_timestamp() {
    // TODO: Test SELECT timestamp FROM Item WHERE timestamp > CURRENT_TIMESTAMP - should find future timestamps
}
