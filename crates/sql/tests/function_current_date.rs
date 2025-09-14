//! CURRENT_DATE function tests
//! Based on gluesql/test-suite/src/function/current_date.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_current_date_default() {
    // TODO: Test CREATE TABLE Item (date DATE DEFAULT CURRENT_DATE)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_date_values() {
    // TODO: Test INSERT INTO Item VALUES ('2021-06-15'), ('9999-12-31')
}

#[ignore = "not yet implemented"]
#[test]
fn test_filter_by_current_date() {
    // TODO: Test SELECT date FROM Item WHERE date > CURRENT_DATE - should find future dates
}
