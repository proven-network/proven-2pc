//! Bitwise shift left operation tests
//! Based on gluesql/test-suite/src/bitwise_shift_left.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_test_tables() {
    // TODO: Test CREATE TABLE Test (id INTEGER, num INTEGER), CREATE TABLE OverflowTest (id INTEGER, num INTEGER), CREATE TABLE NullTest (id INTEGER, num INTEGER)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_test_data() {
    // TODO: Test INSERT INTO Test (id, num) VALUES (1, 1), (1, 2), (3, 4), (4, 8)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_overflow_test_data() {
    // TODO: Test INSERT INTO OverflowTest (id, num) VALUES (1, 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_null_test_data() {
    // TODO: Test INSERT INTO NullTest (id, num) VALUES (NULL, 1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_bitwise_shift_left_basic() {
    // TODO: Test SELECT (num << 1) as num FROM Test should return 2, 4, 8, 16
}

#[ignore = "not yet implemented"]
#[test]
fn test_bitwise_shift_left_overflow() {
    // TODO: Test SELECT (num << 65) as overflowed FROM OverflowTest should fail with BinaryOperationOverflow
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_from_null_test() {
    // TODO: Test SELECT id, num FROM NullTest should return NULL, 1
}
