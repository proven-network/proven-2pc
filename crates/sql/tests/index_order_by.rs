//! Index ORDER BY tests
//! Based on gluesql/test-suite/src/index/order_by.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_for_order_by_index() {
    // TODO: Test CREATE TABLE Test (id INTEGER, num INTEGER NULL, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_data_for_order_by_index() {
    // TODO: Test INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello'), (1, 9, 'Wild'), (3, NULL, 'World'), (4, 7, 'Monday') - 4 inserts
}

#[ignore = "not yet implemented"]
#[test]
fn test_create_order_by_indexes() {
    // TODO: Test CREATE INDEX idx_name ON Test (name)
    // TODO: Test CREATE INDEX idx_id_num_asc ON Test (id + num ASC) - expression index with ASC
    // TODO: Test CREATE INDEX idx_num_desc ON Test (num DESC) - index with DESC order
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_using_name_index() {
    // TODO: Test SELECT * FROM Test ORDER BY name - should use idx_name index and return rows in name order
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_using_expression_index() {
    // TODO: Test SELECT * FROM Test ORDER BY id + num - should use idx_id_num_asc index
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_desc_using_desc_index() {
    // TODO: Test SELECT * FROM Test ORDER BY num DESC - should use idx_num_desc index
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_with_nulls() {
    // TODO: Test how ORDER BY handles NULL values with indexed columns
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_without_matching_index() {
    // TODO: Test ORDER BY on column/expression without matching index - should still work but not use index
}

#[ignore = "not yet implemented"]
#[test]
fn test_order_by_mixed_asc_desc() {
    // TODO: Test ORDER BY with mixed ASC/DESC that doesn't match any single index
}
