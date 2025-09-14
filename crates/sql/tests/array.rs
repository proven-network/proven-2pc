//! Array literal functionality tests
//! Based on gluesql/test-suite/src/array.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_table_with_list_column() {
    // TODO: Test CREATE TABLE Test (id INTEGER DEFAULT 1, name LIST NOT NULL)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_array_literal_single_row() {
    // TODO: Test INSERT INTO Test (id, name) VALUES (1, ['Seongbin','Bernie'])
    // Should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_array_literal_multiple_rows() {
    // TODO: Test INSERT INTO Test (id, name) VALUES (3,Array['Seongbin','Bernie','Chobobdev']), (2,Array['devgony','Henry'])
    // Should return Payload::Insert(2)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_array_literal_simple() {
    // TODO: Test INSERT INTO Test VALUES(5,['Jhon'])
    // Should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_array_with_default_id() {
    // TODO: Test INSERT INTO Test (name) VALUES (['Jane']) - uses DEFAULT value for id
    // Should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_insert_another_array_with_default_id() {
    // TODO: Test INSERT INTO Test (name) VALUES (['GlueSQL'])
    // Should return Payload::Insert(1)
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_all_array_data() {
    // TODO: Test SELECT * FROM Test returns all inserted array data
    // Should show: (1, [Seongbin, Bernie]), (3, [Seongbin, Bernie, Chobobdev]),
    // (2, [devgony, Henry]), (5, [Jhon]), (1, [Jane]), (1, [GlueSQL])
}

#[ignore = "not yet implemented"]
#[test]
fn test_select_mixed_type_array_literal() {
    // TODO: Test SELECT ['name', 1, True] AS list
    // Should return array with string, integer, and boolean values
}

#[ignore = "not yet implemented"]
#[test]
fn test_array_indexing_in_select() {
    // TODO: Test SELECT ['GlueSQL', 1, True] [0] AS list
    // Should return "GlueSQL" (first element of the array)
}

#[ignore = "not yet implemented"]
#[test]
fn test_array_literal_data_types() {
    // TODO: Test that arrays can contain mixed data types (strings, integers, booleans)
}

#[ignore = "not yet implemented"]
#[test]
fn test_array_column_not_null_constraint() {
    // TODO: Test that LIST NOT NULL constraint works properly
}
