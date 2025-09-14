//! Ordering and comparison operator tests
//! Based on gluesql/test-suite/src/ordering.rs

#[ignore = "not yet implemented"]
#[test]
fn test_create_operator_table() {
    // TODO: Test CREATE TABLE Operator (id INTEGER, name TEXT)
}

#[ignore = "not yet implemented"]
#[test]
fn test_delete_and_insert_test_data() {
    // TODO: Test DELETE FROM Operator and INSERT INTO Operator (id, name) VALUES (1, 'Abstract'), (2, 'Azzzz'), (3, 'July'), (4, 'Romeo'), (5, 'Trade')
}

#[ignore = "not yet implemented"]
#[test]
fn test_less_than_operator() {
    // TODO: Test SELECT * FROM Operator WHERE id < 2 should return 1 row
}

#[ignore = "not yet implemented"]
#[test]
fn test_less_than_or_equal_operator() {
    // TODO: Test SELECT * FROM Operator WHERE id <= 2 should return 2 rows
}

#[ignore = "not yet implemented"]
#[test]
fn test_greater_than_operator() {
    // TODO: Test SELECT * FROM Operator WHERE id > 2 should return 3 rows
}

#[ignore = "not yet implemented"]
#[test]
fn test_greater_than_or_equal_operator() {
    // TODO: Test SELECT * FROM Operator WHERE id >= 2 should return 4 rows
}

#[ignore = "not yet implemented"]
#[test]
fn test_reversed_comparison_operators() {
    // TODO: Test SELECT * FROM Operator WHERE 2 > id, WHERE 2 >= id, WHERE 2 < id, WHERE 2 <= id
}

#[ignore = "not yet implemented"]
#[test]
fn test_literal_comparisons() {
    // TODO: Test SELECT * FROM Operator WHERE 1 < 3, WHERE 3 >= 3, WHERE 3 > 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_subquery_comparisons() {
    // TODO: Test SELECT * FROM Operator o1 WHERE 3 > (SELECT MIN(id) FROM Operator WHERE o1.id < 100)
}

#[ignore = "not yet implemented"]
#[test]
fn test_string_comparisons() {
    // TODO: Test SELECT * FROM Operator WHERE name < 'Azzzzzzzzzz', WHERE name < 'Az', WHERE name < 'zz'
}

#[ignore = "not yet implemented"]
#[test]
fn test_string_literal_comparisons() {
    // TODO: Test SELECT * FROM Operator WHERE 'aa' < 'zz', WHERE 'Romeo' >= name
}

#[ignore = "not yet implemented"]
#[test]
fn test_subquery_string_comparisons() {
    // TODO: Test SELECT * FROM Operator WHERE (SELECT name FROM Operator LIMIT 1) >= name, etc.
}

#[ignore = "not yet implemented"]
#[test]
fn test_not_operator_with_inequality() {
    // TODO: Test SELECT * FROM Operator WHERE NOT (1 != 1) should return 5 rows
}

#[ignore = "not yet implemented"]
#[test]
fn test_literal_type_comparisons() {
    // TODO: Test SELECT 1 < 'a', SELECT 1 >= 'a', SELECT 1 = 'a' - all should return false
}
