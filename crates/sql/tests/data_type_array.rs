//! ARRAY data type tests (fixed-length arrays)
//! Arrays have a fixed size - all rows must have the same number of elements

mod common;

use common::setup_test;
use proven_value::Value;
#[test]
fn test_create_table_with_array_column() {
    let mut ctx = setup_test();

    // ARRAY with fixed size - all rows must have exactly 3 elements
    ctx.exec("CREATE TABLE ArrayData (id INT, coordinates INT[3])");
    ctx.commit();
}

#[test]
fn test_insert_fixed_array() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points3D (id INT, position FLOAT[3])");

    // All arrays must have exactly 3 elements
    ctx.exec("INSERT INTO Points3D VALUES (1, '[1.0, 2.0, 3.0]')");
    ctx.exec("INSERT INTO Points3D VALUES (2, '[4.5, 5.5, 6.5]')");
    ctx.exec("INSERT INTO Points3D VALUES (3, '[7.0, 8.0, 9.0]')");

    let results = ctx.query("SELECT id, position FROM Points3D ORDER BY id");
    assert_eq!(results.len(), 3);

    // All arrays have the same fixed size
    assert!(
        results[0]
            .get("position")
            .unwrap()
            .to_string()
            .contains("Array")
    );
    assert!(
        results[1]
            .get("position")
            .unwrap()
            .to_string()
            .contains("Array")
    );
    assert!(
        results[2]
            .get("position")
            .unwrap()
            .to_string()
            .contains("Array")
    );

    ctx.commit();
}

#[test]
#[should_panic(expected = "ArraySizeMismatch")]
fn test_insert_wrong_size_array_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points3D (id INT, position FLOAT[3])");

    // Trying to insert array with 2 elements when 3 are required
    ctx.exec("INSERT INTO Points3D VALUES (1, '[1.0, 2.0]')");
}

#[test]
#[should_panic(expected = "ArraySizeMismatch")]
fn test_insert_too_many_elements_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points3D (id INT, position FLOAT[3])");

    // Trying to insert array with 4 elements when exactly 3 are required
    ctx.exec("INSERT INTO Points3D VALUES (1, '[1.0, 2.0, 3.0, 4.0]')");
}

#[test]
fn test_array_2d_fixed_size() {
    let mut ctx = setup_test();

    // 2x2 matrix - fixed size array
    ctx.exec("CREATE TABLE Matrix2x2 (id INT, matrix FLOAT[2][2])");

    ctx.exec("INSERT INTO Matrix2x2 VALUES (1, '[[1.0, 2.0], [3.0, 4.0]]')");
    ctx.exec("INSERT INTO Matrix2x2 VALUES (2, '[[5.0, 6.0], [7.0, 8.0]]')");

    let results = ctx.query("SELECT matrix FROM Matrix2x2 ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_array_bracket_access() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE RGB (id INT, color INT[3])");

    ctx.exec("INSERT INTO RGB VALUES (1, '[255, 0, 0]')"); // Red
    ctx.exec("INSERT INTO RGB VALUES (2, '[0, 255, 0]')"); // Green
    ctx.exec("INSERT INTO RGB VALUES (3, '[0, 0, 255]')"); // Blue

    // Access array elements by index
    let results =
        ctx.query("SELECT id, color[0] AS r, color[1] AS g, color[2] AS b FROM RGB ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("r").unwrap(), &Value::I32(255));
    assert_eq!(results[0].get("g").unwrap(), &Value::I32(0));
    assert_eq!(results[0].get("b").unwrap(), &Value::I32(0));

    assert_eq!(results[1].get("r").unwrap(), &Value::I32(0));
    assert_eq!(results[1].get("g").unwrap(), &Value::I32(255));
    assert_eq!(results[1].get("b").unwrap(), &Value::I32(0));

    ctx.commit();
}

#[test]
fn test_array_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Vectors (id INT, vec FLOAT[3])");

    ctx.exec("INSERT INTO Vectors VALUES (1, '[1.0, 2.0, 3.0]')");
    ctx.exec("INSERT INTO Vectors VALUES (2, NULL)");
    ctx.exec("INSERT INTO Vectors VALUES (3, '[4.0, 5.0, 6.0]')");

    let results = ctx.query("SELECT id FROM Vectors WHERE vec IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    let results = ctx.query("SELECT id FROM Vectors WHERE vec IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_array_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Vectors (id INT, vec INT[2])");

    ctx.exec("INSERT INTO Vectors VALUES (1, '[1, 2]')");
    ctx.exec("INSERT INTO Vectors VALUES (2, '[3, 4]')");
    ctx.exec("INSERT INTO Vectors VALUES (3, '[1, 2]')"); // Duplicate

    // Arrays can be compared for equality
    // First check what we have
    let check = ctx.query("SELECT * FROM Vectors ORDER BY id");
    eprintln!("Vectors table: {:?}", check);

    let results = ctx.query("SELECT id FROM Vectors WHERE vec = '[1, 2]' ORDER BY id");
    eprintln!("Comparison results: {:?}", results);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    ctx.commit();
}

#[test]
fn test_array_type_checking() {
    let mut ctx = setup_test();

    // Create array with specific element type
    ctx.exec("CREATE TABLE IntArrays (id INT, nums INT[3])");
    ctx.exec("CREATE TABLE FloatArrays (id INT, vals FLOAT[3])");
    ctx.exec("CREATE TABLE StringArrays (id INT, words VARCHAR[2])");

    ctx.exec("INSERT INTO IntArrays VALUES (1, '[1, 2, 3]')");
    ctx.exec("INSERT INTO FloatArrays VALUES (1, '[1.1, 2.2, 3.3]')");
    ctx.exec(r#"INSERT INTO StringArrays VALUES (1, '["hello", "world"]')"#);

    let int_results = ctx.query("SELECT nums FROM IntArrays");
    let float_results = ctx.query("SELECT vals FROM FloatArrays");
    let string_results = ctx.query("SELECT words FROM StringArrays");

    assert_eq!(int_results.len(), 1);
    assert_eq!(float_results.len(), 1);
    assert_eq!(string_results.len(), 1);

    ctx.commit();
}

#[test]
fn test_group_by_array() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points (id INT, coord INT[2])");

    ctx.exec("INSERT INTO Points VALUES (1, '[1, 2]')");
    ctx.exec("INSERT INTO Points VALUES (2, '[3, 4]')");
    ctx.exec("INSERT INTO Points VALUES (3, '[1, 2]')"); // Duplicate
    ctx.exec("INSERT INTO Points VALUES (4, '[3, 4]')"); // Another duplicate

    // GROUP BY should work with ARRAY columns
    let results = ctx.query("SELECT coord, COUNT(*) as cnt FROM Points GROUP BY coord");
    assert_eq!(results.len(), 2);

    // Both groups should have count of 2
    for row in &results {
        assert_eq!(row.get("cnt").unwrap(), &Value::I64(2));
    }

    ctx.commit();
}

#[test]
fn test_cast_to_array() {
    let mut ctx = setup_test();

    // CAST string to typed fixed-size array (DuckDB style)
    let results = ctx.query("SELECT CAST('[1, 2, 3]' AS INT[3]) AS my_array");
    assert_eq!(results.len(), 1);
    assert!(
        results[0]
            .get("my_array")
            .unwrap()
            .to_string()
            .contains("Array")
    );

    // Verify it rejects wrong size
    let error = ctx.exec_error("SELECT CAST('[1, 2]' AS INT[3])");
    assert!(
        error.to_string().contains("array with 3 elements")
            || error.to_string().contains("array with 2 elements")
    );

    ctx.commit();
}

#[test]
fn test_array_update() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Colors (id INT PRIMARY KEY, rgb INT[3])");

    ctx.exec("INSERT INTO Colors VALUES (1, '[255, 0, 0]')");
    ctx.exec("INSERT INTO Colors VALUES (2, '[0, 255, 0]')");

    // Update array value
    ctx.exec("UPDATE Colors SET rgb = '[128, 128, 128]' WHERE id = 1");

    let results = ctx.query("SELECT rgb FROM Colors WHERE id = 1");
    assert_eq!(results.len(), 1);
    // Should be the updated gray color
    assert!(results[0].get("rgb").unwrap().to_string().contains("Array"));

    ctx.commit();
}
