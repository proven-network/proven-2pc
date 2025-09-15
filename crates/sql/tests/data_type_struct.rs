//! STRUCT data type tests (record/composite types)
//! Structs have named fields, similar to row types or records in other databases

mod common;

use common::setup_test;

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_create_table_with_struct_column() {
    let mut ctx = setup_test();

    // STRUCT with named fields
    ctx.exec("CREATE TABLE Users (id INT, profile STRUCT(name VARCHAR, age INT, email VARCHAR))");
    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_insert_struct_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Persons (id INT, info STRUCT(name VARCHAR, age INT))");

    // Insert struct values using object notation
    ctx.exec(r#"INSERT INTO Persons VALUES (1, '{"name": "Alice", "age": 30}')"#);
    ctx.exec(r#"INSERT INTO Persons VALUES (2, '{"name": "Bob", "age": 25}')"#);
    ctx.exec(r#"INSERT INTO Persons VALUES (3, '{"name": "Charlie", "age": 35}')"#);

    let results = ctx.query("SELECT id, info FROM Persons ORDER BY id");
    assert_eq!(results.len(), 3);

    assert!(results[0].get("info").unwrap().contains("Struct"));
    assert!(results[1].get("info").unwrap().contains("Struct"));
    assert!(results[2].get("info").unwrap().contains("Struct"));

    ctx.commit();
}

#[test]
#[ignore = "STRUCT field access not yet implemented"]
fn test_struct_field_access() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Employees (id INT, details STRUCT(name VARCHAR, salary FLOAT, department VARCHAR))");

    ctx.exec(r#"INSERT INTO Employees VALUES (1, '{"name": "Alice", "salary": 75000.0, "department": "Engineering"}')"#);
    ctx.exec(r#"INSERT INTO Employees VALUES (2, '{"name": "Bob", "salary": 65000.0, "department": "Sales"}')"#);
    ctx.exec(r#"INSERT INTO Employees VALUES (3, '{"name": "Charlie", "salary": 80000.0, "department": "Engineering"}')"#);

    // Access struct fields using dot notation
    let results = ctx.query("SELECT id, details.name AS emp_name, details.salary AS emp_salary FROM Employees ORDER BY id");
    assert_eq!(results.len(), 3);

    assert_eq!(results[0].get("emp_name").unwrap(), "Str(\"Alice\")");
    assert_eq!(results[0].get("emp_salary").unwrap(), "F64(75000.0)");
    assert_eq!(results[1].get("emp_name").unwrap(), "Str(\"Bob\")");
    assert_eq!(results[1].get("emp_salary").unwrap(), "F64(65000.0)");

    ctx.commit();
}

#[test]
#[ignore = "STRUCT field access not yet implemented"]
fn test_struct_in_where_clause() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Employees (id INT, details STRUCT(name VARCHAR, department VARCHAR, active BOOL))");

    ctx.exec(r#"INSERT INTO Employees VALUES (1, '{"name": "Alice", "department": "Engineering", "active": true}')"#);
    ctx.exec(r#"INSERT INTO Employees VALUES (2, '{"name": "Bob", "department": "Sales", "active": false}')"#);
    ctx.exec(r#"INSERT INTO Employees VALUES (3, '{"name": "Charlie", "department": "Engineering", "active": true}')"#);

    // Filter by struct field
    let results = ctx.query("SELECT id, details.name FROM Employees WHERE details.department = 'Engineering' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    // Filter by boolean field
    let results = ctx.query("SELECT id FROM Employees WHERE details.active = true ORDER BY id");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_nested_structs() {
    let mut ctx = setup_test();

    // Struct containing another struct
    ctx.exec(
        "CREATE TABLE Companies (
        id INT,
        info STRUCT(
            name VARCHAR,
            address STRUCT(
                street VARCHAR,
                city VARCHAR,
                zipcode VARCHAR
            ),
            founded INT
        )
    )",
    );

    ctx.exec(
        r#"INSERT INTO Companies VALUES (1, '{
        "name": "TechCorp",
        "address": {
            "street": "123 Main St",
            "city": "San Francisco",
            "zipcode": "94102"
        },
        "founded": 2010
    }')"#,
    );

    let results = ctx.query("SELECT info FROM Companies");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("info").unwrap().contains("Struct"));

    ctx.commit();
}

#[test]
#[ignore = "STRUCT field access not yet implemented"]
fn test_nested_struct_field_access() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Companies (
        id INT,
        info STRUCT(
            name VARCHAR,
            address STRUCT(street VARCHAR, city VARCHAR),
            employees INT
        )
    )",
    );

    ctx.exec(
        r#"INSERT INTO Companies VALUES (1, '{
        "name": "TechCorp",
        "address": {"street": "123 Main St", "city": "SF"},
        "employees": 100
    }')"#,
    );

    // Access nested struct fields
    let results = ctx.query("SELECT info.address.city AS city FROM Companies");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("city").unwrap(), "Str(\"SF\")");

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_struct_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Profiles (id INT, data STRUCT(name VARCHAR, age INT))");

    ctx.exec(r#"INSERT INTO Profiles VALUES (1, '{"name": "Alice", "age": 30}')"#);
    ctx.exec("INSERT INTO Profiles VALUES (2, NULL)");
    ctx.exec(r#"INSERT INTO Profiles VALUES (3, '{"name": "Bob", "age": null}')"#); // Null field

    let results = ctx.query("SELECT id FROM Profiles WHERE data IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_struct_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Points (id INT, point STRUCT(x INT, y INT))");

    ctx.exec(r#"INSERT INTO Points VALUES (1, '{"x": 10, "y": 20}')"#);
    ctx.exec(r#"INSERT INTO Points VALUES (2, '{"x": 30, "y": 40}')"#);
    ctx.exec(r#"INSERT INTO Points VALUES (3, '{"x": 10, "y": 20}')"#); // Duplicate

    // Structs can be compared for equality
    let results =
        ctx.query(r#"SELECT id FROM Points WHERE point = '{"x": 10, "y": 20}' ORDER BY id"#);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_struct_in_array() {
    let mut ctx = setup_test();

    // Array of structs
    ctx.exec(
        "CREATE TABLE Events (
        id INT,
        participants LIST
    )",
    );

    ctx.exec(
        r#"INSERT INTO Events VALUES (1, '[
        {"name": "Alice", "role": "Speaker"},
        {"name": "Bob", "role": "Organizer"}
    ]')"#,
    );

    let results = ctx.query("SELECT participants FROM Events");
    assert_eq!(results.len(), 1);

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
fn test_struct_update() {
    let mut ctx = setup_test();

    ctx.exec(
        "CREATE TABLE Users (id INT PRIMARY KEY, profile STRUCT(name VARCHAR, status VARCHAR))",
    );

    ctx.exec(r#"INSERT INTO Users VALUES (1, '{"name": "Alice", "status": "active"}')"#);
    ctx.exec(r#"INSERT INTO Users VALUES (2, '{"name": "Bob", "status": "active"}')"#);

    // Update struct value
    ctx.exec(
        r#"UPDATE Users SET profile = '{"name": "Alice", "status": "inactive"}' WHERE id = 1"#,
    );

    let results = ctx.query("SELECT profile FROM Users WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("profile").unwrap().contains("Struct"));

    ctx.commit();
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
#[should_panic(expected = "StructFieldMissing")]
fn test_insert_struct_missing_field_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Users (id INT, profile STRUCT(name VARCHAR, age INT, email VARCHAR))");

    // Missing 'email' field
    ctx.exec(r#"INSERT INTO Users VALUES (1, '{"name": "Alice", "age": 30}')"#);
}

#[test]
#[ignore = "STRUCT type not yet implemented"]
#[should_panic(expected = "StructFieldTypeMismatch")]
fn test_insert_struct_wrong_type_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Users (id INT, profile STRUCT(name VARCHAR, age INT))");

    // 'age' should be INT, not string
    ctx.exec(r#"INSERT INTO Users VALUES (1, '{"name": "Alice", "age": "thirty"}')"#);
}

#[test]
#[ignore = "GROUP BY with STRUCT not yet implemented"]
fn test_group_by_struct() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Orders (id INT, customer STRUCT(name VARCHAR, city VARCHAR))");

    ctx.exec(r#"INSERT INTO Orders VALUES (1, '{"name": "Alice", "city": "NYC"}')"#);
    ctx.exec(r#"INSERT INTO Orders VALUES (2, '{"name": "Bob", "city": "LA"}')"#);
    ctx.exec(r#"INSERT INTO Orders VALUES (3, '{"name": "Alice", "city": "NYC"}')"#); // Duplicate

    // GROUP BY struct column
    let results =
        ctx.query("SELECT customer, COUNT(*) as cnt FROM Orders GROUP BY customer ORDER BY cnt");
    assert_eq!(results.len(), 2);

    ctx.commit();
}
