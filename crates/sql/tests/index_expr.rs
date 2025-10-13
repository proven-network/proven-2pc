//! Index expression tests
//! Based on gluesql/test-suite/src/index/expr.rs

mod common;
use common::setup_test;
use proven_value::Value;
#[test]
fn test_create_simple_column_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (2, 3, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, 4, 'Test')");

    // Create basic column index
    ctx.exec("CREATE INDEX idx_id ON Test (id)");

    // Verify index can be used for queries
    let results = ctx.query("SELECT * FROM Test WHERE id = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_create_arithmetic_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER, name TEXT)");

    // Insert data
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (1, 2, 'Hello')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (2, 3, 'World')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (3, 4, 'Test')");
    ctx.exec("INSERT INTO Test (id, num, name) VALUES (4, NULL, 'Null')");

    // Create expression index on id + num
    ctx.exec("CREATE INDEX idx_sum ON Test (id + num)");

    // Test ORDER BY using the expression index
    let results = ctx.query("SELECT * FROM Test ORDER BY id + num");
    assert_eq!(results.len(), 4);
    // 1+2=3, 2+3=5, 3+4=7, 4+NULL=NULL (sorts last)
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(4)); // NULL result sorts last

    ctx.commit();
}

#[test]
fn test_create_multiplication_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Test VALUES (2, 3)");
    ctx.exec("INSERT INTO Test VALUES (4, 5)");
    ctx.exec("INSERT INTO Test VALUES (1, 10)");

    // Create expression index on id * num
    ctx.exec("CREATE INDEX idx_product ON Test (id * num)");

    // Test ORDER BY using the expression index
    let results = ctx.query("SELECT id, num, id * num as product FROM Test ORDER BY id * num");
    assert_eq!(results.len(), 3);
    // 2*3=6, 1*10=10, 4*5=20
    assert_eq!(results[0].get("product").unwrap(), &Value::I32(6));
    assert_eq!(results[1].get("product").unwrap(), &Value::I32(10));
    assert_eq!(results[2].get("product").unwrap(), &Value::I32(20));

    ctx.commit();
}

#[test]
#[ignore = "Unary operators in index expressions not yet implemented"]
fn test_create_unary_operation_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Test VALUES (1, 5)");

    // Try to create expression index on -num (unary minus)
    // This should fail as unary operators aren't supported yet
    ctx.exec("CREATE INDEX idx_neg ON Test (-num)");
}

#[test]
fn test_parenthesized_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Test VALUES (1, 2)");
    ctx.exec("INSERT INTO Test VALUES (2, 3)");

    // Create index with parenthesized expression
    ctx.exec("CREATE INDEX idx_paren ON Test ((id))");

    // Verify it works like a regular column index
    let results = ctx.query("SELECT * FROM Test WHERE id = 2");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_complex_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (a INTEGER, b INTEGER, c INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Test VALUES (1, 2, 3)");
    ctx.exec("INSERT INTO Test VALUES (2, 3, 4)");
    ctx.exec("INSERT INTO Test VALUES (3, 4, 5)");

    // Create index with complex expression
    ctx.exec("CREATE INDEX idx_complex ON Test ((a + b) * c)");

    // Test ORDER BY using the complex expression
    let results = ctx.query("SELECT a, b, c, (a + b) * c as result FROM Test ORDER BY (a + b) * c");
    assert_eq!(results.len(), 3);
    // (1+2)*3=9, (2+3)*4=20, (3+4)*5=35
    assert_eq!(results[0].get("result").unwrap(), &Value::I32(9));
    assert_eq!(results[1].get("result").unwrap(), &Value::I32(20));
    assert_eq!(results[2].get("result").unwrap(), &Value::I32(35));

    ctx.commit();
}

#[test]
fn test_expression_index_with_asc_desc() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");

    // Insert data
    ctx.exec("INSERT INTO Test VALUES (1, 5)");
    ctx.exec("INSERT INTO Test VALUES (2, 3)");
    ctx.exec("INSERT INTO Test VALUES (3, 7)");

    // Create expression index with DESC
    ctx.exec("CREATE INDEX idx_sum_desc ON Test (id + num DESC)");

    // Test ORDER BY DESC using the expression index
    let results = ctx.query("SELECT id, num, id + num as sum FROM Test ORDER BY id + num DESC");
    assert_eq!(results.len(), 3);
    // 3+7=10, 1+5=6, 2+3=5
    assert_eq!(results[0].get("sum").unwrap(), &Value::I32(10));
    assert_eq!(results[1].get("sum").unwrap(), &Value::I32(6));
    assert_eq!(results[2].get("sum").unwrap(), &Value::I32(5));

    ctx.commit();
}

#[test]
fn test_drop_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER)");

    // Create expression index
    ctx.exec("CREATE INDEX idx_expr ON Test (id + num)");

    // Drop the index
    ctx.exec("DROP INDEX idx_expr");

    // Try to drop again - should fail because index no longer exists
    let error = ctx.exec_error("DROP INDEX idx_expr");
    assert!(error.contains("IndexNotFound"));
}

#[test]
fn test_expression_index_with_nulls() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, num INTEGER NULL)");

    // Insert data including NULLs
    ctx.exec("INSERT INTO Test VALUES (1, 2)");
    ctx.exec("INSERT INTO Test VALUES (2, NULL)");
    ctx.exec("INSERT INTO Test VALUES (3, 4)");
    ctx.exec("INSERT INTO Test VALUES (4, NULL)");

    // Create expression index
    ctx.exec("CREATE INDEX idx_expr_null ON Test (id + num)");

    // Test ORDER BY with NULLs
    let results = ctx.query("SELECT id, num FROM Test ORDER BY id + num");
    assert_eq!(results.len(), 4);
    // Non-NULL results first: 1+2=3, 3+4=7, then NULLs
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));
    // NULL results sort last
    assert_eq!(results[2].get("num").unwrap(), &Value::Null);
    assert_eq!(results[3].get("num").unwrap(), &Value::Null);

    ctx.commit();
}

#[test]
#[ignore = "String concatenation operator (||) in index expressions not yet implemented"]
fn test_string_concatenation_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, name TEXT)");
    ctx.exec("INSERT INTO Test VALUES (1, 'Hello')");

    // Try to create index with string concatenation (|| operator)
    // This should fail as the || operator isn't parsed yet
    ctx.exec("CREATE INDEX idx_concat ON Test (id || name)");
}

#[test]
fn test_supported_expression_operations() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (a INTEGER, b INTEGER, c INTEGER)");
    ctx.exec("INSERT INTO Test VALUES (1, 2, 3)");
    ctx.exec("INSERT INTO Test VALUES (4, 5, 6)");

    // Test supported arithmetic operations
    // Addition
    ctx.exec("CREATE INDEX idx_add ON Test (a + b)");

    // Subtraction
    ctx.exec("CREATE INDEX idx_sub ON Test (a - b)");

    // Multiplication
    ctx.exec("CREATE INDEX idx_mul ON Test (a * b)");

    // Division
    ctx.exec("CREATE INDEX idx_div ON Test (a / b)");

    // Complex nested expressions
    ctx.exec("CREATE INDEX idx_nested ON Test ((a + b) * c)");

    // Verify we can query using these indexes
    let results = ctx.query("SELECT * FROM Test ORDER BY a + b");
    assert_eq!(results.len(), 2);

    ctx.commit();
}

#[test]
fn test_invalid_expression_index() {
    let mut ctx = setup_test();

    // Create table
    ctx.exec("CREATE TABLE Test (id INTEGER, name TEXT)");

    // Try to create index with invalid expression (should fail)
    // Using SELECT in index expression is not allowed
    let error = ctx.exec_error("CREATE INDEX idx_invalid ON Test ((SELECT 1))");
    // We should get an error about subqueries not being allowed
    assert!(
        error.contains("Subqueries are not allowed") || error.contains("Parse error"),
        "Expected error about subqueries, got: {}",
        error
    );
}
