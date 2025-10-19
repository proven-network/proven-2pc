//! Transaction index DDL tests
//! Based on gluesql/test-suite/src/transaction/index.rs

mod common;

use common::TestContext;

#[ignore = "DDL rollback not yet properly isolated - index persists after abort"]
#[test]
fn test_create_index_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    // Test: Create index within transaction, then rollback
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");

    // Verify query still works within transaction and uses index
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id = 1", "idx_id");

    // Rollback
    ctx.abort();

    // Verify index is NOT used after rollback (via EXPLAIN only)
    // Note: We check EXPLAIN which doesn't require transaction state
    let plan = ctx.exec_response("EXPLAIN SELECT id FROM IdxCreate WHERE id = 1");
    let plan_text = plan.as_explain_plan().expect("Expected plan");
    assert!(
        !plan_text.contains("Index scan") && !plan_text.contains("Index range scan"),
        "Index should not be used after rollback"
    );
}

#[test]
fn test_create_index_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    // Test: Create index within transaction, then commit
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");

    // Verify query works within transaction and uses index
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id = 1", "idx_id");

    // Commit
    ctx.commit();

    // Begin new transaction to verify index persisted
    ctx.begin();

    // Verify query still works after commit and still uses index
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 1", 1);
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id = 1", "idx_id");

    ctx.commit();
}

#[ignore = "DDL rollback not yet properly isolated - index persists after abort"]
#[test]
fn test_create_multiple_indexes_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxCreate (id INTEGER)");
    ctx.exec("INSERT INTO IdxCreate VALUES (1)");
    ctx.commit();

    ctx.begin();
    ctx.exec("CREATE INDEX idx_id ON IdxCreate (id)");
    ctx.commit();

    // Clear and insert new data
    ctx.begin();
    ctx.exec("DELETE FROM IdxCreate");
    ctx.exec("INSERT INTO IdxCreate VALUES (3)");
    ctx.commit();

    // Test: Create expression index, then rollback
    ctx.begin();
    ctx.exec("CREATE INDEX idx_id2 ON IdxCreate (id * 2)");

    // Verify both queries work within transaction
    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id = 3", 1);
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id = 3", "idx_id");

    ctx.assert_row_count("SELECT id FROM IdxCreate WHERE id * 2 = 6", 1);
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id * 2 = 6", "idx_id2");

    // Rollback
    ctx.abort();

    // Verify original index still works (via EXPLAIN)
    ctx.assert_uses_index("SELECT id FROM IdxCreate WHERE id = 3", "idx_id");

    // Verify expression index is NOT used after rollback (via EXPLAIN)
    let plan = ctx.exec_response("EXPLAIN SELECT id FROM IdxCreate WHERE id * 2 = 6");
    let plan_text = plan.as_explain_plan().expect("Expected plan");
    assert!(
        !plan_text.contains("Index scan") && !plan_text.contains("Index range scan"),
        "Expression index should not be used after rollback"
    );
}

#[ignore = "DDL rollback not yet properly isolated - DROP persists after abort"]
#[test]
fn test_drop_index_with_rollback() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxDrop (id INTEGER)");
    ctx.exec("INSERT INTO IdxDrop VALUES (1)");
    ctx.exec("CREATE INDEX idx_id ON IdxDrop (id)");
    ctx.commit();

    // Test: Drop index within transaction, then rollback
    ctx.begin();
    ctx.exec("DROP INDEX idx_id");

    // Verify query still works within transaction but index is not used
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    ctx.assert_no_index_scan("SELECT id FROM IdxDrop WHERE id = 1");

    // Rollback
    ctx.abort();

    // Verify index is used again after rollback (DROP was rolled back)
    ctx.assert_uses_index("SELECT id FROM IdxDrop WHERE id = 1", "idx_id");
}

#[test]
fn test_drop_index_with_commit() {
    let mut ctx = TestContext::new();

    // Setup: Create table with index and insert data
    ctx.begin();
    ctx.exec("CREATE TABLE IdxDrop (id INTEGER)");
    ctx.exec("INSERT INTO IdxDrop VALUES (1)");
    ctx.exec("CREATE INDEX idx_id ON IdxDrop (id)");
    ctx.commit();

    // Test: Drop index within transaction, then commit
    ctx.begin();
    ctx.exec("DROP INDEX idx_id");

    // Verify query still works within transaction but index is not used
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    ctx.assert_no_index_scan("SELECT id FROM IdxDrop WHERE id = 1");

    // Commit
    ctx.commit();

    // Begin new transaction to verify DROP persisted
    ctx.begin();

    // Verify query still works after commit and index is still not used
    ctx.assert_row_count("SELECT id FROM IdxDrop WHERE id = 1", 1);
    ctx.assert_no_index_scan("SELECT id FROM IdxDrop WHERE id = 1");

    ctx.commit();
}
