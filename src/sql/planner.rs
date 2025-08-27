//! Query planner with lock analysis for PCC
//!
//! The query planner analyzes SQL statements to:
//! 1. Determine which tables and rows will be accessed
//! 2. Identify required lock modes (shared vs exclusive)
//! 3. Order lock acquisitions to prevent deadlocks
//! 4. Generate an optimized execution plan

use crate::error::Result;
use crate::lock::LockMode;
use crate::sql::parser::ast;
use crate::sql::schema::Table;
use crate::transaction_id::TransactionContext;
use std::collections::{HashMap, HashSet};

/// A lock requirement for query execution
#[derive(Debug, Clone, PartialEq)]
pub struct LockRequirement {
    /// Table name
    pub table: String,
    /// Lock mode needed
    pub mode: LockMode,
    /// Specific row keys if known (None = table scan)
    pub keys: Option<Vec<Vec<u8>>>,
    /// Whether this is a predicate lock (for range queries)
    pub is_predicate: bool,
}

/// Execution plan for a query
#[derive(Debug)]
pub struct QueryPlan {
    /// The parsed SQL statement
    pub statement: ast::Statement,
    /// Required locks in acquisition order
    pub locks: Vec<LockRequirement>,
    /// Estimated rows to be read
    pub estimated_rows: Option<usize>,
    /// Whether results can be streamed
    pub streamable: bool,
}

/// Query planner analyzes statements and produces execution plans
pub struct QueryPlanner {
    /// Schema information
    schemas: HashMap<String, Table>,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }
    
    /// Register a table schema
    pub fn register_table(&mut self, table: Table) {
        self.schemas.insert(table.name.clone(), table);
    }
    
    /// Analyze a statement and produce an execution plan
    pub fn plan(&self, statement: ast::Statement, context: &TransactionContext) -> Result<QueryPlan> {
        let locks = self.analyze_locks(&statement, context)?;
        let streamable = self.is_streamable(&statement);
        
        Ok(QueryPlan {
            statement,
            locks,
            estimated_rows: None, // TODO: Add statistics
            streamable,
        })
    }
    
    /// Analyze which locks are needed for a statement
    fn analyze_locks(&self, statement: &ast::Statement, context: &TransactionContext) -> Result<Vec<LockRequirement>> {
        let mut locks = Vec::new();
        
        match statement {
            ast::Statement::Select { from, r#where, .. } => {
                // SELECT needs shared locks
                locks.extend(self.analyze_select_locks(from, r#where.as_ref(), context)?);
            }
            
            ast::Statement::Insert { table, .. } => {
                // INSERT needs exclusive lock on the table
                locks.push(LockRequirement {
                    table: table.clone(),
                    mode: LockMode::Exclusive,
                    keys: None, // New row, no specific key yet
                    is_predicate: false,
                });
            }
            
            ast::Statement::Update { table, r#where, .. } => {
                // UPDATE needs exclusive locks on affected rows
                // First, shared locks to find rows
                if let Some(where_clause) = r#where {
                    let keys = self.analyze_where_clause(table, where_clause)?;
                    locks.push(LockRequirement {
                        table: table.clone(),
                        mode: LockMode::Shared,
                        keys: keys.clone(),
                        is_predicate: keys.is_none(),
                    });
                }
                
                // Then exclusive locks to update
                locks.push(LockRequirement {
                    table: table.clone(),
                    mode: LockMode::Exclusive,
                    keys: None, // Will be determined during execution
                    is_predicate: false,
                });
            }
            
            ast::Statement::Delete { table, r#where } => {
                // DELETE needs exclusive locks on affected rows
                if let Some(where_clause) = r#where {
                    let keys = self.analyze_where_clause(table, where_clause)?;
                    locks.push(LockRequirement {
                        table: table.clone(),
                        mode: LockMode::Exclusive,
                        keys,
                        is_predicate: false,
                    });
                } else {
                    // DELETE without WHERE = truncate table
                    locks.push(LockRequirement {
                        table: table.clone(),
                        mode: LockMode::Exclusive,
                        keys: None,
                        is_predicate: false,
                    });
                }
            }
            
            ast::Statement::CreateTable { name, .. } => {
                // CREATE TABLE needs exclusive lock on schema
                // For now, we'll use a special "schema" table lock
                locks.push(LockRequirement {
                    table: "__schema__".to_string(),
                    mode: LockMode::Exclusive,
                    keys: Some(vec![name.as_bytes().to_vec()]),
                    is_predicate: false,
                });
            }
            
            ast::Statement::DropTable { name, .. } => {
                // DROP TABLE needs exclusive locks on both schema and table
                locks.push(LockRequirement {
                    table: "__schema__".to_string(),
                    mode: LockMode::Exclusive,
                    keys: Some(vec![name.as_bytes().to_vec()]),
                    is_predicate: false,
                });
                
                locks.push(LockRequirement {
                    table: name.clone(),
                    mode: LockMode::Exclusive,
                    keys: None,
                    is_predicate: false,
                });
            }
            
            ast::Statement::Begin { .. } | ast::Statement::Commit | ast::Statement::Rollback => {
                // Transaction control statements don't need locks
            }
            
            ast::Statement::Explain(_) => {
                // EXPLAIN doesn't need locks as it doesn't execute
            }
        }
        
        // Sort locks to prevent deadlocks
        // Order: by table name, then by lock mode (shared before exclusive)
        locks.sort_by(|a, b| {
            match a.table.cmp(&b.table) {
                std::cmp::Ordering::Equal => a.mode.cmp(&b.mode),
                other => other,
            }
        });
        
        Ok(locks)
    }
    
    /// Analyze SELECT statement locks
    fn analyze_select_locks(
        &self, 
        from_clauses: &[ast::FromClause], 
        where_clause: Option<&ast::Expression>,
        _context: &TransactionContext
    ) -> Result<Vec<LockRequirement>> {
        let mut locks = Vec::new();
        
        // Analyze FROM clauses
        for from in from_clauses {
            locks.extend(self.analyze_from_clause(from, where_clause)?);
        }
        
        Ok(locks)
    }
    
    /// Analyze a FROM clause for locks
    fn analyze_from_clause(
        &self,
        from: &ast::FromClause,
        where_clause: Option<&ast::Expression>,
    ) -> Result<Vec<LockRequirement>> {
        let mut locks = Vec::new();
        
        match from {
            ast::FromClause::Table { name, .. } => {
                let keys = if let Some(where_clause) = where_clause {
                    self.analyze_where_clause(name, where_clause)?
                } else {
                    None
                };
                
                locks.push(LockRequirement {
                    table: name.clone(),
                    mode: LockMode::Shared,
                    keys,
                    is_predicate: false,
                });
            }
            
            ast::FromClause::Join { left, right, .. } => {
                // Join needs locks on both tables
                locks.extend(self.analyze_from_clause(left, where_clause)?);
                locks.extend(self.analyze_from_clause(right, where_clause)?);
            }
        }
        
        Ok(locks)
    }
    
    /// Analyze WHERE clause to determine which keys to lock
    fn analyze_where_clause(&self, table: &str, where_expr: &ast::Expression) -> Result<Option<Vec<Vec<u8>>>> {
        // Simple analysis for now - look for primary key equality
        // TODO: Expand this to handle more complex predicates
        
        if let Some(schema) = self.schemas.get(table) {
            if let Some(pk_column) = schema.columns.get(schema.primary_key) {
                // Look for primary key equality: pk_col = value
                if self.is_pk_equality(where_expr, &pk_column.name) {
                    // For now, return None to indicate we found a PK predicate
                    // but can't extract the actual key value at planning time
                    // This will be determined during execution
                    return Ok(None);
                }
            }
        }
        
        // Can't determine specific keys - will need table scan
        Ok(None)
    }
    
    /// Check if expression is a primary key equality
    fn is_pk_equality(&self, expr: &ast::Expression, pk_name: &str) -> bool {
        match expr {
            ast::Expression::Operator(op) => match op {
                ast::Operator::Equal(left, right) => {
                    // Check if either side is the PK column
                    let left_is_pk = matches!(left.as_ref(), 
                        ast::Expression::Column(None, name) if name == pk_name);
                    let right_is_pk = matches!(right.as_ref(),
                        ast::Expression::Column(None, name) if name == pk_name);
                        
                    // Check if the other side is a constant
                    let left_is_const = matches!(left.as_ref(), ast::Expression::Literal(_));
                    let right_is_const = matches!(right.as_ref(), ast::Expression::Literal(_));
                    
                    (left_is_pk && right_is_const) || (right_is_pk && left_is_const)
                }
                
                ast::Operator::And(left, right) => {
                    // Check both sides of AND
                    self.is_pk_equality(left, pk_name) || self.is_pk_equality(right, pk_name)
                }
                
                _ => false,
            },
            _ => false,
        }
    }
    
    /// Check if a query can be streamed
    fn is_streamable(&self, statement: &ast::Statement) -> bool {
        match statement {
            ast::Statement::Select { order_by, group_by, .. } => {
                // Can stream if no ORDER BY or GROUP BY
                order_by.is_empty() && group_by.is_empty()
            }
            _ => false,
        }
    }
}

/// Lock analysis results for a specific query
#[derive(Debug)]
pub struct LockAnalysis {
    /// Tables that will be read
    pub read_tables: HashSet<String>,
    /// Tables that will be written
    pub write_tables: HashSet<String>,
    /// Whether the query uses predicates that might cause phantoms
    pub has_predicates: bool,
    /// Suggested lock acquisition order
    pub lock_order: Vec<String>,
}

impl LockAnalysis {
    /// Create lock analysis from a query plan
    pub fn from_plan(plan: &QueryPlan) -> Self {
        let mut read_tables = HashSet::new();
        let mut write_tables = HashSet::new();
        let mut has_predicates = false;
        
        for lock in &plan.locks {
            match lock.mode {
                LockMode::Shared | LockMode::IntentShared => {
                    read_tables.insert(lock.table.clone());
                }
                LockMode::Exclusive | LockMode::IntentExclusive => {
                    write_tables.insert(lock.table.clone());
                }
            }
            
            if lock.is_predicate {
                has_predicates = true;
            }
        }
        
        // Lock order is already sorted in the plan
        let lock_order: Vec<String> = plan.locks
            .iter()
            .map(|l| l.table.clone())
            .collect::<HashSet<_>>() // Deduplicate
            .into_iter()
            .collect();
        
        Self {
            read_tables,
            write_tables,
            has_predicates,
            lock_order,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::Parser;
    use crate::sql::schema::Column;
    use crate::types::DataType;
    
    fn test_planner() -> QueryPlanner {
        let mut planner = QueryPlanner::new();
        
        // Register a test table
        let table = Table::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer).primary_key(),
                Column::new("name".to_string(), DataType::String),
                Column::new("email".to_string(), DataType::String),
            ],
        ).unwrap();
        
        planner.register_table(table);
        planner
    }
    
    fn test_context() -> TransactionContext {
        use crate::hlc::{HlcTimestamp, NodeId};
        use crate::transaction_id::TransactionId;
        
        TransactionContext {
            tx_id: TransactionId {
                global_id: HlcTimestamp::new(1000, 0, NodeId::new(1)),
                sub_seq: 0,
            },
            read_only: false,
            isolation_level: crate::transaction_id::IsolationLevel::Serializable,
        }
    }
    
    #[test]
    fn test_select_locks() {
        let planner = test_planner();
        let ctx = test_context();
        
        // Simple SELECT
        let stmt = Parser::parse("SELECT * FROM users").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        assert_eq!(plan.locks.len(), 1);
        assert_eq!(plan.locks[0].table, "users");
        assert_eq!(plan.locks[0].mode, LockMode::Shared);
        assert!(plan.streamable);
        
        // SELECT with WHERE
        let stmt = Parser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        assert_eq!(plan.locks.len(), 1);
        assert_eq!(plan.locks[0].table, "users");
        assert_eq!(plan.locks[0].mode, LockMode::Shared);
    }
    
    #[test]
    fn test_insert_locks() {
        let planner = test_planner();
        let ctx = test_context();
        
        let stmt = Parser::parse("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        assert_eq!(plan.locks.len(), 1);
        assert_eq!(plan.locks[0].table, "users");
        assert_eq!(plan.locks[0].mode, LockMode::Exclusive);
        assert!(!plan.streamable);
    }
    
    #[test]
    fn test_update_locks() {
        let planner = test_planner();
        let ctx = test_context();
        
        let stmt = Parser::parse("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        // Should have both shared (for finding rows) and exclusive (for updating)
        assert_eq!(plan.locks.len(), 2);
        assert_eq!(plan.locks[0].mode, LockMode::Shared);
        assert_eq!(plan.locks[1].mode, LockMode::Exclusive);
    }
    
    #[test]
    fn test_delete_locks() {
        let planner = test_planner();
        let ctx = test_context();
        
        let stmt = Parser::parse("DELETE FROM users WHERE id = 1").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        assert_eq!(plan.locks.len(), 1);
        assert_eq!(plan.locks[0].table, "users");
        assert_eq!(plan.locks[0].mode, LockMode::Exclusive);
    }
    
    #[test]
    fn test_lock_ordering() {
        let mut planner = QueryPlanner::new();
        
        // Register multiple tables
        for table_name in &["table_a", "table_b", "table_c"] {
            let table = Table::new(
                table_name.to_string(),
                vec![Column::new("id".to_string(), DataType::Integer).primary_key()],
            ).unwrap();
            planner.register_table(table);
        }
        
        let ctx = test_context();
        
        // Query that would touch multiple tables (if we supported joins properly)
        let stmt = Parser::parse("SELECT * FROM table_b").unwrap();
        let plan = planner.plan(stmt, &ctx).unwrap();
        
        // Verify locks are ordered by table name
        for i in 1..plan.locks.len() {
            assert!(plan.locks[i-1].table <= plan.locks[i].table);
        }
    }
}