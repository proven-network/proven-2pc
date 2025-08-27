# ToyDB Fork: Distributed SQL Engine with Pessimistic Concurrency Control

## Executive Summary

This document outlines the design and implementation of a distributed SQL engine built by forking ToyDB. The engine replaces ToyDB's MVCC with Pessimistic Concurrency Control (PCC) using wound-wait deadlock prevention, integrates with an existing Raft consensus layer, and provides full lock visibility for distributed transaction coordination.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Design Goals](#design-goals)
- [System Components](#system-components)
- [Concurrency Control](#concurrency-control)
- [SQL Engine](#sql-engine)
- [Raft Integration](#raft-integration)
- [Performance Considerations](#performance-considerations)
- [Implementation Plan](#implementation-plan)
- [Testing Strategy](#testing-strategy)
- [Risks and Mitigations](#risks-and-mitigations)

## Architecture Overview

### High-Level Architecture

```
┌────────────────────────────────────────────────────┐
│                  Client Layer                      │
│         (Distributed Transaction Coordinator)      │
└────────────────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ SQL Stream 1 │ │ SQL Stream 2 │ │  KV Stream   │
│   (Users)    │ │  (Orders)    │ │   (Cache)    │
└──────────────┘ └──────────────┘ └──────────────┘
        │                │                │
        ▼                ▼                ▼
┌──────────────────────────────────────────────────┐
│              Raft Consensus Layer                │
│         (Existing - Not Part of Fork)            │
└──────────────────────────────────────────────────┘
        │                │                │
        ▼                ▼                ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  SQL Engine  │ │  SQL Engine  │ │  KV Engine   │
│   Instance   │ │   Instance   │ │   Instance   │
└──────────────┘ └──────────────┘ └──────────────┘
```

### SQL Engine Internal Architecture

```
┌─────────────────────────────────────────────────────┐
│                   SQL Engine                        │
├─────────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────────┐  │
│  │            Raft State Machine Interface       │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │               SQL Parser (ToyDB)              │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │         Lock Manager (New Component)          │  │
│  │  • Row-level locks                            │  │
│  │  • Wound-wait deadlock prevention             │  │
│  │  • Lock visibility API                        │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │          Query Executor (Modified)            │  │
│  │  • Lock acquisition before execution          │  │
│  │  • Deterministic execution                    │  │
│  └───────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────┐  │
│  │         Storage Engine (Simplified)           │  │
│  │  • Single-version storage (no MVCC)           │  │
│  │  • In-memory B-trees                          │  │
│  └───────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## Design Goals

### Primary Goals

1. **Strict Serializability**: Ensure all operations maintain strict serializability through pessimistic locking
2. **Distributed Coordination**: Support transactions spanning multiple Raft streams
3. **Lock Visibility**: Provide complete visibility into lock state for debugging and coordination
4. **Deadlock Prevention**: Use wound-wait to guarantee no deadlocks
5. **Deterministic Execution**: Ensure identical execution across all replicas

### Non-Goals

1. **Persistent Storage**: Storage durability is handled by Raft log
2. **Query Optimization**: Complex query optimization (can be added later)
3. **Full SQL Compliance**: Support only essential SQL subset initially
4. **Cross-Region Replication**: Focus on single-region deployment

## System Components

### Lock Manager

#### Data Structures

```rust
pub struct LockManager {
    /// All currently held locks
    locks: RwLock<HashMap<LockKey, LockInfo>>,
    
    /// Transaction states for wound-wait
    transactions: RwLock<HashMap<TxId, TransactionState>>,
    
    /// Wait queue for blocked transactions
    wait_queue: RwLock<HashMap<LockKey, Vec<Waiter>>>,
    
    /// Statistics for monitoring
    stats: LockStatistics,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LockKey {
    /// Row-level lock
    Row { table: String, row_id: u64 },
    
    /// Range lock for scans
    Range { table: String, start: u64, end: u64 },
    
    /// Table-level lock
    Table { table: String },
    
    /// Schema lock for DDL
    Schema,
}

pub struct LockInfo {
    pub holder: TxId,
    pub priority: Priority,
    pub mode: LockMode,
    pub acquired_at: LogicalTimestamp,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LockMode {
    Shared,     // Multiple readers
    Exclusive,  // Single writer
    IntentShared,    // Intent to acquire shared locks on children
    IntentExclusive, // Intent to acquire exclusive locks on children
}
```

#### Lock Compatibility Matrix

| Requested → <br> Held ↓ | S | X | IS | IX |
|-------------------------|---|---|----|----|
| **S** (Shared)          | ✓ | ✗ | ✓  | ✗  |
| **X** (Exclusive)       | ✗ | ✗ | ✗  | ✗  |
| **IS** (Intent Shared)  | ✓ | ✗ | ✓  | ✓  |
| **IX** (Intent Excl.)   | ✗ | ✗ | ✓  | ✓  |

#### Wound-Wait Algorithm

```rust
impl LockManager {
    pub fn try_acquire(
        &self,
        tx: TxId,
        priority: Priority,
        key: LockKey,
        mode: LockMode,
    ) -> Result<LockResult> {
        let mut locks = self.locks.write();
        
        // Check for conflicts
        if let Some(holder) = locks.get(&key) {
            if !self.is_compatible(holder.mode, mode) {
                if priority < holder.priority {
                    // Wound: Abort the holder
                    return Ok(LockResult::ShouldWound(holder.holder));
                } else {
                    // Wait: Block the requester
                    return Ok(LockResult::MustWait);
                }
            }
        }
        
        // Grant lock
        locks.insert(key, LockInfo {
            holder: tx,
            priority,
            mode,
            acquired_at: self.clock.now(),
        });
        
        Ok(LockResult::Granted)
    }
}
```

### Storage Engine

#### Simplified Single-Version Storage

```rust
pub struct Storage {
    /// In-memory tables
    tables: HashMap<String, Table>,
    
    /// Next row ID for each table (deterministic)
    next_ids: HashMap<String, u64>,
}

pub struct Table {
    /// Schema definition
    schema: Schema,
    
    /// Row storage using B-tree for ordered scans
    rows: BTreeMap<u64, Row>,
    
    /// Secondary indexes
    indexes: HashMap<String, Index>,
}

pub struct Row {
    /// Primary key
    id: u64,
    
    /// Column values
    values: Vec<Value>,
    
    /// Deleted flag (for soft deletes)
    deleted: bool,
}

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Decimal(Decimal),  // Using rust_decimal for determinism
    String(String),
    Timestamp(LogicalTimestamp),  // Logical, not wall clock
}
```

### Query Executor

#### Lock Analysis Phase

```rust
pub struct LockAnalyzer {
    catalog: Catalog,
    statistics: TableStatistics,
}

impl LockAnalyzer {
    pub fn analyze(&self, stmt: &Statement) -> Result<Vec<LockRequest>> {
        match stmt {
            Statement::Select(select) => self.analyze_select(select),
            Statement::Insert(insert) => self.analyze_insert(insert),
            Statement::Update(update) => self.analyze_update(update),
            Statement::Delete(delete) => self.analyze_delete(delete),
        }
    }
    
    fn analyze_select(&self, select: &SelectStatement) -> Result<Vec<LockRequest>> {
        let mut requests = Vec::new();
        
        // Analyze WHERE clause to determine row access
        if let Some(predicate) = &select.where_clause {
            match self.analyze_predicate(&select.from, predicate)? {
                AccessPattern::Point(ids) => {
                    // Lock specific rows
                    for id in ids {
                        requests.push(LockRequest {
                            key: LockKey::Row { 
                                table: select.from.clone(), 
                                row_id: id 
                            },
                            mode: LockMode::Shared,
                        });
                    }
                }
                AccessPattern::Range(start, end) => {
                    // Lock range
                    requests.push(LockRequest {
                        key: LockKey::Range { 
                            table: select.from.clone(), 
                            start, 
                            end 
                        },
                        mode: LockMode::Shared,
                    });
                }
                AccessPattern::Scan => {
                    // Lock entire table
                    requests.push(LockRequest {
                        key: LockKey::Table { 
                            table: select.from.clone() 
                        },
                        mode: LockMode::Shared,
                    });
                }
            }
        }
        
        Ok(requests)
    }
}
```

### Transaction Manager

```rust
pub struct Transaction {
    /// Unique transaction ID
    pub id: TxId,
    
    /// Priority for wound-wait
    pub priority: Priority,
    
    /// Current state
    pub state: TransactionState,
    
    /// Locks held by this transaction
    locks_held: Vec<LockKey>,
    
    /// Reference to lock manager
    lock_manager: Arc<LockManager>,
    
    /// Reference to storage
    storage: Arc<RwLock<Storage>>,
    
    /// Track access for distributed coordination
    pub access_log: Vec<AccessLogEntry>,
}

#[derive(Clone, Debug)]
pub enum TransactionState {
    Active,
    Preparing,  // For 2PC
    Committed,
    Aborted,
}

impl Transaction {
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        // Parse SQL
        let stmt = parse_sql(sql)?;
        
        // Validate determinism
        validate_deterministic(&stmt)?;
        
        // Analyze required locks
        let lock_requests = self.analyze_locks(&stmt)?;
        
        // Acquire all locks with wound-wait
        for request in lock_requests {
            self.acquire_lock_with_wound_wait(request)?;
        }
        
        // Execute with locks held
        let result = self.execute_statement(&stmt)?;
        
        // Log access for distributed coordination
        self.log_access(&stmt, &result)?;
        
        Ok(result)
    }
    
    fn acquire_lock_with_wound_wait(&mut self, request: LockRequest) -> Result<()> {
        loop {
            match self.lock_manager.try_acquire(
                self.id,
                self.priority,
                request.key.clone(),
                request.mode,
            )? {
                LockResult::Granted => {
                    self.locks_held.push(request.key);
                    return Ok(());
                }
                LockResult::ShouldWound(victim) => {
                    // Request abort of victim transaction
                    self.lock_manager.abort_transaction(victim)?;
                    // Retry lock acquisition
                    continue;
                }
                LockResult::MustWait => {
                    return Err(Error::WouldBlock);
                }
            }
        }
    }
}
```

## Concurrency Control

### Pessimistic Concurrency Control (PCC)

Unlike ToyDB's MVCC, we use pessimistic locking:

1. **Lock Before Access**: All data access requires prior lock acquisition
2. **No Versions**: Single version per row, no snapshot isolation
3. **Immediate Conflict Detection**: Conflicts detected at lock time, not commit time

### Wound-Wait Deadlock Prevention

```
Transaction T1 (priority=100) holds lock on A, wants B
Transaction T2 (priority=200) holds lock on B, wants A

Wound-Wait Decision:
- T1 has higher priority (100 < 200)
- T1 "wounds" (aborts) T2
- T1 acquires lock on B
- T2 must restart with same priority
```

### Lock Escalation

```rust
const LOCK_ESCALATION_THRESHOLD: usize = 100;

impl LockManager {
    fn maybe_escalate(&mut self, table: &str, row_locks: Vec<u64>) -> LockKey {
        if row_locks.len() > LOCK_ESCALATION_THRESHOLD {
            // Escalate to table lock
            LockKey::Table { table: table.to_string() }
        } else {
            // Keep row locks
            LockKey::Multiple(
                row_locks.into_iter()
                    .map(|id| LockKey::Row { 
                        table: table.to_string(), 
                        row_id: id 
                    })
                    .collect()
            )
        }
    }
}
```

## SQL Engine

### Supported SQL Subset

#### Phase 1 (MVP)
```sql
-- Basic CRUD
SELECT * FROM table WHERE condition;
INSERT INTO table (columns) VALUES (values);
UPDATE table SET column = value WHERE condition;
DELETE FROM table WHERE condition;

-- Simple predicates
WHERE id = ? AND status = ?;
WHERE age BETWEEN ? AND ?;
WHERE name LIKE 'prefix%';
WHERE status IN ('A', 'B', 'C');

-- Ordering and limits
ORDER BY column [ASC|DESC];
LIMIT n;
```

#### Phase 2 (Post-MVP)
```sql
-- Aggregations
SELECT COUNT(*), SUM(amount), AVG(price) FROM table;
SELECT column, COUNT(*) FROM table GROUP BY column;
SELECT column, SUM(amount) FROM table GROUP BY column HAVING SUM(amount) > ?;

-- Simple joins
SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id;
SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id;

-- Transactions
BEGIN;
COMMIT;
ROLLBACK;
```

### Deterministic Execution

#### Forbidden Functions
```rust
const FORBIDDEN_FUNCTIONS: &[&str] = &[
    "RANDOM", "RAND", "UUID", "NOW", "CURRENT_TIMESTAMP",
    "CURRENT_DATE", "CURRENT_TIME", "USER", "DATABASE"
];

fn validate_deterministic(stmt: &Statement) -> Result<()> {
    let mut visitor = NonDeterministicVisitor::default();
    stmt.accept(&mut visitor)?;
    
    if !visitor.violations.is_empty() {
        return Err(Error::NonDeterministicSQL(visitor.violations));
    }
    
    Ok(())
}
```

#### Deterministic Alternatives
```rust
impl DeterministicFunctions {
    /// Replace NOW() with logical timestamp
    fn now(&self, raft_index: u64) -> Timestamp {
        Timestamp::from_raft_index(raft_index)
    }
    
    /// Replace RANDOM() with deterministic pseudo-random
    fn random(&mut self, seed: u64) -> f64 {
        self.prng.next(seed)
    }
    
    /// Replace AUTO_INCREMENT with deterministic counter
    fn next_id(&mut self, table: &str) -> u64 {
        let counter = self.counters.entry(table.to_string()).or_insert(0);
        *counter += 1;
        *counter
    }
}
```

### Type System

```rust
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    // Numeric types
    Integer,      // i64
    Decimal(u8, u8),  // precision, scale
    
    // String types
    String,       // Variable length
    
    // Other
    Boolean,
    Timestamp,    // Logical timestamp
    
    // Nullable variants
    Nullable(Box<DataType>),
}

impl Value {
    /// Type checking without coercion
    pub fn check_type(&self, expected: &DataType) -> Result<()> {
        match (self, expected) {
            (Value::Null, DataType::Nullable(_)) => Ok(()),
            (Value::Integer(_), DataType::Integer) => Ok(()),
            (Value::Decimal(_), DataType::Decimal(_, _)) => Ok(()),
            (Value::String(_), DataType::String) => Ok(()),
            (Value::Boolean(_), DataType::Boolean) => Ok(()),
            _ => Err(Error::TypeMismatch),
        }
    }
}
```

## Raft Integration

### State Machine Interface

```rust
pub struct SqlStateMachine {
    engine: SqlEngine,
    lock_manager: Arc<LockManager>,
}

impl RaftStateMachine for SqlStateMachine {
    type Input = Operation;
    type Output = OperationResult;
    
    fn apply(&mut self, input: Self::Input) -> Self::Output {
        match input {
            Operation::Execute { tx_id, priority, sql } => {
                self.execute_sql(tx_id, priority, sql)
            }
            Operation::BeginTransaction { tx_id, priority } => {
                self.begin_transaction(tx_id, priority)
            }
            Operation::Commit { tx_id } => {
                self.commit_transaction(tx_id)
            }
            Operation::Abort { tx_id } => {
                self.abort_transaction(tx_id)
            }
        }
    }
    
    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&self.engine).unwrap()
    }
    
    fn restore(&mut self, snapshot: Vec<u8>) {
        self.engine = bincode::deserialize(&snapshot).unwrap();
    }
}
```

### Operation Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Execute SQL statement
    Execute {
        tx_id: TxId,
        priority: Priority,
        sql: String,
    },
    
    /// Begin new transaction
    BeginTransaction {
        tx_id: TxId,
        priority: Priority,
    },
    
    /// Commit transaction
    Commit {
        tx_id: TxId,
    },
    
    /// Abort transaction (from wound-wait or error)
    Abort {
        tx_id: TxId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationResult {
    Success(QueryResult),
    Error(ErrorKind),
    WouldBlock,  // For distributed coordination
}
```

## Performance Considerations

### Memory Layout

```rust
/// Cache-friendly row storage
#[repr(C)]
pub struct CompactRow {
    id: u64,
    flags: u8,  // deleted, locked, etc.
    _padding: [u8; 7],
    data: [u8; 256],  // Inline storage for small rows
}

/// Column-oriented storage for analytics
pub struct ColumnarTable {
    ids: Vec<u64>,
    columns: Vec<ColumnVector>,
}

enum ColumnVector {
    Integer(Vec<i64>),
    Decimal(Vec<Decimal>),
    String(StringColumn),
}
```

### Lock Optimization

```rust
/// Hierarchical locking to reduce lock manager overhead
impl LockManager {
    fn acquire_hierarchical(&mut self, request: LockRequest) -> Result<()> {
        match request.key {
            LockKey::Row { table, row_id } => {
                // First acquire intent lock on table
                self.acquire(LockKey::Table { table }, LockMode::IntentShared)?;
                // Then acquire actual row lock
                self.acquire(LockKey::Row { table, row_id }, request.mode)?;
            }
            _ => self.acquire(request.key, request.mode),
        }
    }
}
```

### SIMD Optimization (Future)

```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Vectorized sum for aggregations
unsafe fn sum_i64_avx2(data: &[i64]) -> i64 {
    let mut sum = _mm256_setzero_si256();
    
    for chunk in data.chunks_exact(4) {
        let values = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        sum = _mm256_add_epi64(sum, values);
    }
    
    // Horizontal sum
    let sum_array = std::mem::transmute::<__m256i, [i64; 4]>(sum);
    sum_array.iter().sum()
}
```

## Implementation Plan

### Week 1-2: Foundation
- [ ] Fork ToyDB repository
- [ ] Remove MVCC implementation
- [ ] Remove Raft implementation
- [ ] Simplify storage to single-version
- [ ] Set up basic project structure

### Week 3-4: Lock Manager
- [ ] Implement lock data structures
- [ ] Implement wound-wait algorithm
- [ ] Add lock compatibility checking
- [ ] Create lock visibility API
- [ ] Write lock manager tests

### Week 5-6: Storage & Transactions
- [ ] Simplify storage engine
- [ ] Implement deterministic ID generation
- [ ] Create transaction manager
- [ ] Integrate lock manager with transactions
- [ ] Add automatic lock release

### Week 7-8: Query Execution
- [ ] Implement lock analysis
- [ ] Modify executor for lock acquisition
- [ ] Ensure deterministic execution
- [ ] Handle lock escalation
- [ ] Test concurrent queries

### Week 9-10: Raft Integration
- [ ] Create state machine interface
- [ ] Define operation types
- [ ] Implement apply/snapshot/restore
- [ ] Test deterministic replay
- [ ] Integrate with existing Raft

### Week 11-12: Testing & Optimization
- [ ] Comprehensive testing
- [ ] Performance benchmarking
- [ ] Memory optimization
- [ ] Documentation
- [ ] Integration testing

## Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_wound_wait() {
        let manager = LockManager::new();
        
        // T1 acquires lock
        assert_eq!(
            manager.try_acquire(tx1, 100, key, Exclusive),
            Ok(LockResult::Granted)
        );
        
        // T2 with lower priority wounds T1
        assert_eq!(
            manager.try_acquire(tx2, 50, key, Exclusive),
            Ok(LockResult::ShouldWound(tx1))
        );
    }
    
    #[test]
    fn test_deterministic_execution() {
        let mut engine1 = SqlEngine::new();
        let mut engine2 = SqlEngine::new();
        
        let op = Operation::Execute {
            tx_id: 1,
            priority: 100,
            sql: "INSERT INTO users VALUES (DEFAULT, 'Alice')".into(),
        };
        
        assert_eq!(engine1.apply(op.clone()), engine2.apply(op));
    }
}
```

### Integration Tests
```rust
#[test]
fn test_concurrent_transactions() {
    let engine = Arc::new(SqlEngine::new());
    
    let handles: Vec<_> = (0..10).map(|i| {
        let engine = engine.clone();
        thread::spawn(move || {
            engine.execute(
                TxId(i),
                Priority(i * 10),
                "UPDATE accounts SET balance = balance + 1"
            )
        })
    }).collect();
    
    for h in handles {
        h.join().unwrap();
    }
    
    // Verify consistency
    let result = engine.query("SELECT SUM(balance) FROM accounts");
    assert_eq!(result, 10);
}
```

### Chaos Testing
- Random transaction aborts
- Network partitions during 2PC
- Node failures and recovery
- Lock manager stress testing

## Risks and Mitigations

### Risk 1: Performance Regression
**Risk**: PCC may be slower than MVCC for read-heavy workloads  
**Mitigation**: 
- Implement shared locks for readers
- Add read-only transaction optimization
- Consider hybrid approach for analytics

### Risk 2: Lock Contention
**Risk**: High contention on hot rows  
**Mitigation**:
- Implement lock escalation
- Add timeout mechanisms
- Monitor and alert on contention

### Risk 3: Non-Deterministic SQL
**Risk**: Accidental non-determinism breaks consensus  
**Mitigation**:
- Strict SQL validation
- Comprehensive testing
- Clear documentation

### Risk 4: Memory Usage
**Risk**: Lock manager memory overhead  
**Mitigation**:
- Implement lock escalation
- Add memory limits
- Use compact lock representations

## Success Metrics

1. **Functional**:
   - All tests pass
   - Deterministic execution verified
   - No deadlocks detected

2. **Performance**:
   - 100k+ transactions/second (simple queries)
   - <1ms p50 latency
   - <10ms p99 latency under contention

3. **Scale**:
   - Support 1000+ concurrent transactions
   - Handle 1M+ rows per table
   - Manage 10k+ locks efficiently

## Appendix A: Removed ToyDB Components

### Components to Remove
- `src/raft/` - Entire Raft implementation
- `src/storage/mvcc.rs` - MVCC logic
- `src/storage/log.rs` - Transaction log
- `src/network/` - Network layer
- Version-related types and functions

### Components to Keep
- `src/sql/parser.rs` - SQL parser
- `src/sql/types.rs` - Basic SQL types
- `src/catalog/` - Table catalog
- Test framework (modified)

## Appendix B: API Examples

### Lock Visibility API
```rust
// Query current locks
GET /locks
{
  "locks": [
    {
      "key": {"type": "row", "table": "users", "id": 42},
      "holder": "tx-123",
      "mode": "exclusive",
      "priority": 100,
      "held_for_ms": 150
    }
  ]
}

// Query transaction state
GET /transactions
{
  "transactions": [
    {
      "id": "tx-123",
      "state": "active",
      "priority": 100,
      "locks_held": 3,
      "started_at": "2024-01-01T12:00:00Z"
    }
  ]
}
```

### SQL Extensions
```sql
-- Show locks (debugging)
SHOW LOCKS;

-- Show transactions
SHOW TRANSACTIONS;

-- Explain locks for query
EXPLAIN LOCKS SELECT * FROM users WHERE age > 30;
```

## Appendix C: Configuration

```yaml
# config.yaml
sql_engine:
  # Lock manager settings
  lock_escalation_threshold: 100
  lock_timeout_ms: 5000
  max_locks_per_transaction: 1000
  
  # Storage settings  
  max_tables: 100
  max_rows_per_table: 10000000
  
  # Transaction settings
  max_concurrent_transactions: 1000
  default_transaction_priority: 1000
  
  # Performance settings
  enable_simd: false  # Experimental
  use_decimal_math: true  # For determinism
```

## Conclusion

This design provides a solid foundation for building a distributed SQL engine with pessimistic concurrency control. By forking ToyDB and replacing MVCC with PCC, we can achieve:

1. Full lock visibility for distributed coordination
2. Deadlock-free operation via wound-wait
3. Deterministic execution for Raft consensus
4. Strong consistency guarantees

The implementation plan provides a clear path forward with measurable milestones and success criteria.
