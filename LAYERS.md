# System Architecture Layers

This document describes the layered architecture of the Proven SQL engine, a distributed SQL system using Pessimistic Concurrency Control (PCC) with consensus-ordered operations.

## Overview

The system processes a consensus-ordered stream of SQL operations, where operations from different transactions may be interleaved. PCC with wound-wait deadlock prevention ensures consistency without needing MVCC-style snapshots.

## Layer Architecture

```
┌─────────────────────────────────────────┐
│         Layer 6: Client API             │ ← Async, streams results to clients
├─────────────────────────────────────────┤
│    Layer 5: Raft State Machine          │ ← Applies consensus operations sequentially
├─────────────────────────────────────────┤
│      Layer 4: SQL Execution             │ ← Parses SQL, plans queries, executes
├─────────────────────────────────────────┤
│    Layer 3: Transaction Manager         │ ← Coordinates locks with data access
├─────────────────────────────────────────┤
│       Layer 2: Lock Manager             │ ← Wound-wait deadlock prevention
├─────────────────────────────────────────┤
│       Layer 1: Storage Engine           │ ← Pure in-memory data structures
└─────────────────────────────────────────┘
```

## Layer 1: Storage Engine (`storage.rs`)

**Purpose:** Pure in-memory data structures with efficient access patterns.

### Responsibilities
- Maintain table schemas and data
- Provide CRUD operations on rows
- Manage secondary indexes
- Support efficient scanning and point lookups

### Key Design Decisions
- **No concurrency control** - Just data structures, locks handled above
- **No snapshots** - PCC provides isolation, not MVCC
- **Arc-wrapped rows** - For efficient result copying, not versioning
- **Single-version storage** - Only current state, no history
- **Synchronous API** - Returns iterators, not streams

### Interface
```rust
pub struct Table {
    rows: BTreeMap<u64, Arc<RowInner>>,  // Arc for cheap cloning
    indexes: HashMap<String, BTreeMap<Value, Vec<u64>>>,
}

impl Table {
    pub fn insert(&mut self, values: Vec<Value>) -> Result<u64>
    pub fn update(&mut self, id: u64, values: Vec<Value>) -> Result<()>
    pub fn delete(&mut self, id: u64) -> Result<()>
    pub fn scan(&self) -> impl Iterator<Item = (u64, &Row)>
}
```

## Layer 2: Lock Manager (`lock.rs`)

**Purpose:** Implement wound-wait deadlock prevention and track lock state.

### Responsibilities
- Track all locks held by transactions
- Implement wound-wait algorithm
- Maintain lock compatibility matrix
- Provide lock visibility for debugging

### Key Design Decisions
- **Wound-wait algorithm** - Older transactions wound younger ones
- **No automatic release** - Caller manages lock lifetime
- **Hierarchical locking** - Row, range, table, schema levels
- **Full visibility** - Can query all locks for debugging

### Lock Compatibility Matrix
| Mode | S | X | IS | IX |
|------|---|---|----|----|
| S    | ✓ | ✗ | ✓  | ✗  |
| X    | ✗ | ✗ | ✗  | ✗  |
| IS   | ✓ | ✗ | ✓  | ✓  |
| IX   | ✗ | ✗ | ✓  | ✓  |

### Interface
```rust
impl LockManager {
    pub fn try_acquire(&self, tx_id: TxId, priority: Priority, 
                       key: LockKey, mode: LockMode) -> Result<LockResult>
    pub fn release(&self, tx_id: TxId, key: LockKey) -> Result<()>
    pub fn release_all(&self, tx_id: TxId) -> Result<()>
}
```

## Layer 3: Transaction Manager (`transaction.rs`)

**Purpose:** Coordinate locks with data access and track transaction state.

### Responsibilities
- Enforce lock-before-access pattern
- Track transaction state (active, committed, aborted)
- Hold locks until commit/abort
- Manage transaction isolation

### Key Design Decisions
- **Eager locking** - Acquire locks before any data access
- **Hold until commit** - No early lock release
- **Single-threaded per transaction** - Operations execute sequentially
- **No read-your-writes problem** - Transaction sees its own changes immediately

### Interface
```rust
impl Transaction {
    pub fn read(&self, table: &str, row_id: u64) -> Result<Vec<Value>>
    pub fn write(&self, table: &str, row_id: u64, values: Vec<Value>) -> Result<()>
    pub fn scan(&self, table: &str) -> Result<Vec<(u64, Vec<Value>)>>
    pub fn commit(&self) -> Result<()>
    pub fn abort(&self) -> Result<()>
}
```

## Layer 4: SQL Execution (`sql.rs` - future)

**Purpose:** Parse SQL, plan queries, and execute operators.

### Responsibilities
- Parse SQL statements into AST
- Analyze required locks
- Plan query execution
- Execute operators (scan, filter, join, aggregate)
- Materialize results for consensus

### Key Design Decisions
- **Deterministic execution** - No random(), now(), etc.
- **Lock analysis phase** - Determine all locks before execution
- **Stream between operators** - But materialize final results
- **No parallelism** - Sequential execution for determinism

### Interface
```rust
impl QueryExecutor {
    pub fn execute(&self, tx: &Transaction, sql: &str) -> Result<QueryResult>
    pub fn analyze_locks(&self, sql: &str) -> Result<Vec<LockRequirement>>
}
```

## Layer 5: Raft State Machine (`raft.rs`)

**Purpose:** Apply consensus-ordered SQL operations deterministically.

### Responsibilities
- Apply operations in consensus order
- Maintain transaction-to-client mapping
- Store query results for retrieval
- Provide snapshots for recovery

### Key Design Decisions
- **Sequential application** - One operation at a time
- **No concurrent execution** - Even from different transactions
- **Result materialization** - Cannot stream during apply
- **Deterministic only** - Reject non-deterministic SQL

### Interface
```rust
impl RaftStateMachine for SqlStateMachine {
    fn apply(&mut self, op: Operation) -> OperationResult
    fn snapshot(&self) -> Vec<u8>
    fn restore(&mut self, snapshot: Vec<u8>)
}

enum Operation {
    Sql { client_id: ClientId, sql: String },
}
```

## Layer 6: Client API (future)

**Purpose:** Provide client interface for SQL submission and result retrieval.

### Responsibilities
- Submit SQL through consensus
- Retrieve and stream results
- Manage client sessions
- Optimize read-only queries

### Key Design Decisions
- **Async interface** - Clients wait for consensus
- **Separate result retrieval** - Results fetched after consensus
- **Streaming results** - Can stream materialized results to clients
- **Read-only optimization** - Might bypass consensus with read lease

### Interface
```rust
impl SqlClient {
    pub async fn execute(&self, sql: &str) -> Result<QueryId>
    pub fn get_results(&self, id: QueryId) -> impl Stream<Item = Row>
}
```

## Data Flow Example

```
1. Client A: "BEGIN"
   → Consensus orders as Op#1
   → State machine creates Transaction(id=1, priority=100)

2. Client B: "BEGIN"  
   → Consensus orders as Op#2
   → State machine creates Transaction(id=2, priority=200)

3. Client A: "SELECT * FROM users"
   → Consensus orders as Op#3
   → Transaction 1 acquires S lock on users
   → Executes scan, stores results

4. Client B: "SELECT * FROM users"
   → Consensus orders as Op#4
   → Transaction 2 acquires S lock on users (compatible)
   → Executes scan, stores results

5. Client A: "INSERT INTO users VALUES (...)"
   → Consensus orders as Op#5
   → Transaction 1 needs X lock on users
   → Conflicts with Transaction 2's S lock
   → Transaction 1 (priority 100) wounds Transaction 2
   → Transaction 2 aborted
   → Transaction 1 acquires X lock, performs insert

6. Client A: "COMMIT"
   → Consensus orders as Op#6
   → Transaction 1 commits, releases all locks

7. Client B: "COMMIT"
   → Consensus orders as Op#7
   → Fails - transaction already aborted
```

## Key Insights

1. **Consensus provides ordering, not isolation** - We still need PCC for isolation
2. **No snapshots needed** - PCC handles isolation through locking
3. **Sequential but lock-aware** - Operations execute one at a time, but locks determine conflicts
4. **Streaming happens at edges** - Internal execution uses iterators, client API provides streams
5. **Arc for efficiency, not versioning** - Cheap result copying, not MVCC

## Implementation Status

- ✅ Layer 1: Storage Engine (basic implementation)
- ✅ Layer 2: Lock Manager (wound-wait implemented)
- ✅ Layer 3: Transaction Manager (basic implementation)
- 🚧 Layer 4: SQL Execution (placeholder)
- ❌ Layer 5: Raft State Machine (needs rewrite)
- ❌ Layer 6: Client API (not started)

## Next Steps

1. Build SQL execution layer with parser from toydb
2. Implement correct Raft state machine based on this architecture
3. Add client API with result streaming
