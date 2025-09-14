//! Error types for the SQL engine

use proven_hlc::HlcTimestamp;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone, PartialEq)]
pub enum Error {
    // Storage errors
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    DuplicateTable(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Column already exists: {0}")]
    DuplicateColumn(String),

    // Type errors
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("NULL constraint violation on column: {0}")]
    NullConstraintViolation(String),

    #[error("Operation would block")]
    WouldBlock,

    #[error("Predicate conflict with transaction {holder}: {reason}")]
    PredicateConflict {
        holder: HlcTimestamp,
        reason: String,
    },

    #[error("Deadlock detected")]
    DeadlockDetected,

    #[error("Lock acquisition timeout")]
    LockTimeout,

    // Transaction errors
    #[error("Transaction not found: {0}")]
    TransactionNotFound(HlcTimestamp),

    #[error("Transaction aborted: {0}")]
    TransactionAborted(HlcTimestamp),

    #[error("Transaction not active: {0}")]
    TransactionNotActive(HlcTimestamp),

    #[error("Transaction wounded by {wounded_by} to prevent deadlock")]
    TransactionWounded { wounded_by: HlcTimestamp },

    // SQL errors
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Compound objects are not supported")]
    CompoundObjectNotSupported,

    // Constraint errors
    #[error("Primary key violation: {0}")]
    PrimaryKeyViolation(String),

    #[error("Unique constraint violation: {0}")]
    UniqueConstraintViolation(String),

    #[error("Foreign key violation: {0}")]
    ForeignKeyViolation(String),

    #[error("Check constraint violation: {0}")]
    CheckConstraintViolation(String),

    // System errors
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}
