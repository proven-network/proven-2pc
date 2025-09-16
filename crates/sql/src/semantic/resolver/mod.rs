//! Name resolution sub-modules

pub mod column;
pub mod scope;
pub mod table;

pub use column::ColumnResolver;
pub use scope::ScopeManager;
pub use table::TableResolver;