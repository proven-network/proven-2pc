//! Resource operations that can be performed within transactions

use crate::types::Amount;
use proven_common::{Operation, OperationType};
use serde::{Deserialize, Serialize};

/// Operations that can be performed on a resource
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceOperation {
    /// Initialize a new resource with metadata
    Initialize {
        name: String,
        symbol: String,
        decimals: u32,
    },

    /// Update resource metadata
    UpdateMetadata {
        name: Option<String>,
        symbol: Option<String>,
    },

    /// Mint new units of the resource
    Mint {
        to: String,
        amount: Amount,
        memo: Option<String>,
    },

    /// Burn units of the resource
    Burn {
        from: String,
        amount: Amount,
        memo: Option<String>,
    },

    /// Transfer units between accounts
    Transfer {
        from: String,
        to: String,
        amount: Amount,
        memo: Option<String>,
    },

    /// Query balance for an account
    GetBalance { account: String },

    /// Query resource metadata
    GetMetadata,

    /// Query total supply
    GetTotalSupply,
}

impl Operation for ResourceOperation {
    fn operation_type(&self) -> OperationType {
        match self {
            ResourceOperation::Initialize { .. } => OperationType::Write,
            ResourceOperation::UpdateMetadata { .. } => OperationType::Write,
            ResourceOperation::Mint { .. } => OperationType::Write,
            ResourceOperation::Burn { .. } => OperationType::Write,
            ResourceOperation::Transfer { .. } => OperationType::Write,
            ResourceOperation::GetBalance { .. } => OperationType::Read,
            ResourceOperation::GetMetadata => OperationType::Read,
            ResourceOperation::GetTotalSupply => OperationType::Read,
        }
    }
}
