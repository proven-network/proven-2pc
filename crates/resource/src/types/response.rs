//! Response types for resource operations

use crate::types::Amount;
use proven_common::Response;
use proven_value::Vault;
use serde::{Deserialize, Serialize};

/// Response from a resource operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceResponse {
    /// Resource was initialized
    Initialized {
        name: String,
        symbol: String,
        decimals: u32,
    },

    /// Metadata was updated
    MetadataUpdated {
        name: Option<String>,
        symbol: Option<String>,
    },

    /// Units were minted
    Minted {
        to: Vault,
        amount: Amount,
        new_balance: Amount,
        total_supply: Amount,
    },

    /// Units were burned
    Burned {
        from: Vault,
        amount: Amount,
        new_balance: Amount,
        total_supply: Amount,
    },

    /// Units were transferred
    Transferred {
        from: Vault,
        to: Vault,
        amount: Amount,
        from_balance: Amount,
        to_balance: Amount,
    },

    /// Balance query result
    Balance { account: Vault, amount: Amount },

    /// Metadata query result
    Metadata {
        name: String,
        symbol: String,
        decimals: u32,
        total_supply: Amount,
    },

    /// Total supply query result
    TotalSupply { amount: Amount },

    /// Error occurred
    Error(String),
}

impl Response for ResourceResponse {}
