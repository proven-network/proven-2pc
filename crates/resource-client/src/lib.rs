//! Resource client for coordinator-based transactions

use proven_coordinator::Transaction;
use proven_resource::types::Amount;
use proven_resource::{ResourceOperation, ResourceResponse};

/// Resource client that works with coordinator transactions
#[derive(Clone)]
pub struct ResourceClient {
    /// The transaction this client is associated with
    transaction: Transaction,
}

impl ResourceClient {
    /// Create a new Resource client for a transaction
    pub fn new(transaction: Transaction) -> Self {
        Self { transaction }
    }

    /// Initialize a new resource with metadata
    pub async fn initialize(
        &self,
        stream_name: impl Into<String>,
        name: impl Into<String>,
        symbol: impl Into<String>,
        decimals: u32,
    ) -> Result<(), ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::Initialize {
            name: name.into(),
            symbol: symbol.into(),
            decimals,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Initialized { .. } => Ok(()),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Update resource metadata
    pub async fn update_metadata(
        &self,
        stream_name: impl Into<String>,
        name: Option<String>,
        symbol: Option<String>,
    ) -> Result<(), ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::UpdateMetadata { name, symbol };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::MetadataUpdated { .. } => Ok(()),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Mint new units of the resource
    pub async fn mint(
        &self,
        stream_name: impl Into<String>,
        to: impl Into<String>,
        amount: Amount,
    ) -> Result<(Amount, Amount), ResourceError> {
        self.mint_with_memo(stream_name, to, amount, None).await
    }

    /// Mint new units with a memo
    pub async fn mint_with_memo(
        &self,
        stream_name: impl Into<String>,
        to: impl Into<String>,
        amount: Amount,
        memo: Option<String>,
    ) -> Result<(Amount, Amount), ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::Mint {
            to: to.into(),
            amount,
            memo,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Minted {
                new_balance,
                total_supply,
                ..
            } => Ok((new_balance, total_supply)),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Mint integer amount (convenience method)
    pub async fn mint_integer(
        &self,
        stream_name: impl Into<String>,
        to: impl Into<String>,
        amount: u64,
    ) -> Result<(Amount, Amount), ResourceError> {
        use rust_decimal::Decimal;
        self.mint(stream_name, to, Amount::new(Decimal::from(amount)))
            .await
    }

    /// Burn units of the resource
    pub async fn burn(
        &self,
        stream_name: impl Into<String>,
        from: impl Into<String>,
        amount: Amount,
    ) -> Result<(Amount, Amount), ResourceError> {
        self.burn_with_memo(stream_name, from, amount, None).await
    }

    /// Burn units with a memo
    pub async fn burn_with_memo(
        &self,
        stream_name: impl Into<String>,
        from: impl Into<String>,
        amount: Amount,
        memo: Option<String>,
    ) -> Result<(Amount, Amount), ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::Burn {
            from: from.into(),
            amount,
            memo,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Burned {
                new_balance,
                total_supply,
                ..
            } => Ok((new_balance, total_supply)),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Transfer units between accounts
    pub async fn transfer(
        &self,
        stream_name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        amount: Amount,
    ) -> Result<(Amount, Amount), ResourceError> {
        self.transfer_with_memo(stream_name, from, to, amount, None)
            .await
    }

    /// Transfer units with a memo
    pub async fn transfer_with_memo(
        &self,
        stream_name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        amount: Amount,
        memo: Option<String>,
    ) -> Result<(Amount, Amount), ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::Transfer {
            from: from.into(),
            to: to.into(),
            amount,
            memo,
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Transferred {
                from_balance,
                to_balance,
                ..
            } => Ok((from_balance, to_balance)),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Transfer integer amount (convenience method)
    pub async fn transfer_integer(
        &self,
        stream_name: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        amount: u64,
    ) -> Result<(Amount, Amount), ResourceError> {
        use rust_decimal::Decimal;
        self.transfer(stream_name, from, to, Amount::new(Decimal::from(amount)))
            .await
    }

    /// Get account balance
    pub async fn get_balance(
        &self,
        stream_name: impl Into<String>,
        account: impl Into<String>,
    ) -> Result<Amount, ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::GetBalance {
            account: account.into(),
        };

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Balance { amount, .. } => Ok(amount),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Get account balance as integer (convenience method)
    pub async fn get_balance_integer(
        &self,
        stream_name: impl Into<String>,
        account: impl Into<String>,
    ) -> Result<u64, ResourceError> {
        let balance = self.get_balance(stream_name, account).await?;
        use rust_decimal::prelude::ToPrimitive;
        Ok(balance.0.to_u64().unwrap_or(0))
    }

    /// Get resource metadata
    pub async fn get_metadata(
        &self,
        stream_name: impl Into<String>,
    ) -> Result<ResourceMetadata, ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::GetMetadata;

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::Metadata {
                name,
                symbol,
                decimals,
                ..
            } => Ok(ResourceMetadata {
                name,
                symbol,
                decimals,
            }),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Get total supply
    pub async fn get_total_supply(
        &self,
        stream_name: impl Into<String>,
    ) -> Result<Amount, ResourceError> {
        let stream_name = stream_name.into();
        let operation = ResourceOperation::GetTotalSupply;

        let response = self.execute_operation(stream_name, operation).await?;

        match response {
            ResourceResponse::TotalSupply { amount } => Ok(amount),
            _ => Err(ResourceError::UnexpectedResponse),
        }
    }

    /// Execute an operation and deserialize the response
    async fn execute_operation(
        &self,
        stream_name: String,
        operation: ResourceOperation,
    ) -> Result<ResourceResponse, ResourceError> {
        // Execute through the transaction with the operation object
        let response_bytes = self
            .transaction
            .execute(stream_name, &operation)
            .await
            .map_err(|e| ResourceError::CoordinatorError(e.to_string()))?;

        // Deserialize the response
        let response = serde_json::from_slice(&response_bytes)
            .map_err(|e| ResourceError::DeserializationError(e.to_string()))?;

        Ok(response)
    }
}

/// Resource metadata
#[derive(Debug, Clone)]
pub struct ResourceMetadata {
    pub name: String,
    pub symbol: String,
    pub decimals: u32,
}

/// Resource-specific error type
#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("Coordinator error: {0}")]
    CoordinatorError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Unexpected response type")]
    UnexpectedResponse,
}
