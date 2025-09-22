//! Value types for resource amounts

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::{Add, Sub};
use std::str::FromStr;

/// Amount represents a quantity of a resource with decimal precision
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Amount(pub Decimal);

impl Amount {
    /// Create a new amount from a decimal
    pub fn new(value: Decimal) -> Self {
        Amount(value)
    }

    /// Create zero amount
    pub fn zero() -> Self {
        Amount(Decimal::ZERO)
    }

    /// Check if amount is zero
    pub fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    /// Check if amount is positive
    pub fn is_positive(&self) -> bool {
        self.0.is_sign_positive() && !self.0.is_zero()
    }

    /// Create amount from integer with decimals
    /// e.g., from_integer(100, 2) = 1.00
    pub fn from_integer(value: i64, decimals: u32) -> Self {
        let scale = 10_i64.pow(decimals);
        let decimal = Decimal::from(value) / Decimal::from(scale);
        Amount(decimal)
    }

    /// Convert to integer with decimals
    /// e.g., to_integer(1.00, 2) = 100
    pub fn to_integer(&self, decimals: u32) -> Option<i64> {
        let scale = Decimal::from(10_i64.pow(decimals));
        let scaled = self.0 * scale;
        scaled.round().to_i64()
    }
}

impl fmt::Display for Amount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for Amount {
    type Err = rust_decimal::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Amount(Decimal::from_str(s)?))
    }
}

impl Add for Amount {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Amount(self.0 + other.0)
    }
}

impl Sub for Amount {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Amount(self.0 - other.0)
    }
}

impl From<Decimal> for Amount {
    fn from(decimal: Decimal) -> Self {
        Amount(decimal)
    }
}

impl From<Amount> for Decimal {
    fn from(amount: Amount) -> Self {
        amount.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_amount_operations() {
        let a = Amount::from_integer(150, 2); // 1.50
        let b = Amount::from_integer(250, 2); // 2.50

        let sum = a + b;
        assert_eq!(sum.to_integer(2), Some(400)); // 4.00

        let diff = b - a;
        assert_eq!(diff.to_integer(2), Some(100)); // 1.00
    }

    #[test]
    fn test_amount_comparison() {
        let a = Amount::from_integer(100, 2);
        let b = Amount::from_integer(200, 2);

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, a);
    }
}
