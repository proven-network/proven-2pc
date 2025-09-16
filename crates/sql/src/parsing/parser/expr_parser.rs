//! Expression parser module
//!
//! Handles parsing of SQL expressions including operators, functions, literals,
//! and complex expressions like CASE/WHEN.

use crate::parsing::ast::{Expression, Literal, Operator};
use std::ops::Add;

/// Operator precedence.
pub type Precedence = u8;

/// Operator associativity.
pub enum Associativity {
    Left,
    Right,
}

impl Add<Associativity> for Precedence {
    type Output = Self;

    fn add(self, rhs: Associativity) -> Self {
        // Left-associative operators have increased precedence, so they bind
        // tighter to their left-hand side.
        self + match rhs {
            Associativity::Left => 1,
            Associativity::Right => 0,
        }
    }
}

/// Prefix operators.
pub enum PrefixOperator {
    Minus, // -a
    Not,   // NOT a
    Plus,  // +a
}

impl PrefixOperator {
    /// The operator precedence.
    pub fn precedence(&self) -> Precedence {
        match self {
            Self::Not => 3,
            Self::Minus | Self::Plus => 10,
        }
    }

    // The operator associativity. Prefix operators are right-associative by
    // definition.
    pub fn associativity(&self) -> Associativity {
        Associativity::Right
    }

    /// Builds an AST expression for the operator.
    pub fn into_expression(self, rhs: Expression) -> Expression {
        let rhs = Box::new(rhs);
        match self {
            Self::Plus => Operator::Identity(rhs).into(),
            Self::Minus => Operator::Negate(rhs).into(),
            Self::Not => Operator::Not(rhs).into(),
        }
    }
}

/// Infix operators.
pub enum InfixOperator {
    Add,                // a + b
    And,                // a AND b
    Divide,             // a / b
    Equal,              // a = b
    Exponentiate,       // a ^ b
    GreaterThan,        // a > b
    GreaterThanOrEqual, // a >= b
    LessThan,           // a < b
    LessThanOrEqual,    // a <= b
    Like,               // a LIKE b
    Multiply,           // a * b
    NotEqual,           // a != b
    Or,                 // a OR b
    Remainder,          // a % b
    Subtract,           // a - b
}

impl InfixOperator {
    /// The operator precedence.
    ///
    /// Mostly follows Postgres, except IS and LIKE having same precedence as =.
    /// This is similar to SQLite and MySQL.
    pub fn precedence(&self) -> Precedence {
        match self {
            Self::Or => 1,
            Self::And => 2,
            // Self::Not => 3
            Self::Equal | Self::NotEqual | Self::Like => 4, // also Self::Is
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 5,
            Self::Add | Self::Subtract => 6,
            Self::Multiply | Self::Divide | Self::Remainder => 7,
            Self::Exponentiate => 8,
        }
    }

    /// The operator associativity.
    pub fn associativity(&self) -> Associativity {
        match self {
            Self::Exponentiate => Associativity::Right,
            _ => Associativity::Left,
        }
    }

    /// Builds an AST expression for the infix operator.
    pub fn into_expression(self, lhs: Expression, rhs: Expression) -> Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        match self {
            Self::Add => Operator::Add(lhs, rhs).into(),
            Self::And => Operator::And(lhs, rhs).into(),
            Self::Divide => Operator::Divide(lhs, rhs).into(),
            Self::Equal => Operator::Equal(lhs, rhs).into(),
            Self::Exponentiate => Operator::Exponentiate(lhs, rhs).into(),
            Self::GreaterThan => Operator::GreaterThan(lhs, rhs).into(),
            Self::GreaterThanOrEqual => Operator::GreaterThanOrEqual(lhs, rhs).into(),
            Self::LessThan => Operator::LessThan(lhs, rhs).into(),
            Self::LessThanOrEqual => Operator::LessThanOrEqual(lhs, rhs).into(),
            Self::Like => Operator::Like(lhs, rhs).into(),
            Self::Multiply => Operator::Multiply(lhs, rhs).into(),
            Self::NotEqual => Operator::NotEqual(lhs, rhs).into(),
            Self::Or => Operator::Or(lhs, rhs).into(),
            Self::Remainder => Operator::Remainder(lhs, rhs).into(),
            Self::Subtract => Operator::Subtract(lhs, rhs).into(),
        }
    }
}

/// Postfix operators.
pub enum PostfixOperator {
    Factorial,                             // a!
    Is(Literal),                           // a IS NULL | NAN
    IsNot(Literal),                        // a IS NOT NULL | NAN
    InList(Vec<Expression>, bool),         // a IN (list) or a NOT IN (list)
    Between(Expression, Expression, bool), // a BETWEEN low AND high or a NOT BETWEEN low AND high
    ArrayAccess(Expression),               // a[index]
    FieldAccess(String),                   // a.field
}

impl PostfixOperator {
    // The operator precedence.
    pub fn precedence(&self) -> Precedence {
        match self {
            Self::Is(_) | Self::IsNot(_) | Self::InList(_, _) | Self::Between(_, _, _) => 4,
            Self::Factorial => 9,
            Self::ArrayAccess(_) | Self::FieldAccess(_) => 10, // Highest precedence
        }
    }

    /// Builds an AST expression for the operator.
    pub fn into_expression(self, lhs: Expression) -> Expression {
        let lhs = Box::new(lhs);
        match self {
            Self::Factorial => Operator::Factorial(lhs).into(),
            Self::Is(v) => Operator::Is(lhs, v).into(),
            Self::IsNot(v) => Operator::Not(Box::new(Operator::Is(lhs, v).into())).into(),
            Self::InList(list, negated) => Operator::InList {
                expr: lhs,
                list,
                negated,
            }
            .into(),
            Self::Between(low, high, negated) => Operator::Between {
                expr: lhs,
                low: Box::new(low),
                high: Box::new(high),
                negated,
            }
            .into(),
            Self::ArrayAccess(index) => Expression::ArrayAccess {
                base: lhs,
                index: Box::new(index),
            },
            Self::FieldAccess(field) => Expression::FieldAccess { base: lhs, field },
        }
    }
}
