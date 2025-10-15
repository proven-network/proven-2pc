//! SQL expression evaluation with transaction context for deterministic functions
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Key changes:
//! - Added TransactionContext for deterministic function evaluation
//! - Integrated with our Value types (including UUID, Timestamp, Blob)

use super::Value;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

/// An expression, made up of nested operations and values. Values are either
/// constants, or numeric column references which are looked up in rows.
/// Evaluated to a final value during query execution.
#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    /// A constant value.
    Constant(Value),
    /// A column reference. Looks up the value in a row during evaluation.
    Column(usize),
    /// A parameter placeholder for prepared statements (0-indexed).
    Parameter(usize),
    /// All columns - used for COUNT(DISTINCT *)
    All,

    /// a AND b: logical AND of two booleans.
    And(Box<Expression>, Box<Expression>),
    /// a OR b: logical OR of two booleans.
    Or(Box<Expression>, Box<Expression>),
    /// a XOR b: logical XOR (exclusive OR) of two booleans.
    Xor(Box<Expression>, Box<Expression>),
    /// NOT a: logical NOT of a boolean.
    Not(Box<Expression>),

    /// a = b: equality comparison of two values.
    Equal(Box<Expression>, Box<Expression>),
    /// a > b: greater than comparison of two values.
    GreaterThan(Box<Expression>, Box<Expression>),
    /// a < b: less than comparison of two values.
    LessThan(Box<Expression>, Box<Expression>),
    /// a >= b: greater than or equal comparison.
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    /// a <= b: less than or equal comparison.
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    /// a != b: not equal comparison.
    NotEqual(Box<Expression>, Box<Expression>),
    /// a IS NULL or a IS NAN: checks for the given value.
    Is(Box<Expression>, Value),

    /// a + b: adds two numbers or concatenates strings.
    Add(Box<Expression>, Box<Expression>),
    /// a || b: concatenates strings (SQL standard).
    Concat(Box<Expression>, Box<Expression>),
    /// a - b: subtracts two numbers.
    Subtract(Box<Expression>, Box<Expression>),
    /// a * b: multiplies two numbers.
    Multiply(Box<Expression>, Box<Expression>),
    /// a / b: divides two numbers.
    Divide(Box<Expression>, Box<Expression>),
    /// a % b: remainder of two numbers.
    Remainder(Box<Expression>, Box<Expression>),
    /// a ^ b: exponentiates two numbers.
    Exponentiate(Box<Expression>, Box<Expression>),
    /// a!: takes the factorial of a number.
    Factorial(Box<Expression>),
    /// +a: the identity function, returns the same number.
    Identity(Box<Expression>),
    /// -a: negates a number.
    Negate(Box<Expression>),

    /// a ILIKE pattern: SQL pattern matching (case-insensitive).
    ILike(Box<Expression>, Box<Expression>),
    /// a LIKE pattern: SQL pattern matching.
    Like(Box<Expression>, Box<Expression>),

    /// a IN (list): checks if value is in list.
    InList(Box<Expression>, Vec<Expression>, bool), // expr, list, negated

    /// a BETWEEN low AND high: checks if value is in range.
    Between(Box<Expression>, Box<Expression>, Box<Expression>, bool), // expr, low, high, negated

    /// Function call with deterministic evaluation.
    Function(String, Vec<Expression>),

    /// Array/List element access: base[index]
    ArrayAccess(Box<Expression>, Box<Expression>),

    /// Struct field access: base.field
    FieldAccess(Box<Expression>, String),

    /// Array literal: [1, 2, 3]
    ArrayLiteral(Vec<Expression>),

    /// Map literal: {key1: value1, key2: value2}
    MapLiteral(Vec<(Expression, Expression)>),

    /// IN subquery: expr IN (SELECT ...)
    /// Store the subquery plan for execution
    InSubquery(
        Box<Expression>,
        Box<super::super::planning::plan::Plan>,
        bool,
    ), // expr, subquery plan, negated

    /// EXISTS subquery: EXISTS (SELECT ...)
    Exists(Box<super::super::planning::plan::Plan>, bool), // subquery plan, negated

    /// Scalar subquery: (SELECT ...)
    Subquery(Box<super::super::planning::plan::Plan>),

    /// CASE expression with optional operand and when/then clauses
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Expression::*;
        match self {
            Constant(value) => write!(f, "{}", value),
            Column(i) => write!(f, "#{}", i),
            Parameter(idx) => write!(f, "?{}", idx),
            All => write!(f, "*"),

            And(lhs, rhs) => write!(f, "({} AND {})", lhs, rhs),
            Or(lhs, rhs) => write!(f, "({} OR {})", lhs, rhs),
            Xor(lhs, rhs) => write!(f, "({} XOR {})", lhs, rhs),
            Not(expr) => write!(f, "(NOT {})", expr),

            Equal(lhs, rhs) => write!(f, "({} = {})", lhs, rhs),
            NotEqual(lhs, rhs) => write!(f, "({} != {})", lhs, rhs),
            GreaterThan(lhs, rhs) => write!(f, "({} > {})", lhs, rhs),
            GreaterThanOrEqual(lhs, rhs) => write!(f, "({} >= {})", lhs, rhs),
            LessThan(lhs, rhs) => write!(f, "({} < {})", lhs, rhs),
            LessThanOrEqual(lhs, rhs) => write!(f, "({} <= {})", lhs, rhs),
            Is(expr, value) => write!(f, "({} IS {})", expr, value),

            Add(lhs, rhs) => write!(f, "({} + {})", lhs, rhs),
            Concat(lhs, rhs) => write!(f, "({} || {})", lhs, rhs),
            Subtract(lhs, rhs) => write!(f, "({} - {})", lhs, rhs),
            Multiply(lhs, rhs) => write!(f, "({} * {})", lhs, rhs),
            Divide(lhs, rhs) => write!(f, "({} / {})", lhs, rhs),
            Remainder(lhs, rhs) => write!(f, "({} % {})", lhs, rhs),
            Exponentiate(lhs, rhs) => write!(f, "({} ^ {})", lhs, rhs),
            Factorial(expr) => write!(f, "({}!)", expr),
            Identity(expr) => write!(f, "(+{})", expr),
            Negate(expr) => write!(f, "(-{})", expr),

            ILike(lhs, rhs) => write!(f, "({} ILIKE {})", lhs, rhs),
            Like(lhs, rhs) => write!(f, "({} LIKE {})", lhs, rhs),

            Function(name, args) => {
                write!(f, "{}(", name)?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", arg)?;
                }
                write!(f, ")")
            }

            InList(expr, list, negated) => {
                write!(f, "{}", expr)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN (")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, ")")
            }

            Between(expr, low, high, negated) => {
                write!(f, "{}", expr)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " BETWEEN {} AND {}", low, high)
            }

            ArrayAccess(base, index) => write!(f, "{}[{}]", base, index),

            FieldAccess(base, field) => write!(f, "{}.{}", base, field),

            ArrayLiteral(elements) => {
                write!(f, "[")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")
            }

            MapLiteral(pairs) => {
                write!(f, "{{")?;
                for (i, (key, val)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, val)?;
                }
                write!(f, "}}")
            }

            InSubquery(expr, _, negated) => {
                write!(f, "{}", expr)?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN (subquery)")
            }

            Exists(_, negated) => {
                if *negated {
                    write!(f, "NOT EXISTS (subquery)")
                } else {
                    write!(f, "EXISTS (subquery)")
                }
            }

            Subquery(_) => write!(f, "(subquery)"),

            Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {}", op)?;
                }
                for (when, then) in when_clauses {
                    write!(f, " WHEN {} THEN {}", when, then)?;
                }
                if let Some(else_expr) = else_clause {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
        }
    }
}

/// A DEFAULT expression - a subset of Expression that's valid in DEFAULT clauses.
/// Cannot contain column references, subqueries, or other runtime-dependent expressions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DefaultExpression {
    /// A constant value
    Constant(Value),

    /// Function call (e.g., CURRENT_DATE(), UUID())
    Function(String, Vec<DefaultExpression>),

    // Boolean operations
    And(Box<DefaultExpression>, Box<DefaultExpression>),
    Or(Box<DefaultExpression>, Box<DefaultExpression>),
    Xor(Box<DefaultExpression>, Box<DefaultExpression>),
    Not(Box<DefaultExpression>),

    // Comparison operations
    Equal(Box<DefaultExpression>, Box<DefaultExpression>),
    GreaterThan(Box<DefaultExpression>, Box<DefaultExpression>),
    LessThan(Box<DefaultExpression>, Box<DefaultExpression>),
    GreaterThanOrEqual(Box<DefaultExpression>, Box<DefaultExpression>),
    LessThanOrEqual(Box<DefaultExpression>, Box<DefaultExpression>),
    NotEqual(Box<DefaultExpression>, Box<DefaultExpression>),
    Is(Box<DefaultExpression>, Value),

    // Arithmetic operations
    Add(Box<DefaultExpression>, Box<DefaultExpression>),
    Concat(Box<DefaultExpression>, Box<DefaultExpression>),
    Subtract(Box<DefaultExpression>, Box<DefaultExpression>),
    Multiply(Box<DefaultExpression>, Box<DefaultExpression>),
    Divide(Box<DefaultExpression>, Box<DefaultExpression>),
    Remainder(Box<DefaultExpression>, Box<DefaultExpression>),
    Exponentiate(Box<DefaultExpression>, Box<DefaultExpression>),
    Factorial(Box<DefaultExpression>),
    Identity(Box<DefaultExpression>),
    Negate(Box<DefaultExpression>),

    // Pattern matching
    ILike(Box<DefaultExpression>, Box<DefaultExpression>),
    Like(Box<DefaultExpression>, Box<DefaultExpression>),

    // IN list (but not subqueries)
    InList(Box<DefaultExpression>, Vec<DefaultExpression>, bool),

    // BETWEEN
    Between(
        Box<DefaultExpression>,
        Box<DefaultExpression>,
        Box<DefaultExpression>,
        bool,
    ),

    // Literals
    ArrayLiteral(Vec<DefaultExpression>),
    MapLiteral(Vec<(DefaultExpression, DefaultExpression)>),

    // CASE expression
    Case {
        operand: Option<Box<DefaultExpression>>,
        when_clauses: Vec<(DefaultExpression, DefaultExpression)>,
        else_clause: Option<Box<DefaultExpression>>,
    },
}

impl Display for DefaultExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Delegate to Expression's Display by converting
        let expr: Expression = self.clone().into();
        write!(f, "{}", expr)
    }
}

impl From<DefaultExpression> for Expression {
    fn from(default_expr: DefaultExpression) -> Self {
        use DefaultExpression as DE;
        use Expression as E;

        match default_expr {
            DE::Constant(v) => E::Constant(v),
            DE::Function(name, args) => {
                E::Function(name, args.into_iter().map(|a| a.into()).collect())
            }
            DE::And(l, r) => E::And(Box::new((*l).into()), Box::new((*r).into())),
            DE::Or(l, r) => E::Or(Box::new((*l).into()), Box::new((*r).into())),
            DE::Xor(l, r) => E::Xor(Box::new((*l).into()), Box::new((*r).into())),
            DE::Not(e) => E::Not(Box::new((*e).into())),
            DE::Equal(l, r) => E::Equal(Box::new((*l).into()), Box::new((*r).into())),
            DE::GreaterThan(l, r) => E::GreaterThan(Box::new((*l).into()), Box::new((*r).into())),
            DE::LessThan(l, r) => E::LessThan(Box::new((*l).into()), Box::new((*r).into())),
            DE::GreaterThanOrEqual(l, r) => {
                E::GreaterThanOrEqual(Box::new((*l).into()), Box::new((*r).into()))
            }
            DE::LessThanOrEqual(l, r) => {
                E::LessThanOrEqual(Box::new((*l).into()), Box::new((*r).into()))
            }
            DE::NotEqual(l, r) => E::NotEqual(Box::new((*l).into()), Box::new((*r).into())),
            DE::Is(e, v) => E::Is(Box::new((*e).into()), v),
            DE::Add(l, r) => E::Add(Box::new((*l).into()), Box::new((*r).into())),
            DE::Concat(l, r) => E::Concat(Box::new((*l).into()), Box::new((*r).into())),
            DE::Subtract(l, r) => E::Subtract(Box::new((*l).into()), Box::new((*r).into())),
            DE::Multiply(l, r) => E::Multiply(Box::new((*l).into()), Box::new((*r).into())),
            DE::Divide(l, r) => E::Divide(Box::new((*l).into()), Box::new((*r).into())),
            DE::Remainder(l, r) => E::Remainder(Box::new((*l).into()), Box::new((*r).into())),
            DE::Exponentiate(l, r) => E::Exponentiate(Box::new((*l).into()), Box::new((*r).into())),
            DE::Factorial(e) => E::Factorial(Box::new((*e).into())),
            DE::Identity(e) => E::Identity(Box::new((*e).into())),
            DE::Negate(e) => E::Negate(Box::new((*e).into())),
            DE::ILike(l, r) => E::ILike(Box::new((*l).into()), Box::new((*r).into())),
            DE::Like(l, r) => E::Like(Box::new((*l).into()), Box::new((*r).into())),
            DE::InList(e, list, neg) => E::InList(
                Box::new((*e).into()),
                list.into_iter().map(|a| a.into()).collect(),
                neg,
            ),
            DE::Between(e, l, h, neg) => E::Between(
                Box::new((*e).into()),
                Box::new((*l).into()),
                Box::new((*h).into()),
                neg,
            ),
            DE::ArrayLiteral(elems) => {
                E::ArrayLiteral(elems.into_iter().map(|a| a.into()).collect())
            }
            DE::MapLiteral(pairs) => E::MapLiteral(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
            ),
            DE::Case {
                operand,
                when_clauses,
                else_clause,
            } => E::Case {
                operand: operand.map(|o| Box::new((*o).into())),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(w, t)| (w.into(), t.into()))
                    .collect(),
                else_clause: else_clause.map(|e| Box::new((*e).into())),
            },
        }
    }
}
