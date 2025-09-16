//! SQL expressions and operators

use std::hash::{Hash, Hasher};

/// SQL expressions, e.g. `a + 7 > b`. Can be nested.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Expression {
    /// All columns, i.e. *.
    All,
    /// A column reference, optionally qualified with a table name.
    Column(Option<String>, String),
    /// A literal value.
    Literal(Literal),
    /// A function call (name and parameters).
    Function(String, Vec<Expression>),
    /// An operator.
    Operator(Operator),
    /// A parameter placeholder (? in SQL), with its position (0-indexed).
    Parameter(usize),
    /// CASE WHEN expression
    Case {
        /// Optional expression to compare against (for simple CASE)
        operand: Option<Box<Expression>>,
        /// List of WHEN conditions and their results
        when_clauses: Vec<(Expression, Expression)>,
        /// Optional ELSE result
        else_clause: Option<Box<Expression>>,
    },
    /// Array or list element access: base[index]
    ArrayAccess {
        base: Box<Expression>,
        index: Box<Expression>,
    },
    /// Struct field access: base.field
    FieldAccess {
        base: Box<Expression>,
        field: String,
    },
    /// Array literal: [1, 2, 3]
    ArrayLiteral(Vec<Expression>),
    /// Map literal: {key1: value1, key2: value2}
    MapLiteral(Vec<(Expression, Expression)>),
}

/// Expression literal values.
#[derive(Clone, Debug)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(f64),
    String(String),
    Bytea(Vec<u8>),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    Interval(crate::types::data_type::Interval),
}

/// Expression operators.
///
/// Since this is a recursive data structure, we have to box each child
/// expression, which incurs a heap allocation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Operator {
    And(Box<Expression>, Box<Expression>), // a AND b
    Not(Box<Expression>),                  // NOT a
    Or(Box<Expression>, Box<Expression>),  // a OR b

    Equal(Box<Expression>, Box<Expression>),       // a = b
    GreaterThan(Box<Expression>, Box<Expression>), // a > b
    GreaterThanOrEqual(Box<Expression>, Box<Expression>), // a >= b
    Is(Box<Expression>, Literal),                  // IS NULL or IS NAN
    LessThan(Box<Expression>, Box<Expression>),    // a < b
    LessThanOrEqual(Box<Expression>, Box<Expression>), // a <= b
    NotEqual(Box<Expression>, Box<Expression>),    // a != b

    Add(Box<Expression>, Box<Expression>),          // a + b
    Divide(Box<Expression>, Box<Expression>),       // a / b
    Exponentiate(Box<Expression>, Box<Expression>), // a ^ b
    Factorial(Box<Expression>),                     // a!
    Identity(Box<Expression>),                      // +a
    Multiply(Box<Expression>, Box<Expression>),     // a * b
    Negate(Box<Expression>),                        // -a
    Remainder(Box<Expression>, Box<Expression>),    // a % b
    Subtract(Box<Expression>, Box<Expression>),     // a - b

    Like(Box<Expression>, Box<Expression>), // a LIKE b

    // IN and BETWEEN operators
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    }, // a IN (b, c, d) or a NOT IN (b, c, d)
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    }, // a BETWEEN b AND c or a NOT BETWEEN b AND c
}

/// To allow using expressions and literals in e.g. hashmaps, implement simple
/// equality by value for all types, including Null and f64::NAN. This only
/// checks that the values are the same, and ignores SQL semantics for e.g. NULL
/// and NaN (which is handled by SQL expression evaluation).
impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(l), Self::Boolean(r)) => l == r,
            (Self::Integer(l), Self::Integer(r)) => l == r,
            (Self::Float(l), Self::Float(r)) => l.to_bits() == r.to_bits(),
            (Self::String(l), Self::String(r)) => l == r,
            (Self::Bytea(l), Self::Bytea(r)) => l == r,
            (Self::Date(l), Self::Date(r)) => l == r,
            (Self::Time(l), Self::Time(r)) => l == r,
            (Self::Timestamp(l), Self::Timestamp(r)) => l == r,
            (Self::Interval(l), Self::Interval(r)) => l == r,
            (_, _) => false,
        }
    }
}

impl Eq for Literal {}

impl Hash for Literal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::Boolean(v) => v.hash(state),
            Self::Integer(v) => v.hash(state),
            Self::Float(v) => v.to_bits().hash(state),
            Self::String(v) => v.hash(state),
            Self::Bytea(v) => v.hash(state),
            Self::Date(v) => v.hash(state),
            Self::Time(v) => v.hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::Interval(v) => v.hash(state),
        }
    }
}

impl From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        Expression::Literal(literal)
    }
}

impl From<Operator> for Expression {
    fn from(operator: Operator) -> Self {
        Expression::Operator(operator)
    }
}

impl Expression {
    /// Walks the expression tree depth-first, calling a closure for every node.
    /// Halts and returns false if the closure returns false.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        use Operator::*;

        if !visitor(self) {
            return false;
        }

        match self {
            Self::Operator(op) => match op {
                Add(lhs, rhs)
                | And(lhs, rhs)
                | Divide(lhs, rhs)
                | Equal(lhs, rhs)
                | Exponentiate(lhs, rhs)
                | GreaterThan(lhs, rhs)
                | GreaterThanOrEqual(lhs, rhs)
                | LessThan(lhs, rhs)
                | LessThanOrEqual(lhs, rhs)
                | Like(lhs, rhs)
                | Multiply(lhs, rhs)
                | NotEqual(lhs, rhs)
                | Or(lhs, rhs)
                | Remainder(lhs, rhs)
                | Subtract(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

                Factorial(expr) | Identity(expr) | Is(expr, _) | Negate(expr) | Not(expr) => {
                    expr.walk(visitor)
                }

                InList { expr, list, .. } => {
                    expr.walk(visitor) && list.iter().all(|e| e.walk(visitor))
                }

                Between {
                    expr, low, high, ..
                } => expr.walk(visitor) && low.walk(visitor) && high.walk(visitor),
            },

            Self::Function(_, exprs) => exprs.iter().all(|expr| expr.walk(visitor)),

            Self::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                // Walk operand if present
                if let Some(op) = operand
                    && !op.walk(visitor)
                {
                    return false;
                }
                // Walk all when clauses
                for (cond, result) in when_clauses {
                    if !cond.walk(visitor) || !result.walk(visitor) {
                        return false;
                    }
                }
                // Walk else clause if present
                if let Some(else_expr) = else_clause
                    && !else_expr.walk(visitor)
                {
                    return false;
                }
                true
            }

            Self::ArrayAccess { base, index } => base.walk(visitor) && index.walk(visitor),

            Self::FieldAccess { base, field: _ } => base.walk(visitor),

            Self::ArrayLiteral(elements) => elements.iter().all(|e| e.walk(visitor)),

            Self::MapLiteral(pairs) => pairs
                .iter()
                .all(|(k, v)| k.walk(visitor) && v.walk(visitor)),

            _ => true,
        }
    }

    /// Transforms the expression tree depth-first, by applying a closure to every
    /// node and replacing the current node with the returned value.
    pub fn transform<E>(
        &mut self,
        transformer: &mut impl FnMut(&mut Expression) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        use Operator::*;

        // Transform children first.
        match self {
            Self::Operator(op) => match op {
                Add(lhs, rhs)
                | And(lhs, rhs)
                | Divide(lhs, rhs)
                | Equal(lhs, rhs)
                | Exponentiate(lhs, rhs)
                | GreaterThan(lhs, rhs)
                | GreaterThanOrEqual(lhs, rhs)
                | LessThan(lhs, rhs)
                | LessThanOrEqual(lhs, rhs)
                | Like(lhs, rhs)
                | Multiply(lhs, rhs)
                | NotEqual(lhs, rhs)
                | Or(lhs, rhs)
                | Remainder(lhs, rhs)
                | Subtract(lhs, rhs) => {
                    lhs.transform(transformer)?;
                    rhs.transform(transformer)?;
                }

                Factorial(expr) | Identity(expr) | Is(expr, _) | Negate(expr) | Not(expr) => {
                    expr.transform(transformer)?
                }

                InList { expr, list, .. } => {
                    expr.transform(transformer)?;
                    for item in list {
                        item.transform(transformer)?;
                    }
                }

                Between {
                    expr, low, high, ..
                } => {
                    expr.transform(transformer)?;
                    low.transform(transformer)?;
                    high.transform(transformer)?;
                }
            },

            Self::Function(_, exprs) => {
                for expr in exprs {
                    expr.transform(transformer)?;
                }
            }

            Self::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    op.transform(transformer)?;
                }
                for (cond, result) in when_clauses {
                    cond.transform(transformer)?;
                    result.transform(transformer)?;
                }
                if let Some(else_expr) = else_clause {
                    else_expr.transform(transformer)?;
                }
            }

            Self::ArrayAccess { base, index } => {
                base.transform(transformer)?;
                index.transform(transformer)?;
            }

            Self::FieldAccess { base, .. } => base.transform(transformer)?,

            Self::ArrayLiteral(elements) => {
                for element in elements {
                    element.transform(transformer)?;
                }
            }

            Self::MapLiteral(pairs) => {
                for (key, value) in pairs {
                    key.transform(transformer)?;
                    value.transform(transformer)?;
                }
            }

            _ => {}
        }

        // Transform the current node.
        transformer(self)
    }

    /// Returns whether the expression is a constant, without any column references.
    pub fn is_constant(&self) -> bool {
        !self.walk(&mut |expr| !matches!(expr, Self::Column(_, _)))
    }
}
