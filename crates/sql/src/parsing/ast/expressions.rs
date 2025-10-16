//! SQL expressions and operators

use std::hash::{Hash, Hasher};

/// SQL expressions, e.g. `a + 7 > b`. Can be nested.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Expression {
    /// All columns, i.e. *.
    All,
    /// Qualified wildcard, i.e. table.*
    QualifiedWildcard(String),
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
    /// Subquery
    Subquery(Box<super::SelectStatement>),
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
    Interval(proven_value::Interval),
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
    Xor(Box<Expression>, Box<Expression>), // a XOR b

    Equal(Box<Expression>, Box<Expression>),       // a = b
    GreaterThan(Box<Expression>, Box<Expression>), // a > b
    GreaterThanOrEqual(Box<Expression>, Box<Expression>), // a >= b
    Is(Box<Expression>, Literal),                  // IS NULL or IS NAN
    LessThan(Box<Expression>, Box<Expression>),    // a < b
    LessThanOrEqual(Box<Expression>, Box<Expression>), // a <= b
    NotEqual(Box<Expression>, Box<Expression>),    // a != b

    Add(Box<Expression>, Box<Expression>),          // a + b
    Concat(Box<Expression>, Box<Expression>),       // a || b
    Divide(Box<Expression>, Box<Expression>),       // a / b
    Exponentiate(Box<Expression>, Box<Expression>), // a ** b
    Factorial(Box<Expression>),                     // a!
    Identity(Box<Expression>),                      // +a
    Multiply(Box<Expression>, Box<Expression>),     // a * b
    Negate(Box<Expression>),                        // -a
    Remainder(Box<Expression>, Box<Expression>),    // a % b
    Subtract(Box<Expression>, Box<Expression>),     // a - b

    BitwiseAnd(Box<Expression>, Box<Expression>), // a & b
    BitwiseOr(Box<Expression>, Box<Expression>),  // a | b
    BitwiseXor(Box<Expression>, Box<Expression>), // a ^ b
    BitwiseNot(Box<Expression>),                  // ~a
    BitwiseShiftLeft(Box<Expression>, Box<Expression>), // a << b
    BitwiseShiftRight(Box<Expression>, Box<Expression>), // a >> b

    ILike(Box<Expression>, Box<Expression>), // a ILIKE b
    Like(Box<Expression>, Box<Expression>),  // a LIKE b

    // IN and BETWEEN operators
    InList {
        expr: Box<Expression>,
        list: Vec<Expression>,
        negated: bool,
    }, // a IN (b, c, d) or a NOT IN (b, c, d)
    InSubquery {
        expr: Box<Expression>,
        subquery: Box<Expression>, // Should be Expression::Subquery
        negated: bool,
    }, // a IN (SELECT ...) or a NOT IN (SELECT ...)
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool,
    }, // a BETWEEN b AND c or a NOT BETWEEN b AND c

    // EXISTS operator
    Exists {
        subquery: Box<Expression>, // Should be Expression::Subquery
        negated: bool,
    }, // EXISTS (SELECT ...) or NOT EXISTS (SELECT ...)
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
    /// Convert an expression to a string suitable for use as a column name.
    /// This generates descriptive names for simple expressions and falls back
    /// to "expr" for complex cases where users should provide explicit aliases.
    pub fn to_column_name(&self) -> String {
        match self {
            Expression::Column(Some(table), col) => format!("{}.{}", table, col),
            Expression::Column(None, col) => col.clone(),

            Expression::Literal(Literal::Integer(i)) => i.to_string(),
            Expression::Literal(Literal::Float(f)) => f.to_string(),
            Expression::Literal(Literal::String(s)) => format!("'{}'", s),
            Expression::Literal(Literal::Null) => "NULL".to_string(),
            Expression::Literal(Literal::Boolean(b)) => {
                if *b { "true" } else { "false" }.to_string()
            }
            Expression::Literal(Literal::Date(d)) => format!("'{}'", d),
            Expression::Literal(Literal::Time(t)) => format!("'{}'", t),
            Expression::Literal(Literal::Timestamp(ts)) => format!("'{}'", ts),
            Expression::Literal(Literal::Bytea(_)) => "bytea".to_string(),
            Expression::Literal(Literal::Interval(_)) => "interval".to_string(),

            Expression::All => "*".to_string(),
            Expression::QualifiedWildcard(table) => format!("{}.*", table),

            Expression::Function(name, args) => {
                let arg_str = args
                    .iter()
                    .map(|a| a.to_column_name())
                    .collect::<Vec<_>>()
                    .join(", ");
                // Use uppercase for function names (SQL convention)
                format!("{}({})", name.to_uppercase(), arg_str)
            }

            Expression::Operator(op) => {
                match op {
                    Operator::Add(l, r) => {
                        format!("{} + {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Subtract(l, r) => {
                        format!("{} - {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Multiply(l, r) => {
                        format!("{} * {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Divide(l, r) => {
                        format!("{} / {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Remainder(l, r) => {
                        format!("{} % {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Exponentiate(l, r) => {
                        format!("{} ** {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Concat(l, r) => {
                        format!("{} || {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::BitwiseAnd(l, r) => {
                        format!("{} & {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::BitwiseOr(l, r) => {
                        format!("{} | {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::BitwiseXor(l, r) => {
                        format!("{} ^ {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::BitwiseShiftLeft(l, r) => {
                        format!("{} << {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::BitwiseShiftRight(l, r) => {
                        format!("{} >> {}", l.to_column_name(), r.to_column_name())
                    }
                    Operator::Not(e) => format!("NOT {}", e.to_column_name()),
                    Operator::Negate(e) => format!("-{}", e.to_column_name()),
                    Operator::Identity(e) => format!("+{}", e.to_column_name()),
                    Operator::BitwiseNot(e) => format!("~{}", e.to_column_name()),
                    _ => "expr".to_string(), // Complex operators get generic name
                }
            }

            Expression::Subquery(select) => {
                // Try to generate a reasonable name for simple scalar subqueries
                if select.select.len() == 1 && select.select[0].1.is_none() {
                    let inner_expr = &select.select[0].0;
                    format!("(SELECT {})", inner_expr.to_column_name())
                } else {
                    "(SELECT ...)".to_string()
                }
            }

            // All other expression types get generic "expr" name
            _ => "expr".to_string(),
        }
    }
}
