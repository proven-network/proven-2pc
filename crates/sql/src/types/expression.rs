//! SQL expression evaluation with transaction context for deterministic functions
//!
//! Adapted from toydb for proven-sql's PCC architecture.
//! Key changes:
//! - Added TransactionContext for deterministic function evaluation
//! - Integrated with our Value types (including UUID, Timestamp, Blob)

use super::value::Value;
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
}

impl Expression {
    /// Walks the expression tree, calling the visitor function for each node.
    pub fn walk(&self, visitor: &mut impl FnMut(&Expression) -> bool) -> bool {
        use Expression::*;

        if !visitor(self) {
            return false;
        }

        match self {
            Constant(_) | Column(_) | Parameter(_) | All => true,

            And(lhs, rhs)
            | Or(lhs, rhs)
            | Equal(lhs, rhs)
            | NotEqual(lhs, rhs)
            | GreaterThan(lhs, rhs)
            | GreaterThanOrEqual(lhs, rhs)
            | LessThan(lhs, rhs)
            | LessThanOrEqual(lhs, rhs)
            | Add(lhs, rhs)
            | Concat(lhs, rhs)
            | Subtract(lhs, rhs)
            | Multiply(lhs, rhs)
            | Divide(lhs, rhs)
            | Remainder(lhs, rhs)
            | Exponentiate(lhs, rhs)
            | Like(lhs, rhs) => lhs.walk(visitor) && rhs.walk(visitor),

            Not(expr) | Factorial(expr) | Identity(expr) | Negate(expr) | Is(expr, _) => {
                expr.walk(visitor)
            }

            Function(_, args) => args.iter().all(|arg| arg.walk(visitor)),

            ArrayAccess(base, index) => base.walk(visitor) && index.walk(visitor),

            FieldAccess(base, _) => base.walk(visitor),

            ArrayLiteral(elements) => elements.iter().all(|e| e.walk(visitor)),

            MapLiteral(pairs) => pairs
                .iter()
                .all(|(k, v)| k.walk(visitor) && v.walk(visitor)),

            InList(expr, list, _) => expr.walk(visitor) && list.iter().all(|e| e.walk(visitor)),

            Between(expr, low, high, _) => {
                expr.walk(visitor) && low.walk(visitor) && high.walk(visitor)
            }

            InSubquery(expr, _, _) => expr.walk(visitor),

            Exists(_, _) => true,

            Subquery(_) => true,
        }
    }

    /// Replaces column references using the given map.
    pub fn remap_columns(self, map: &[Option<usize>]) -> Self {
        use Expression::*;
        match self {
            Column(i) => map
                .get(i)
                .and_then(|i| i.map(Column))
                .unwrap_or(Constant(Value::Null)),
            Parameter(idx) => Parameter(idx),
            All => All,

            And(lhs, rhs) => And(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Or(lhs, rhs) => Or(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Not(expr) => Not(Box::new(expr.remap_columns(map))),

            Equal(lhs, rhs) => Equal(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            NotEqual(lhs, rhs) => NotEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            GreaterThan(lhs, rhs) => GreaterThan(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            GreaterThanOrEqual(lhs, rhs) => GreaterThanOrEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            LessThan(lhs, rhs) => LessThan(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            LessThanOrEqual(lhs, rhs) => LessThanOrEqual(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Is(expr, value) => Is(Box::new(expr.remap_columns(map)), value),

            Add(lhs, rhs) => Add(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Concat(lhs, rhs) => Concat(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Subtract(lhs, rhs) => Subtract(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Multiply(lhs, rhs) => Multiply(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Divide(lhs, rhs) => Divide(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Remainder(lhs, rhs) => Remainder(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),
            Exponentiate(lhs, rhs) => Exponentiate(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),

            Factorial(expr) => Factorial(Box::new(expr.remap_columns(map))),
            Identity(expr) => Identity(Box::new(expr.remap_columns(map))),
            Negate(expr) => Negate(Box::new(expr.remap_columns(map))),

            Like(lhs, rhs) => Like(
                Box::new(lhs.remap_columns(map)),
                Box::new(rhs.remap_columns(map)),
            ),

            Function(name, args) => Function(
                name,
                args.into_iter().map(|arg| arg.remap_columns(map)).collect(),
            ),

            ArrayAccess(base, index) => ArrayAccess(
                Box::new(base.remap_columns(map)),
                Box::new(index.remap_columns(map)),
            ),

            FieldAccess(base, field) => FieldAccess(Box::new(base.remap_columns(map)), field),

            ArrayLiteral(elements) => {
                ArrayLiteral(elements.into_iter().map(|e| e.remap_columns(map)).collect())
            }

            MapLiteral(pairs) => MapLiteral(
                pairs
                    .into_iter()
                    .map(|(k, v)| (k.remap_columns(map), v.remap_columns(map)))
                    .collect(),
            ),

            InList(expr, list, negated) => InList(
                Box::new(expr.remap_columns(map)),
                list.into_iter().map(|e| e.remap_columns(map)).collect(),
                negated,
            ),

            Between(expr, low, high, negated) => Between(
                Box::new(expr.remap_columns(map)),
                Box::new(low.remap_columns(map)),
                Box::new(high.remap_columns(map)),
                negated,
            ),

            InSubquery(expr, plan, negated) => {
                InSubquery(Box::new(expr.remap_columns(map)), plan, negated)
            }

            Exists(plan, negated) => Exists(plan, negated),

            Subquery(plan) => Subquery(plan),

            Constant(value) => Constant(value),
        }
    }
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
        }
    }
}

// Tests have been moved to execution/expression.rs since that's where evaluate() now lives
