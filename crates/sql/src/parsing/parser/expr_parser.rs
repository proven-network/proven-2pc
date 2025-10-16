//! Expression parser module
//!
//! Handles parsing of SQL expressions including operators, functions, literals,
//! and complex expressions like CASE/WHEN.

use super::super::{Keyword, Token};
use super::{dml_parser::DmlParser, literal_parser::LiteralParser, token_helper::TokenHelper};
use crate::error::{Error, Result};
use crate::parsing::ast::{Expression, Literal, Operator, SelectStatement};
use crate::types::data_type::DataType;
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
    Concat,             // a || b
    Divide,             // a / b
    Equal,              // a = b
    Exponentiate,       // a ^ b
    GreaterThan,        // a > b
    GreaterThanOrEqual, // a >= b
    LessThan,           // a < b
    LessThanOrEqual,    // a <= b
    ILike,              // a ILIKE b
    Like,               // a LIKE b
    Multiply,           // a * b
    NotEqual,           // a != b
    Or,                 // a OR b
    Remainder,          // a % b
    Subtract,           // a - b
    Xor,                // a XOR b
}

impl InfixOperator {
    /// The operator precedence.
    ///
    /// Mostly follows Postgres, except IS and LIKE having same precedence as =.
    /// This is similar to SQLite and MySQL.
    pub fn precedence(&self) -> Precedence {
        match self {
            Self::Or | Self::Xor => 1,
            Self::And => 2,
            // Self::Not => 3
            Self::Equal | Self::NotEqual | Self::ILike | Self::Like => 4, // also Self::Is
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 5,
            Self::Add | Self::Concat | Self::Subtract => 6,
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
            Self::Concat => Operator::Concat(lhs, rhs).into(),
            Self::Divide => Operator::Divide(lhs, rhs).into(),
            Self::Equal => Operator::Equal(lhs, rhs).into(),
            Self::Exponentiate => Operator::Exponentiate(lhs, rhs).into(),
            Self::GreaterThan => Operator::GreaterThan(lhs, rhs).into(),
            Self::GreaterThanOrEqual => Operator::GreaterThanOrEqual(lhs, rhs).into(),
            Self::LessThan => Operator::LessThan(lhs, rhs).into(),
            Self::LessThanOrEqual => Operator::LessThanOrEqual(lhs, rhs).into(),
            Self::ILike => Operator::ILike(lhs, rhs).into(),
            Self::Like => Operator::Like(lhs, rhs).into(),
            Self::Multiply => Operator::Multiply(lhs, rhs).into(),
            Self::NotEqual => Operator::NotEqual(lhs, rhs).into(),
            Self::Or => Operator::Or(lhs, rhs).into(),
            Self::Remainder => Operator::Remainder(lhs, rhs).into(),
            Self::Subtract => Operator::Subtract(lhs, rhs).into(),
            Self::Xor => Operator::Xor(lhs, rhs).into(),
        }
    }
}

/// Postfix operators.
pub enum PostfixOperator {
    Factorial,                              // a!
    Is(Literal),                            // a IS NULL | NAN
    IsNot(Literal),                         // a IS NOT NULL | NAN
    InList(Vec<Expression>, bool),          // a IN (list) or a NOT IN (list)
    InSubquery(Box<SelectStatement>, bool), // a IN (SELECT...) or a NOT IN (SELECT...)
    Between(Expression, Expression, bool),  // a BETWEEN low AND high or a NOT BETWEEN low AND high
    ArrayAccess(Expression),                // a[index]
    FieldAccess(String),                    // a.field
}

impl PostfixOperator {
    // The operator precedence.
    pub fn precedence(&self) -> Precedence {
        match self {
            Self::Is(_)
            | Self::IsNot(_)
            | Self::InList(_, _)
            | Self::InSubquery(_, _)
            | Self::Between(_, _, _) => 4,
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
            Self::InSubquery(select, negated) => Operator::InSubquery {
                expr: lhs,
                subquery: Box::new(Expression::Subquery(select)),
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

/// Parser trait for SQL expressions
pub trait ExpressionParser: TokenHelper + LiteralParser + DmlParser {
    /// Increment and return the new parameter count
    fn increment_param_count(&mut self) -> u32;

    /// Parse a type (needed for CAST expressions)
    fn parse_type(&mut self) -> Result<DataType>;

    /// Parse an expression using the precedence climbing algorithm.
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_expression_at(0)
    }

    /// Parses an expression at the given minimum precedence.
    fn parse_expression_at(&mut self, min_precedence: Precedence) -> Result<Expression> {
        // If the left-hand side is a prefix operator, recursively parse it and
        // its operand. Otherwise, parse the left-hand side as an atom.
        let mut lhs = if let Some(prefix) = self.parse_prefix_operator_at(min_precedence) {
            let next_precedence = prefix.precedence() + prefix.associativity();
            let rhs = self.parse_expression_at(next_precedence)?;
            prefix.into_expression(rhs)
        } else {
            self.parse_expression_atom()?
        };

        // Apply any postfix operators to the left-hand side.
        while let Some(postfix) = self.parse_postfix_operator_at(min_precedence)? {
            lhs = postfix.into_expression(lhs)
        }

        // Repeatedly apply any infix operators to the left-hand side as long as
        // their precedence is greater than or equal to the current minimum
        // precedence (i.e. that of the upstack operator).
        //
        // The right-hand side expression parsing will recursively apply any
        // infix operators at or above this operator's precedence to the
        // right-hand side.
        while let Some(infix) = self.parse_infix_operator_at(min_precedence) {
            let next_precedence = infix.precedence() + infix.associativity();
            let rhs = self.parse_expression_at(next_precedence)?;
            lhs = infix.into_expression(lhs, rhs);
        }

        // Apply any postfix operators after the binary operator. Consider e.g.
        // 1 + NULL IS NULL.
        while let Some(postfix) = self.parse_postfix_operator_at(min_precedence)? {
            lhs = postfix.into_expression(lhs)
        }

        Ok(lhs)
    }

    /// Parses an expression atom.
    fn parse_expression_atom(&mut self) -> Result<Expression> {
        let token = self.next()?;

        // Try to parse as a simple literal first
        if let Some(expr) = self.parse_literal(token.clone())? {
            return Ok(expr);
        }

        Ok(match token {
            // All columns.
            Token::Asterisk => Expression::All,

            // NOT EXISTS (SELECT ...)
            Token::Keyword(Keyword::Not)
                if matches!(self.peek()?, Some(Token::Keyword(Keyword::Exists))) =>
            {
                self.expect(Keyword::Exists.into())?;
                self.expect(Token::OpenParen)?;
                if self.peek()? != Some(&Token::Keyword(Keyword::Select)) {
                    return Err(Error::ParseError(
                        "NOT EXISTS must be followed by a subquery (SELECT)".into(),
                    ));
                }
                let (distinct, select) = self.parse_select_clause()?;
                let select = Box::new(SelectStatement {
                    distinct,
                    select,
                    from: self.parse_from_clause()?,
                    r#where: self.parse_where_clause()?,
                    group_by: self.parse_group_by_clause()?,
                    having: self.parse_having_clause()?,
                    order_by: self.parse_order_by_clause()?,
                    limit: self.parse_limit_clause()?,
                    offset: self.parse_offset_clause()?,
                });
                self.expect(Token::CloseParen)?;
                Operator::Exists {
                    subquery: Box::new(Expression::Subquery(select)),
                    negated: true,
                }
                .into()
            }

            // DATE literal: DATE 'YYYY-MM-DD' or column name "date"
            Token::Keyword(Keyword::Date) => {
                // Check if this is a DATE literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => self.parse_date_literal()?,
                    _ => Expression::Column(None, "date".to_string()),
                }
            }

            // TIME literal: TIME 'HH:MM:SS' or column name "time"
            Token::Keyword(Keyword::Time) => {
                // Check if this is a TIME literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => self.parse_time_literal()?,
                    _ => Expression::Column(None, "time".to_string()),
                }
            }

            // TIMESTAMP literal: TIMESTAMP 'YYYY-MM-DD HH:MM:SS' or column name "timestamp"
            Token::Keyword(Keyword::Timestamp) => {
                // Check if this is a TIMESTAMP literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => self.parse_timestamp_literal()?,
                    _ => Expression::Column(None, "timestamp".to_string()),
                }
            }

            // INTERVAL literal: INTERVAL '1' DAY/MONTH/YEAR or column name "interval"
            Token::Keyword(Keyword::Interval) => {
                // Check if this is an INTERVAL literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => self.parse_interval_literal()?,
                    _ => Expression::Column(None, "interval".to_string()),
                }
            }

            // CURRENT_DATE - SQL standard keyword (no parentheses required)
            Token::Keyword(Keyword::CurrentDate) => {
                // Allow optional parentheses for compatibility: CURRENT_DATE or CURRENT_DATE()
                if self.peek()? == Some(&Token::OpenParen) {
                    self.next()?; // consume (
                    self.expect(Token::CloseParen)?;
                }
                Expression::Function("CURRENT_DATE".to_string(), vec![])
            }

            // CURRENT_TIME - SQL standard keyword (no parentheses required)
            Token::Keyword(Keyword::CurrentTime) => {
                // Allow optional parentheses for compatibility: CURRENT_TIME or CURRENT_TIME()
                if self.peek()? == Some(&Token::OpenParen) {
                    self.next()?; // consume (
                    self.expect(Token::CloseParen)?;
                }
                Expression::Function("CURRENT_TIME".to_string(), vec![])
            }

            // CURRENT_TIMESTAMP - SQL standard keyword (no parentheses required)
            Token::Keyword(Keyword::CurrentTimestamp) => {
                // Allow optional parentheses for compatibility: CURRENT_TIMESTAMP or CURRENT_TIMESTAMP()
                if self.peek()? == Some(&Token::OpenParen) {
                    self.next()?; // consume (
                    self.expect(Token::CloseParen)?;
                }
                Expression::Function("CURRENT_TIMESTAMP".to_string(), vec![])
            }

            // EXISTS (SELECT ...)
            Token::Keyword(Keyword::Exists) => {
                self.expect(Token::OpenParen)?;
                if self.peek()? != Some(&Token::Keyword(Keyword::Select)) {
                    return Err(Error::ParseError(
                        "EXISTS must be followed by a subquery (SELECT)".into(),
                    ));
                }
                let (distinct, select) = self.parse_select_clause()?;
                let select = Box::new(SelectStatement {
                    distinct,
                    select,
                    from: self.parse_from_clause()?,
                    r#where: self.parse_where_clause()?,
                    group_by: self.parse_group_by_clause()?,
                    having: self.parse_having_clause()?,
                    order_by: self.parse_order_by_clause()?,
                    limit: self.parse_limit_clause()?,
                    offset: self.parse_offset_clause()?,
                });
                self.expect(Token::CloseParen)?;
                Operator::Exists {
                    subquery: Box::new(Expression::Subquery(select)),
                    negated: false,
                }
                .into()
            }

            // CASE expression: CASE [expr] WHEN ... THEN ... [ELSE ...] END
            Token::Keyword(Keyword::Case) => {
                // Check if this is a simple CASE (with operand) or searched CASE
                let operand = if matches!(self.peek()?, Some(Token::Keyword(Keyword::When))) {
                    None
                } else {
                    Some(Box::new(<Self as ExpressionParser>::parse_expression(
                        self,
                    )?))
                };

                // Parse WHEN clauses
                let mut when_clauses = Vec::new();
                while self.next_is(Token::Keyword(Keyword::When)) {
                    let when_expr = <Self as ExpressionParser>::parse_expression(self)?;
                    self.expect(Token::Keyword(Keyword::Then))?;
                    let then_expr = <Self as ExpressionParser>::parse_expression(self)?;
                    when_clauses.push((when_expr, then_expr));
                }

                if when_clauses.is_empty() {
                    return Err(Error::ParseError(
                        "CASE expression must have at least one WHEN clause".into(),
                    ));
                }

                // Parse optional ELSE clause
                let else_clause = if self.next_is(Token::Keyword(Keyword::Else)) {
                    Some(Box::new(<Self as ExpressionParser>::parse_expression(
                        self,
                    )?))
                } else {
                    None
                };

                self.expect(Token::Keyword(Keyword::End))?;

                Expression::Case {
                    operand,
                    when_clauses,
                    else_clause,
                }
            }

            // ARRAY literal: ARRAY[1, 2, 3]
            Token::Keyword(Keyword::Array) => {
                self.expect(Token::OpenBracket)?;
                let mut elements = Vec::new();
                if self.peek()? != Some(&Token::CloseBracket) {
                    elements.push(<Self as ExpressionParser>::parse_expression(self)?);
                    while self.next_is(Token::Comma) {
                        elements.push(<Self as ExpressionParser>::parse_expression(self)?);
                    }
                }
                self.expect(Token::CloseBracket)?;
                Expression::ArrayLiteral(elements)
            }

            // CAST expression: CAST(expr AS type)
            Token::Keyword(Keyword::Cast) => {
                self.expect(Token::OpenParen)?;
                let expr = <Self as ExpressionParser>::parse_expression(self)?;
                self.expect(Token::Keyword(Keyword::As))?;

                // Parse the target type using parse_type() to support complex types
                let target_type = self.parse_type()?;

                // Convert the DataType to a string representation for cast_value
                let type_string = data_type_to_cast_string(&target_type);

                self.expect(Token::CloseParen)?;

                // Use the Function expression with a special CAST function name
                // We'll encode the type as the second argument
                Expression::Function(
                    "CAST".to_string(),
                    vec![expr, Expression::Literal(Literal::String(type_string))],
                )
            }

            // EXTRACT expression: EXTRACT(field FROM source)
            Token::Keyword(Keyword::Extract) => {
                self.expect(Token::OpenParen)?;

                // Parse the datetime field (singular or plural forms accepted)
                let field = match self.next()? {
                    Token::Keyword(Keyword::Year) | Token::Keyword(Keyword::Years) => "YEAR",
                    Token::Keyword(Keyword::Month) | Token::Keyword(Keyword::Months) => "MONTH",
                    Token::Keyword(Keyword::Day) | Token::Keyword(Keyword::Days) => "DAY",
                    Token::Keyword(Keyword::Hour) | Token::Keyword(Keyword::Hours) => "HOUR",
                    Token::Keyword(Keyword::Minute) | Token::Keyword(Keyword::Minutes) => "MINUTE",
                    Token::Keyword(Keyword::Second) | Token::Keyword(Keyword::Seconds) => "SECOND",
                    other => {
                        return Err(Error::ParseError(format!(
                            "expected datetime field (YEAR/YEARS, MONTH/MONTHS, DAY/DAYS, HOUR/HOURS, MINUTE/MINUTES, SECOND/SECONDS), found {}",
                            other
                        )));
                    }
                };

                self.expect(Token::Keyword(Keyword::From))?;

                // Parse the source expression (timestamp, date, time, or interval)
                let source = <Self as ExpressionParser>::parse_expression(self)?;

                self.expect(Token::CloseParen)?;

                // Use the Function expression with EXTRACT function name
                // We'll encode the field as a string literal as the first argument
                Expression::Function(
                    "EXTRACT".to_string(),
                    vec![
                        Expression::Literal(Literal::String(field.to_string())),
                        source,
                    ],
                )
            }

            // TRIM expression: TRIM([[LEADING | TRAILING | BOTH] [removal_chars] FROM] source)
            Token::Keyword(Keyword::Trim) => {
                self.expect(Token::OpenParen)?;

                // Check for optional trim specification (LEADING, TRAILING, BOTH)
                let trim_spec = match self.peek()? {
                    Some(Token::Keyword(Keyword::Leading)) => {
                        self.next()?;
                        Some("LEADING")
                    }
                    Some(Token::Keyword(Keyword::Trailing)) => {
                        self.next()?;
                        Some("TRAILING")
                    }
                    Some(Token::Keyword(Keyword::Both)) => {
                        self.next()?;
                        Some("BOTH")
                    }
                    _ => None,
                };

                // Now we need to determine if there's a removal_chars expression and a FROM clause
                // We parse the next expression, then check if FROM follows
                let first_expr = <Self as ExpressionParser>::parse_expression(self)?;

                // Check if FROM keyword follows
                if self.next_is(Token::Keyword(Keyword::From)) {
                    // The first_expr was the removal_chars, now parse the source
                    let source = <Self as ExpressionParser>::parse_expression(self)?;
                    self.expect(Token::CloseParen)?;

                    // Build arguments: [trim_spec, removal_chars, source]
                    let mut args = Vec::new();
                    if let Some(spec) = trim_spec {
                        args.push(Expression::Literal(Literal::String(spec.to_string())));
                    } else {
                        // Default to BOTH if no spec given
                        args.push(Expression::Literal(Literal::String("BOTH".to_string())));
                    }
                    args.push(first_expr);
                    args.push(source);

                    Expression::Function("TRIM".to_string(), args)
                } else {
                    // No FROM, so first_expr is the source to trim
                    self.expect(Token::CloseParen)?;

                    // Build arguments based on whether we have a trim_spec
                    let mut args = Vec::new();
                    if let Some(spec) = trim_spec {
                        // TRIM(LEADING expr) or TRIM(TRAILING expr) or TRIM(BOTH expr)
                        // This means trim whitespace with the given spec
                        args.push(Expression::Literal(Literal::String(spec.to_string())));
                    }
                    args.push(first_expr);

                    Expression::Function("TRIM".to_string(), args)
                }
            }

            // Function call.
            Token::Ident(name) if self.next_is(Token::OpenParen) => {
                let mut args = Vec::new();

                // Check for DISTINCT keyword in aggregate functions
                let is_distinct = if matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDEV" | "VARIANCE"
                ) {
                    if let Ok(Some(Token::Keyword(Keyword::Distinct))) = self.peek() {
                        let _ = self.next(); // consume DISTINCT
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                while !self.next_is(Token::CloseParen) {
                    if !args.is_empty() {
                        self.expect(Token::Comma)?;
                    }
                    args.push(<Self as ExpressionParser>::parse_expression(self)?);
                }

                // If DISTINCT was used, create a special function name
                if is_distinct {
                    Expression::Function(format!("{}_DISTINCT", name.to_uppercase()), args)
                } else {
                    Expression::Function(name, args)
                }
            }

            // Column name, either qualified as table.column or table.* or unqualified.
            Token::Ident(table) if self.next_is(Token::Period) => {
                // Check if next token is * for qualified wildcard
                if matches!(self.peek()?, Some(Token::Asterisk)) {
                    self.next()?; // consume the *
                    Expression::QualifiedWildcard(table)
                } else {
                    Expression::Column(Some(table), self.next_ident()?)
                }
            }
            Token::Ident(column) => Expression::Column(None, column),

            // Parameter placeholder (?)
            Token::Question => {
                let param_idx = self.increment_param_count();
                Expression::Parameter(param_idx as usize)
            }

            // Parenthesized expression.
            // Nested expression or subquery
            Token::OpenParen => {
                // Check if this is a subquery (starts with SELECT)
                if self.peek()? == Some(&Token::Keyword(Keyword::Select)) {
                    // Parse subquery
                    let (distinct, select) = self.parse_select_clause()?;
                    let select = Box::new(SelectStatement {
                        distinct,
                        select,
                        from: self.parse_from_clause()?,
                        r#where: self.parse_where_clause()?,
                        group_by: self.parse_group_by_clause()?,
                        having: self.parse_having_clause()?,
                        order_by: self.parse_order_by_clause()?,
                        limit: self.parse_limit_clause()?,
                        offset: self.parse_offset_clause()?,
                    });
                    self.expect(Token::CloseParen)?;
                    Expression::Subquery(select)
                } else {
                    // Regular nested expression
                    let expr = <Self as ExpressionParser>::parse_expression(self)?;
                    self.expect(Token::CloseParen)?;
                    expr
                }
            }

            // Array literal: [1, 2, 3]
            Token::OpenBracket => {
                let mut elements = Vec::new();
                if self.peek()? != Some(&Token::CloseBracket) {
                    elements.push(<Self as ExpressionParser>::parse_expression(self)?);
                    while self.next_is(Token::Comma) {
                        elements.push(<Self as ExpressionParser>::parse_expression(self)?);
                    }
                }
                self.expect(Token::CloseBracket)?;
                Expression::ArrayLiteral(elements)
            }

            // Map literal: {key1: value1, key2: value2}
            Token::OpenBrace => {
                let mut pairs = Vec::new();
                if self.peek()? != Some(&Token::CloseBrace) {
                    // Parse first key-value pair
                    let key = <Self as ExpressionParser>::parse_expression(self)?;
                    self.expect(Token::Colon)?;
                    let value = <Self as ExpressionParser>::parse_expression(self)?;
                    pairs.push((key, value));

                    // Parse remaining pairs
                    while self.next_is(Token::Comma) {
                        let key = <Self as ExpressionParser>::parse_expression(self)?;
                        self.expect(Token::Colon)?;
                        let value = <Self as ExpressionParser>::parse_expression(self)?;
                        pairs.push((key, value));
                    }
                }
                self.expect(Token::CloseBrace)?;
                Expression::MapLiteral(pairs)
            }

            // Anything else is an error
            token => {
                return Err(Error::ParseError(format!(
                    "expected expression atom, found {}",
                    token
                )));
            }
        })
    }

    /// Parses a prefix operator, if there is one and its precedence is at least
    /// min_precedence.
    fn parse_prefix_operator_at(&mut self, min_precedence: Precedence) -> Option<PrefixOperator> {
        self.next_if_map(|token| {
            let operator = match token {
                Token::Keyword(Keyword::Not) => PrefixOperator::Not,
                Token::Minus => PrefixOperator::Minus,
                Token::Plus => PrefixOperator::Plus,
                _ => return None,
            };
            Some(operator).filter(|op| op.precedence() >= min_precedence)
        })
    }

    /// Parses an infix operator, if there is one and its precedence is at least
    /// min_precedence.
    fn parse_infix_operator_at(&mut self, min_precedence: Precedence) -> Option<InfixOperator> {
        self.next_if_map(|token| {
            let operator = match token {
                Token::Asterisk => InfixOperator::Multiply,
                Token::Caret => InfixOperator::Exponentiate,
                Token::Concat => InfixOperator::Concat,
                Token::Equal => InfixOperator::Equal,
                Token::GreaterThan => InfixOperator::GreaterThan,
                Token::GreaterThanOrEqual => InfixOperator::GreaterThanOrEqual,
                Token::Keyword(Keyword::And) => InfixOperator::And,
                Token::Keyword(Keyword::ILike) => InfixOperator::ILike,
                Token::Keyword(Keyword::Like) => InfixOperator::Like,
                Token::Keyword(Keyword::Or) => InfixOperator::Or,
                Token::Keyword(Keyword::Xor) => InfixOperator::Xor,
                Token::LessOrGreaterThan => InfixOperator::NotEqual,
                Token::LessThan => InfixOperator::LessThan,
                Token::LessThanOrEqual => InfixOperator::LessThanOrEqual,
                Token::Minus => InfixOperator::Subtract,
                Token::NotEqual => InfixOperator::NotEqual,
                Token::Percent => InfixOperator::Remainder,
                Token::Plus => InfixOperator::Add,
                Token::Slash => InfixOperator::Divide,
                _ => return None,
            };
            Some(operator).filter(|op| op.precedence() >= min_precedence)
        })
    }

    /// Parses a postfix operator, if there is one and its precedence is at
    /// least min_precedence.
    fn parse_postfix_operator_at(
        &mut self,
        min_precedence: Precedence,
    ) -> Result<Option<PostfixOperator>> {
        // Handle IS (NOT) NULL/NAN separately, since it's multiple tokens.
        if self.peek()? == Some(&Token::Keyword(Keyword::Is)) {
            // We can't consume tokens unless the precedence is satisfied, so we
            // assume IS NULL (they all have the same precedence).
            if PostfixOperator::Is(Literal::Null).precedence() < min_precedence {
                return Ok(None);
            }
            self.expect(Keyword::Is.into())?;
            let not = self.next_is(Keyword::Not.into());
            let value = match self.next()? {
                Token::Keyword(Keyword::NaN) => Literal::Float(f64::NAN),
                Token::Keyword(Keyword::Null) => Literal::Null,
                token => return Err(Error::ParseError(format!("unexpected token {}", token))),
            };
            let operator = match not {
                false => PostfixOperator::Is(value),
                true => PostfixOperator::IsNot(value),
            };
            return Ok(Some(operator));
        }

        // Handle (NOT) IN and (NOT) BETWEEN
        // Check if we have NOT followed by IN or BETWEEN
        let negated = if self.peek()? == Some(&Token::Keyword(Keyword::Not)) {
            // Consume NOT and check next token
            self.next()?;

            // If next token is IN or BETWEEN, this is negated form
            // Otherwise we consumed NOT incorrectly - but since we're in postfix position
            // after an expression, NOT shouldn't appear here anyway except for NOT IN/BETWEEN
            match self.peek()? {
                Some(&Token::Keyword(Keyword::In)) | Some(&Token::Keyword(Keyword::Between)) => {
                    true
                }
                _ => {
                    return Err(Error::ParseError("expected IN or BETWEEN after NOT".into()));
                }
            }
        } else {
            false
        };

        // Handle IN
        if self.peek()? == Some(&Token::Keyword(Keyword::In)) {
            // Check precedence before consuming
            if PostfixOperator::InList(vec![], false).precedence() < min_precedence {
                return Ok(None);
            }
            self.expect(Keyword::In.into())?;
            self.expect(Token::OpenParen)?;

            // Check if this is a subquery (starts with SELECT) or a list
            if self.peek()? == Some(&Token::Keyword(Keyword::Select)) {
                // Parse subquery
                let (distinct, select) = self.parse_select_clause()?;
                let select = Box::new(SelectStatement {
                    distinct,
                    select,
                    from: self.parse_from_clause()?,
                    r#where: self.parse_where_clause()?,
                    group_by: self.parse_group_by_clause()?,
                    having: self.parse_having_clause()?,
                    order_by: self.parse_order_by_clause()?,
                    limit: self.parse_limit_clause()?,
                    offset: self.parse_offset_clause()?,
                });
                self.expect(Token::CloseParen)?;
                return Ok(Some(PostfixOperator::InSubquery(select, negated)));
            }

            // Handle empty list or expression list
            let mut list = Vec::new();
            if self.peek()? != Some(&Token::CloseParen) {
                // Parse first expression
                list.push(<Self as ExpressionParser>::parse_expression(self)?);
                // Parse remaining expressions
                while self.next_is(Token::Comma) {
                    list.push(<Self as ExpressionParser>::parse_expression(self)?);
                }
            }
            self.expect(Token::CloseParen)?;

            return Ok(Some(PostfixOperator::InList(list, negated)));
        }

        // Handle BETWEEN
        if self.peek()? == Some(&Token::Keyword(Keyword::Between)) {
            // Check precedence before consuming
            if PostfixOperator::Between(
                Expression::Literal(Literal::Null),
                Expression::Literal(Literal::Null),
                false,
            )
            .precedence()
                < min_precedence
            {
                return Ok(None);
            }
            self.expect(Keyword::Between.into())?;
            // Parse low value - use higher precedence to stop at AND
            // AND has precedence 2, so we use 3 to stop before it
            let low = self.parse_expression_at(3)?;
            self.expect(Keyword::And.into())?;
            // Parse high value - also use higher precedence to be consistent
            let high = self.parse_expression_at(3)?;

            return Ok(Some(PostfixOperator::Between(low, high, negated)));
        }

        // Handle bracket notation for array/list/map access
        if self.peek()? == Some(&Token::OpenBracket) {
            // Check precedence before consuming
            if PostfixOperator::ArrayAccess(Expression::Literal(Literal::Null)).precedence()
                < min_precedence
            {
                return Ok(None);
            }
            self.expect(Token::OpenBracket)?;
            let index = <Self as ExpressionParser>::parse_expression(self)?;
            self.expect(Token::CloseBracket)?;
            return Ok(Some(PostfixOperator::ArrayAccess(index)));
        }

        // Handle dot notation for struct field access
        if self.peek()? == Some(&Token::Period) {
            // Check precedence before consuming
            if PostfixOperator::FieldAccess(String::new()).precedence() < min_precedence {
                return Ok(None);
            }
            self.expect(Token::Period)?;
            let field = self.next_ident()?;
            return Ok(Some(PostfixOperator::FieldAccess(field)));
        }

        Ok(self.next_if_map(|token| {
            let operator = match token {
                Token::Exclamation => PostfixOperator::Factorial,
                _ => return None,
            };
            Some(operator).filter(|op| op.precedence() >= min_precedence)
        }))
    }
}

/// Helper function to convert DataType to a cast string.
fn data_type_to_cast_string(data_type: &DataType) -> String {
    // This should match the reverse of what's expected in cast_value
    match data_type {
        DataType::Bool => "BOOLEAN".to_string(),
        DataType::I8 => "TINYINT".to_string(),
        DataType::I16 => "SMALLINT".to_string(),
        DataType::I32 => "INT".to_string(),
        DataType::I64 => "BIGINT".to_string(),
        DataType::I128 => "HUGEINT".to_string(),
        DataType::U8 => "TINYINT UNSIGNED".to_string(),
        DataType::U16 => "SMALLINT UNSIGNED".to_string(),
        DataType::U32 => "INT UNSIGNED".to_string(),
        DataType::U64 => "BIGINT UNSIGNED".to_string(),
        DataType::U128 => "HUGEINT UNSIGNED".to_string(),
        DataType::F32 => "REAL".to_string(),
        DataType::F64 => "DOUBLE".to_string(),
        DataType::Text => "TEXT".to_string(),
        DataType::Str => "STRING".to_string(),
        DataType::Bytea => "BYTEA".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Uuid => "UUID".to_string(),
        DataType::Array(inner, size) => {
            if let Some(s) = size {
                format!("{}[{}]", data_type_to_cast_string(inner), s)
            } else {
                format!("{}[]", data_type_to_cast_string(inner))
            }
        }
        DataType::List(inner) => format!("{}[]", data_type_to_cast_string(inner)),
        DataType::Map(key, value) => format!(
            "MAP<{}, {}>",
            data_type_to_cast_string(key),
            data_type_to_cast_string(value)
        ),
        DataType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(name, dt)| format!("{}: {}", name, data_type_to_cast_string(dt)))
                .collect();
            format!("STRUCT<{}>", field_strs.join(", "))
        }
        DataType::Decimal(p, s) => match (p, s) {
            (Some(precision), Some(scale)) => format!("DECIMAL({}, {})", precision, scale),
            (Some(precision), None) => format!("DECIMAL({})", precision),
            _ => "DECIMAL".to_string(),
        },
        DataType::Inet => "INET".to_string(),
        DataType::Point => "POINT".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Nullable(inner) => format!("{}?", data_type_to_cast_string(inner)),
        DataType::Null => "NULL".to_string(),
    }
}
