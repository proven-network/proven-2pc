//! Modular SQL parser implementation
//!
//! The parser is split into several modules for better organization:
//! - expr_parser: Expression parsing with operator precedence
//! - type_parser: Data type parsing
//! - ddl_parser: DDL statement parsing (CREATE, DROP)
//! - dml_parser: DML statement parsing (SELECT, INSERT, UPDATE, DELETE)
//! - common: Shared utilities

pub mod ddl_parser;
pub mod dml_parser;
pub mod expr_parser;
pub mod type_parser;

use std::iter::Peekable;

use self::ddl_parser::DdlParser;
use self::dml_parser::DmlParser;
use self::expr_parser::{InfixOperator, PostfixOperator, Precedence, PrefixOperator};
use self::type_parser::{TypeParser, data_type_to_cast_string};
use super::ast::{Expression, Literal, Statement};
use super::{Keyword, Lexer, Token};
use crate::error::{Error, Result};

/// The SQL parser takes tokens from the lexer and parses the SQL syntax into an
/// Abstract Syntax Tree (AST).
///
/// The AST represents the syntactic structure of a SQL query (e.g. the SELECT
/// and FROM clauses, values, arithmetic expressions, etc.). However, it only
/// ensures the syntax is well-formed, and does not know whether e.g. a given
/// table or column exists or which kind of join to use -- that is the job of
/// the planner.
pub struct Parser<'a> {
    pub lexer: Peekable<Lexer<'a>>,
    /// Counter for parameter placeholders (?)
    param_count: usize,
}

impl Parser<'_> {
    /// Parses the input string into a SQL statement AST. The entire string must
    /// be parsed as a single statement, ending with an optional semicolon.
    pub fn parse(statement: &str) -> Result<Statement> {
        let mut parser = Self::new(statement);
        let statement = parser.parse_statement()?;
        parser.skip(Token::Semicolon);
        if let Some(token) = parser.lexer.next().transpose()? {
            return Err(Error::ParseError(format!("unexpected token {}", token)));
        }
        Ok(statement)
    }

    /// Parse the input string into a SQL expression AST. The entire string must
    /// be parsed as a single expression. Only used in tests.
    #[cfg(test)]
    pub fn parse_expr(expr: &str) -> Result<Expression> {
        let mut parser = Self::new(expr);
        let expression = parser.parse_expression()?;
        if let Some(token) = parser.lexer.next().transpose()? {
            return Err(Error::ParseError(format!("unexpected token {}", token)));
        }
        Ok(expression)
    }

    /// Creates a new parser for the given raw SQL string.
    fn new(input: &str) -> Parser<'_> {
        Parser {
            lexer: Lexer::new(input).peekable(),
            param_count: 0,
        }
    }

    /// Fetches the next lexer token, or errors if none is found.
    fn next(&mut self) -> Result<Token> {
        self.lexer
            .next()
            .transpose()?
            .ok_or_else(|| Error::ParseError("unexpected end of input".into()))
    }

    /// Returns the next identifier, or errors if not found.
    fn next_ident(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::ParseError(format!(
                "expected identifier, got {}",
                token
            ))),
        }
    }

    /// Returns the next identifier or keyword as identifier (for contexts where keywords can be identifiers).
    fn next_ident_or_keyword(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            // Allow certain keywords to be used as identifiers
            Token::Keyword(Keyword::Text) => Ok("text".to_string()),
            Token::Keyword(Keyword::String) => Ok("string".to_string()),
            Token::Keyword(Keyword::Int) => Ok("int".to_string()),
            Token::Keyword(Keyword::Integer) => Ok("integer".to_string()),
            Token::Keyword(Keyword::Bool) => Ok("bool".to_string()),
            Token::Keyword(Keyword::Boolean) => Ok("boolean".to_string()),
            Token::Keyword(Keyword::Float) => Ok("float".to_string()),
            Token::Keyword(Keyword::Double) => Ok("double".to_string()),
            Token::Keyword(Keyword::Varchar) => Ok("varchar".to_string()),
            Token::Keyword(Keyword::Date) => Ok("date".to_string()),
            Token::Keyword(Keyword::Time) => Ok("time".to_string()),
            Token::Keyword(Keyword::Timestamp) => Ok("timestamp".to_string()),
            Token::Keyword(Keyword::Interval) => Ok("interval".to_string()),
            token => Err(Error::ParseError(format!(
                "expected identifier, got {}",
                token
            ))),
        }
    }

    /// Returns the next lexer token if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.peek().ok()?.filter(|t| predicate(t))?;
        self.next().ok()
    }

    /// Passes the next lexer token through the closure, consuming it if the
    /// closure returns Some. Returns the result of the closure.
    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Option<T> {
        self.peek().ok()?.map(f)?.inspect(|_| drop(self.next()))
    }

    /// Returns the next keyword if there is one.
    fn next_if_keyword(&mut self) -> Option<Keyword> {
        self.next_if_map(|token| match token {
            Token::Keyword(keyword) => Some(*keyword),
            _ => None,
        })
    }

    /// Returns true if the next token is an identifier matching the given string (case-insensitive)
    fn next_if_ident_eq(&mut self, expected: &str) -> bool {
        self.next_if_map(|token| match token {
            Token::Ident(s) if s.to_uppercase() == expected.to_uppercase() => Some(()),
            _ => None,
        })
        .is_some()
    }

    /// Consumes the next lexer token if it is the given token, returning true.
    fn next_is(&mut self, token: Token) -> bool {
        self.next_if(|t| t == &token).is_some()
    }

    /// Consumes the next lexer token if it's the expected token, or errors.
    fn expect(&mut self, expect: Token) -> Result<()> {
        let token = self.next()?;
        if token != expect {
            return Err(Error::ParseError(format!(
                "expected token {}, found {}",
                expect, token
            )));
        }
        Ok(())
    }

    /// Consumes the next lexer token if it is the given token. Equivalent to
    /// next_is(), but expresses intent better.
    fn skip(&mut self, token: Token) {
        self.next_is(token);
    }

    /// Peeks the next lexer token if any, but transposes it for convenience.
    fn peek(&mut self) -> Result<Option<&Token>> {
        self.lexer
            .peek()
            .map(|r| r.as_ref().map_err(|err| err.clone()))
            .transpose()
    }

    /// Parses a SQL statement.
    fn parse_statement(&mut self) -> Result<Statement> {
        let Some(token) = self.peek()? else {
            return Err(Error::ParseError("unexpected end of input".into()));
        };
        match token {
            Token::Keyword(Keyword::Explain) => self.parse_explain(),

            Token::Keyword(Keyword::Create) => self.parse_create(),
            Token::Keyword(Keyword::Drop) => self.parse_drop(),

            Token::Keyword(Keyword::Delete) => self.parse_delete(),
            Token::Keyword(Keyword::Insert) => self.parse_insert(),
            Token::Keyword(Keyword::Select) => self.parse_select(),
            Token::Keyword(Keyword::Update) => self.parse_update(),

            token => Err(Error::ParseError(format!("unexpected token {}", token))),
        }
    }

    /// Parses an EXPLAIN statement.
    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(Keyword::Explain.into())?;
        if self.next_is(Keyword::Explain.into()) {
            return Err(Error::ParseError("cannot nest EXPLAIN statements".into()));
        }
        Ok(Statement::Explain(Box::new(self.parse_statement()?)))
    }

    /// Parses an expression using the precedence climbing algorithm. See:
    ///
    /// <https://en.wikipedia.org/wiki/Operator-precedence_parser#Precedence_climbing_method>
    /// <https://eli.thegreenplace.net/2012/08/02/parsing-expressions-by-precedence-climbing>
    ///
    /// Expressions are made up of two main entities:
    ///
    /// * Atoms: values, variables, functions, and parenthesized expressions.
    /// * Operators: performs operations on atoms and sub-expressions.
    ///   * Prefix operators: e.g. `-a` or `NOT a`.
    ///   * Infix operators: e.g. `a + b`  or `a AND b`.
    ///   * Postfix operators: e.g. `a!` or `a IS NULL`.
    ///
    /// During parsing, we have to respect the mathematical precedence and
    /// associativity of operators. Consider e.g.:
    ///
    /// 2 ^ 3 ^ 2 - 4 * 3
    ///
    /// By the rules of precedence and associativity, this expression should
    /// be interpreted as:
    ///
    /// (2 ^ (3 ^ 2)) - (4 * 3)
    ///
    /// Specifically, the exponentiation operator ^ is right-associative, so it
    /// should be 2 ^ (3 ^ 2) = 512, not (2 ^ 3) ^ 2 = 64. Similarly,
    /// exponentiation and multiplication have higher precedence than
    /// subtraction, so it should be (2 ^ 3 ^ 2) - (4 * 3) = 500, not
    /// 2 ^ 3 ^ (2 - 4) * 3 = -3.24.
    ///
    /// To use precedence climbing, we first need to specify the relative
    /// precedence of operators as a number, where 1 is the lowest precedence:
    ///
    /// * 1: OR
    /// * 2: AND
    /// * 3: NOT
    /// * 4: =, !=, LIKE, IS
    /// * 5: <, <=, >, >=
    /// * 6: +, -
    /// * 7: *, /, %
    /// * 8: ^
    /// * 9: !
    /// * 10: +, - (prefix)
    ///
    /// We also have to specify the associativity of operators:
    ///
    /// * Right-associative: ^ and all prefix operators.
    /// * Left-associative: all other operators.
    ///
    /// Left-associative operators get a +1 to their precedence, so that they
    /// bind tighter to their left operand than right-associative operators.
    ///
    /// The precedence climbing algorithm works by recursively parsing the
    /// left-hand side of an expression (including any prefix operators), any
    /// infix operators and recursive right-hand side expressions, and finally
    /// any postfix operators.
    ///
    /// The grouping is determined by where the right-hand side recursion
    /// terminates. The algorithm will greedily consume as many operators as
    /// possible, but only as long as their precedence is greater than or equal
    /// to the precedence of the previous operator (hence the name "climbing").
    /// When we find an operator with lower precedence, we return the current
    /// expression up the recursion stack and resume parsing the operator at a
    /// lower precedence.
    ///
    /// The precedence levels for the previous example are as follows:
    ///
    /// ```text
    ///     ~~~~~          Precedence 9: ^ right-associativity
    /// ~~~~~~~~~          Precedence 9: ^
    ///             ~~~~~  Precedence 7: *
    /// ~~~~~~~~~~~~~~~~~  Precedence 6: -
    /// 2 ^ 3 ^ 2 - 4 * 3
    /// ```
    ///
    /// Let's walk through the recursive parsing of this expression:
    ///
    /// parse_expression_at(prec=0)
    ///   lhs = parse_expression_atom() = 2
    ///   op = parse_infix_operator(prec=0) = ^ (prec=9)
    ///   rhs = parse_expression_at(prec=9)
    ///     lhs = parse_expression_atom() = 3
    ///     op = parse_infix_operator(prec=9) = ^ (prec=9)
    ///     rhs = parse_expression_at(prec=9)
    ///       lhs = parse_expression_atom() = 2
    ///       op = parse_infix_operator(prec=9) = None (reject - at prec=6)
    ///       return lhs = 2
    ///     lhs = (lhs op rhs) = (3 ^ 2)
    ///     op = parse_infix_operator(prec=9) = None (reject - at prec=6)
    ///     return lhs = (3 ^ 2)
    ///   lhs = (lhs op rhs) = (2 ^ (3 ^ 2))
    ///   op = parse_infix_operator(prec=0) = - (prec=6)
    ///   rhs = parse_expression_at(prec=6)
    ///     lhs = parse_expression_atom() = 4
    ///     op = parse_infix_operator(prec=6) = * (prec=7)
    ///     rhs = parse_expression_at(prec=7)
    ///       lhs = parse_expression_atom() = 3
    ///       op = parse_infix_operator(prec=7) = None (end of expression)
    ///       return lhs = 3
    ///     lhs = (lhs op rhs) = (4 * 3)
    ///     op = parse_infix_operator(prec=6) = None (end of expression)
    ///     return lhs = (4 * 3)
    ///   lhs = (lhs op rhs) = ((2 ^ (3 ^ 2)) - (4 * 3))
    ///   op = parse_infix_operator(prec=0) = None (end of expression)
    ///   return lhs = ((2 ^ (3 ^ 2)) - (4 * 3))
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

    /// Parses an expression atom. This is either:
    ///
    /// * A literal value.
    /// * A column name.
    /// * A function call.
    /// * A parenthesized expression.
    fn parse_expression_atom(&mut self) -> Result<Expression> {
        let token = self.next()?;
        Ok(match token {
            // All columns.
            Token::Asterisk => Expression::All,

            // Literal value.
            Token::Number(n) if n.chars().all(|c| c.is_ascii_digit()) => {
                // Try to parse as i128 first, then as u128 if that fails
                match n.parse::<i128>() {
                    Ok(val) => Literal::Integer(val).into(),
                    Err(_) => {
                        // Try parsing as u128 for large unsigned values
                        match n.parse::<u128>() {
                            Ok(val) => {
                                // We need to handle u128 values that are > i128::MAX
                                // For now, we'll store them as i128 and let coercion handle it
                                // This is a bit of a hack but works for INSERT INTO unsigned columns
                                if val <= i128::MAX as u128 {
                                    Literal::Integer(val as i128).into()
                                } else {
                                    // For values > i128::MAX, use Float to preserve the value
                                    // This will be converted to U128 later during coercion
                                    Literal::Float(val as f64).into()
                                }
                            }
                            Err(e) => {
                                return Err(Error::ParseError(format!("invalid integer: {}", e)));
                            }
                        }
                    }
                }
            }
            Token::Number(n) => Literal::Float(
                n.parse()
                    .map_err(|e| Error::ParseError(format!("invalid float: {}", e)))?,
            )
            .into(),
            Token::String(s) => Literal::String(s).into(),
            Token::HexString(h) => {
                // Convert hex string to bytes
                match hex::decode(&h) {
                    Ok(bytes) => Literal::Bytea(bytes).into(),
                    Err(_) => return Err(Error::ParseError(format!("Invalid hex string: {}", h))),
                }
            }
            Token::Keyword(Keyword::True) => Literal::Boolean(true).into(),
            Token::Keyword(Keyword::False) => Literal::Boolean(false).into(),
            Token::Keyword(Keyword::Infinity) => Literal::Float(f64::INFINITY).into(),
            Token::Keyword(Keyword::NaN) => Literal::Float(f64::NAN).into(),
            Token::Keyword(Keyword::Null) => Literal::Null.into(),

            // DATE literal: DATE 'YYYY-MM-DD' or column name "date"
            Token::Keyword(Keyword::Date) => {
                // Check if this is a DATE literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => {
                        // It's a DATE literal
                        match self.next()? {
                            Token::String(date_str) => {
                                // Parse the date string into a NaiveDate
                                use chrono::NaiveDate;
                                match NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                                    Ok(date) => Literal::Date(date).into(),
                                    Err(_) => {
                                        return Err(Error::ParseError(format!(
                                            "Invalid date format: '{}'. Expected 'YYYY-MM-DD'",
                                            date_str
                                        )));
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        // It's a column reference named "date"
                        Expression::Column(None, "date".to_string())
                    }
                }
            }

            // TIME literal: TIME 'HH:MM:SS' or column name "time"
            Token::Keyword(Keyword::Time) => {
                // Check if this is a TIME literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => {
                        // It's a TIME literal
                        match self.next()? {
                            Token::String(time_str) => {
                                // Parse the time string into a NaiveTime
                                use chrono::NaiveTime;
                                match NaiveTime::parse_from_str(&time_str, "%H:%M:%S") {
                                    Ok(time) => Literal::Time(time).into(),
                                    Err(_) => {
                                        // Try with fractional seconds
                                        match NaiveTime::parse_from_str(&time_str, "%H:%M:%S%.f") {
                                            Ok(time) => Literal::Time(time).into(),
                                            Err(_) => {
                                                return Err(Error::ParseError(format!(
                                                    "Invalid time format: '{}'. Expected 'HH:MM:SS[.fraction]'",
                                                    time_str
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        // It's a column reference named "time"
                        Expression::Column(None, "time".to_string())
                    }
                }
            }

            // TIMESTAMP literal: TIMESTAMP 'YYYY-MM-DD HH:MM:SS' or column name "timestamp"
            Token::Keyword(Keyword::Timestamp) => {
                // Check if this is a TIMESTAMP literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => {
                        // It's a TIMESTAMP literal
                        match self.next()? {
                            Token::String(timestamp_str) => {
                                // Parse the timestamp string into a NaiveDateTime
                                use chrono::NaiveDateTime;
                                match NaiveDateTime::parse_from_str(
                                    &timestamp_str,
                                    "%Y-%m-%d %H:%M:%S",
                                ) {
                                    Ok(timestamp) => Literal::Timestamp(timestamp).into(),
                                    Err(_) => {
                                        // Try with fractional seconds
                                        match NaiveDateTime::parse_from_str(
                                            &timestamp_str,
                                            "%Y-%m-%d %H:%M:%S%.f",
                                        ) {
                                            Ok(timestamp) => Literal::Timestamp(timestamp).into(),
                                            Err(_) => {
                                                return Err(Error::ParseError(format!(
                                                    "Invalid timestamp format: '{}'. Expected 'YYYY-MM-DD HH:MM:SS[.fraction]'",
                                                    timestamp_str
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        // It's a column reference named "timestamp"
                        Expression::Column(None, "timestamp".to_string())
                    }
                }
            }

            // INTERVAL literal: INTERVAL '1' DAY/MONTH/YEAR or column name "interval"
            Token::Keyword(Keyword::Interval) => {
                // Check if this is an INTERVAL literal (followed by a string) or a column reference
                match self.peek()? {
                    Some(Token::String(_)) => {
                        // It's an INTERVAL literal
                        match self.next()? {
                            Token::String(interval_str) => {
                                // Check for TO keyword (complex interval formats)
                                if self.peek()?.is_some_and(|t| matches!(t, Token::Ident(s) if s.to_uppercase().ends_with("S") || s.to_uppercase().ends_with("R") || s.to_uppercase().ends_with("H") || s.to_uppercase().ends_with("Y") || s.to_uppercase().ends_with("E"))) {
                                    let unit = self.next_ident_or_keyword()?;
                                    if self.next_if_map(|t| match t {
                                        Token::Keyword(Keyword::To) => Some(()),
                                        Token::Ident(s) if s.to_uppercase() == "TO" => Some(()),
                                        _ => None
                                    }).is_some() {
                                        // Complex format with TO keyword
                                        let _end_unit = self.next_ident_or_keyword()?;
                                        return self.parse_complex_interval_format(&interval_str, &unit);
                                    }
                                    // Not a TO format, process as simple interval
                                    // Put the unit back for normal processing
                                    let unit_upper = unit.to_uppercase();

                                    // Parse the interval value (possibly negative)
                                    let value: i32 = interval_str.parse().map_err(|_| {
                                        Error::ParseError(format!(
                                            "Invalid interval value: '{}'. Expected a number",
                                            interval_str
                                        ))
                                    })?;

                                    use crate::types::data_type::Interval;
                                    let interval = match unit_upper.as_str() {
                                        "DAY" | "DAYS" => Interval {
                                            months: 0,
                                            days: value,
                                            microseconds: 0,
                                        },
                                        "MONTH" | "MONTHS" => Interval {
                                            months: value,
                                            days: 0,
                                            microseconds: 0,
                                        },
                                        "YEAR" | "YEARS" => Interval {
                                            months: value * 12,
                                            days: 0,
                                            microseconds: 0,
                                        },
                                        "HOUR" | "HOURS" => Interval {
                                            months: 0,
                                            days: 0,
                                            microseconds: value as i64 * 3_600_000_000,
                                        },
                                        "MINUTE" | "MINUTES" => Interval {
                                            months: 0,
                                            days: 0,
                                            microseconds: value as i64 * 60_000_000,
                                        },
                                        "SECOND" | "SECONDS" => Interval {
                                            months: 0,
                                            days: 0,
                                            microseconds: value as i64 * 1_000_000,
                                        },
                                        _ => {
                                            return Err(Error::ParseError(format!(
                                                "Invalid interval unit: {}",
                                                unit_upper
                                            )))
                                        }
                                    };
                                    Literal::Interval(interval).into()
                                } else {
                                    // Simple format - parse as before
                                    // Parse the interval value (possibly negative)
                                    let value: i32 = interval_str.parse().map_err(|_| {
                                        Error::ParseError(format!(
                                            "Invalid interval value: '{}'. Expected a number",
                                            interval_str
                                        ))
                                    })?;

                                    // Parse the interval unit (DAY, MONTH, YEAR, etc.)
                                    let unit = self.next_ident_or_keyword()?;
                                    let unit_upper = unit.to_uppercase();

                                use crate::types::data_type::Interval;
                                let interval = match unit_upper.as_str() {
                                    "DAY" | "DAYS" => Interval {
                                        months: 0,
                                        days: value,
                                        microseconds: 0,
                                    },
                                    "MONTH" | "MONTHS" => Interval {
                                        months: value,
                                        days: 0,
                                        microseconds: 0,
                                    },
                                    "YEAR" | "YEARS" => Interval {
                                        months: value * 12,
                                        days: 0,
                                        microseconds: 0,
                                    },
                                    "HOUR" | "HOURS" => Interval {
                                        months: 0,
                                        days: 0,
                                        microseconds: value as i64 * 3_600_000_000,
                                    },
                                    "MINUTE" | "MINUTES" => Interval {
                                        months: 0,
                                        days: 0,
                                        microseconds: value as i64 * 60_000_000,
                                    },
                                    "SECOND" | "SECONDS" => Interval {
                                        months: 0,
                                        days: 0,
                                        microseconds: value as i64 * 1_000_000,
                                    },
                                    _ => {
                                        return Err(Error::ParseError(format!(
                                            "Invalid interval unit: '{}'. Expected DAY, MONTH, YEAR, HOUR, MINUTE, or SECOND",
                                            unit
                                        )));
                                    }
                                };
                                Literal::Interval(interval).into()
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => {
                        // It's a column reference named "interval"
                        Expression::Column(None, "interval".to_string())
                    }
                }
            }

            // CAST expression: CAST(expr AS type)
            Token::Keyword(Keyword::Cast) => {
                self.expect(Token::OpenParen)?;
                let expr = self.parse_expression()?;
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
                    args.push(self.parse_expression()?);
                }

                // If DISTINCT was used, create a special function name
                if is_distinct {
                    Expression::Function(format!("{}_DISTINCT", name.to_uppercase()), args)
                } else {
                    Expression::Function(name, args)
                }
            }

            // Column name, either qualified as table.column or unqualified.
            Token::Ident(table) if self.next_is(Token::Period) => {
                Expression::Column(Some(table), self.next_ident()?)
            }
            Token::Ident(column) => Expression::Column(None, column),

            // Parameter placeholder (?)
            Token::Question => {
                let param_idx = self.param_count;
                self.param_count += 1;
                Expression::Parameter(param_idx)
            }

            // Parenthesized expression.
            Token::OpenParen => {
                let expr = self.parse_expression()?;
                self.expect(Token::CloseParen)?;
                expr
            }

            // Array literal: [1, 2, 3]
            Token::OpenBracket => {
                let mut elements = Vec::new();
                if self.peek()? != Some(&Token::CloseBracket) {
                    elements.push(self.parse_expression()?);
                    while self.next_is(Token::Comma) {
                        elements.push(self.parse_expression()?);
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
                    let key = self.parse_expression()?;
                    self.expect(Token::Colon)?;
                    let value = self.parse_expression()?;
                    pairs.push((key, value));

                    // Parse remaining pairs
                    while self.next_is(Token::Comma) {
                        let key = self.parse_expression()?;
                        self.expect(Token::Colon)?;
                        let value = self.parse_expression()?;
                        pairs.push((key, value));
                    }
                }
                self.expect(Token::CloseBrace)?;
                Expression::MapLiteral(pairs)
            }

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
                Token::Equal => InfixOperator::Equal,
                Token::GreaterThan => InfixOperator::GreaterThan,
                Token::GreaterThanOrEqual => InfixOperator::GreaterThanOrEqual,
                Token::Keyword(Keyword::And) => InfixOperator::And,
                Token::Keyword(Keyword::Like) => InfixOperator::Like,
                Token::Keyword(Keyword::Or) => InfixOperator::Or,
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

            // Handle empty list
            let mut list = Vec::new();
            if self.peek()? != Some(&Token::CloseParen) {
                // Parse first expression
                list.push(self.parse_expression()?);
                // Parse remaining expressions
                while self.next_is(Token::Comma) {
                    list.push(self.parse_expression()?);
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
            let index = self.parse_expression()?;
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

// Implement DmlParser trait for Parser
impl DmlParser for Parser<'_> {
    fn next(&mut self) -> Result<Token> {
        self.next()
    }

    fn next_ident(&mut self) -> Result<String> {
        self.next_ident()
    }

    fn next_ident_or_keyword(&mut self) -> Result<String> {
        self.next_ident_or_keyword()
    }

    fn next_is(&mut self, token: Token) -> bool {
        self.next_is(token)
    }

    fn expect(&mut self, expect: Token) -> Result<()> {
        self.expect(expect)
    }

    fn peek(&mut self) -> Result<Option<&Token>> {
        self.peek()
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_expression()
    }

    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Option<T> {
        self.next_if_map(f)
    }

    fn skip(&mut self, token: Token) {
        self.skip(token);
    }
}

// Implement DdlParser trait for Parser
impl DdlParser for Parser<'_> {
    fn next_ident(&mut self) -> Result<String> {
        self.next_ident()
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_expression()
    }

    fn next_if_keyword(&mut self) -> Option<Keyword> {
        self.next_if_keyword()
    }
}

// Implement TypeParser trait for Parser
impl TypeParser for Parser<'_> {
    fn next(&mut self) -> Result<Token> {
        self.next()
    }

    fn next_ident_or_keyword(&mut self) -> Result<String> {
        self.next_ident_or_keyword()
    }

    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.next_if(predicate)
    }

    fn next_is(&mut self, token: Token) -> bool {
        self.next_is(token)
    }

    fn next_if_ident_eq(&mut self, expected: &str) -> bool {
        self.next_if_ident_eq(expected)
    }

    fn expect(&mut self, expect: Token) -> Result<()> {
        self.expect(expect)
    }

    fn peek(&mut self) -> Result<Option<&Token>> {
        self.peek()
    }
}

impl Parser<'_> {
    /// Parse complex interval formats like '1-2' YEAR TO MONTH or '3 14' DAY TO HOUR
    fn parse_complex_interval_format(
        &mut self,
        interval_str: &str,
        start_unit: &str,
    ) -> Result<Expression> {
        use crate::types::data_type::Interval;

        let start_upper = start_unit.to_uppercase();

        // Check if the whole interval is negative
        let (is_negative, abs_interval_str) = if let Some(stripped) = interval_str.strip_prefix('-')
        {
            (true, stripped)
        } else {
            (false, interval_str)
        };

        // Handle YEAR TO MONTH format: '1-2' means 1 year 2 months
        if start_upper == "YEAR" && abs_interval_str.contains('-') {
            let parts: Vec<&str> = abs_interval_str.split('-').collect();
            if parts.len() == 2 {
                let years: i32 = parts[0].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid year value in interval: '{}'", parts[0]))
                })?;
                let months: i32 = parts[1].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid month value in interval: '{}'", parts[1]))
                })?;
                let total_months = years * 12 + months;
                let interval = Interval {
                    months: if is_negative {
                        -total_months
                    } else {
                        total_months
                    },
                    days: 0,
                    microseconds: 0,
                };
                return Ok(Literal::Interval(interval).into());
            }
        }

        // Handle DAY TO SECOND format first (before DAY TO HOUR): '3 14:30:12.5' means 3 days 14 hours 30 minutes 12.5 seconds
        if start_upper == "DAY" && abs_interval_str.contains(':') {
            let parts: Vec<&str> = abs_interval_str.splitn(2, ' ').collect();
            let mut days = 0;
            let time_part;

            if parts.len() == 2 {
                days = parts[0].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid day value in interval: '{}'", parts[0]))
                })?;
                time_part = parts[1];
            } else {
                time_part = abs_interval_str;
            }

            // Parse time part (HH:MM:SS.fff)
            let time_parts: Vec<&str> = time_part.split(':').collect();
            if time_parts.len() >= 2 {
                let hours: i32 = time_parts[0].parse().map_err(|_| {
                    Error::ParseError(format!(
                        "Invalid hour value in interval: '{}'",
                        time_parts[0]
                    ))
                })?;
                let minutes: i32 = time_parts[1].parse().map_err(|_| {
                    Error::ParseError(format!(
                        "Invalid minute value in interval: '{}'",
                        time_parts[1]
                    ))
                })?;

                let mut seconds = 0.0;
                if time_parts.len() >= 3 {
                    seconds = time_parts[2].parse().map_err(|_| {
                        Error::ParseError(format!(
                            "Invalid second value in interval: '{}'",
                            time_parts[2]
                        ))
                    })?;
                }

                let total_microseconds = (hours as i64 * 3_600_000_000)
                    + (minutes as i64 * 60_000_000)
                    + ((seconds * 1_000_000.0) as i64);

                let interval = Interval {
                    months: 0,
                    days: if is_negative { -days } else { days },
                    microseconds: if is_negative {
                        -total_microseconds
                    } else {
                        total_microseconds
                    },
                };
                return Ok(Literal::Interval(interval).into());
            }
        }

        // Handle DAY TO HOUR format: '3 14' means 3 days 14 hours
        if start_upper == "DAY" && abs_interval_str.contains(' ') && !abs_interval_str.contains(':')
        {
            let parts: Vec<&str> = abs_interval_str.split_whitespace().collect();
            if parts.len() == 2 {
                let days: i32 = parts[0].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid day value in interval: '{}'", parts[0]))
                })?;
                let hours: i32 = parts[1].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid hour value in interval: '{}'", parts[1]))
                })?;
                let microseconds = hours as i64 * 3_600_000_000;
                let interval = Interval {
                    months: 0,
                    days: if is_negative { -days } else { days },
                    microseconds: if is_negative {
                        -microseconds
                    } else {
                        microseconds
                    },
                };
                return Ok(Literal::Interval(interval).into());
            }
        }

        Err(Error::ParseError(format!(
            "Unsupported complex interval format: '{}' {} TO ...",
            interval_str, start_upper
        )))
    }
}

#[test]
fn test_parse_collection_types() {
    use super::ast::ddl::DdlStatement;
    use crate::types::DataType;

    // Test LIST type
    let sql = "CREATE TABLE test (items LIST)";
    let stmt = Parser::parse(sql).unwrap();
    if let Statement::Ddl(DdlStatement::CreateTable { columns, .. }) = stmt {
        assert!(matches!(columns[0].datatype, DataType::List(_)));
        println!("✓ LIST parsing works");
    }

    // Test ARRAY with size
    let sql = "CREATE TABLE test (coords INT[3])";
    let stmt = Parser::parse(sql).unwrap();
    if let Statement::Ddl(DdlStatement::CreateTable { columns, .. }) = stmt {
        assert!(matches!(columns[0].datatype, DataType::Array(_, Some(3))));
        println!("✓ ARRAY[SIZE] parsing works");
    }

    // Test MAP type
    let sql = "CREATE TABLE test (settings MAP)";
    let stmt = Parser::parse(sql).unwrap();
    if let Statement::Ddl(DdlStatement::CreateTable { columns, .. }) = stmt {
        assert!(matches!(columns[0].datatype, DataType::Map(_, _)));
        println!("✓ MAP parsing works");
    }
}

#[test]
fn test_parse_collection_expressions() {
    // Test array literal
    let expr = Parser::parse_expr("[1, 2, 3]").unwrap();
    match expr {
        Expression::ArrayLiteral(elements) => {
            assert_eq!(elements.len(), 3);
            println!("✓ Array literal parsing works");
        }
        _ => panic!("Expected ArrayLiteral"),
    }

    // Test map literal
    let expr = Parser::parse_expr("{'key1': 'value1', 'key2': 'value2'}").unwrap();
    match expr {
        Expression::MapLiteral(pairs) => {
            assert_eq!(pairs.len(), 2);
            println!("✓ Map literal parsing works");
        }
        _ => panic!("Expected MapLiteral"),
    }

    // Test array access
    let expr = Parser::parse_expr("arr[0]").unwrap();
    match expr {
        Expression::ArrayAccess { base, index } => {
            assert!(matches!(*base, Expression::Column(None, _)));
            assert!(matches!(*index, Expression::Literal(Literal::Integer(0))));
            println!("✓ Array access parsing works");
        }
        _ => panic!("Expected ArrayAccess"),
    }

    // Test struct field access - need to use parentheses or array access to disambiguate from table.column
    let expr = Parser::parse_expr("(person).name").unwrap();
    match expr {
        Expression::FieldAccess { base, field } => {
            assert!(matches!(*base, Expression::Column(None, _)));
            assert_eq!(field, "name");
            println!("✓ Field access parsing works");
        }
        _ => panic!("Expected FieldAccess, got: {:?}", expr),
    }

    // Test nested access
    let expr = Parser::parse_expr("users[0].address.city").unwrap();
    match expr {
        Expression::FieldAccess { base, field } => {
            assert_eq!(field, "city");
            // Check nested structure
            match *base {
                Expression::FieldAccess {
                    base: inner_base,
                    field: inner_field,
                } => {
                    assert_eq!(inner_field, "address");
                    assert!(matches!(*inner_base, Expression::ArrayAccess { .. }));
                    println!("✓ Nested access parsing works");
                }
                _ => panic!("Expected nested FieldAccess"),
            }
        }
        _ => panic!("Expected FieldAccess"),
    }
}
