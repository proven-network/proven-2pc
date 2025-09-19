//! Modular SQL parser implementation
//!
//! The parser is split into several modules for better organization:
//! - expr_parser: Expression parsing with operator precedence
//! - type_parser: Data type parsing
//! - ddl_parser: DDL statement parsing (CREATE, DROP)
//! - dml_parser: DML statement parsing (SELECT, INSERT, UPDATE, DELETE)
//! - literal_parser: Literal value parsing
//! - token_helper: Base trait for token navigation

pub mod ddl_parser;
pub mod dml_parser;
pub mod expr_parser;
pub mod literal_parser;
pub mod token_helper;
pub mod type_parser;

use std::iter::Peekable;

use self::ddl_parser::DdlParser;
use self::dml_parser::DmlParser;
use self::expr_parser::ExpressionParser;
use self::literal_parser::LiteralParser;
use self::token_helper::TokenHelper;
use self::type_parser::TypeParser;
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
    param_count: u32,
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

    /// Creates a new parser for the given string.
    pub fn new(input: &str) -> Parser<'_> {
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
                "expected identifier, found {}",
                token
            ))),
        }
    }

    /// Returns the next identifier or keyword as identifier (for contexts where keywords can be identifiers).
    fn next_ident_or_keyword(&mut self) -> Result<String> {
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            Token::Keyword(keyword) => Ok(keyword.to_string().to_lowercase()),
            token => Err(Error::ParseError(format!(
                "expected identifier or keyword, found {}",
                token
            ))),
        }
    }

    /// Returns the next lexer token if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.peek().ok()?.filter(|&t| predicate(t))?;
        self.next().ok()
    }

    /// Passes the next lexer token through the closure, consuming it if the
    /// closure returns Some. Returns the result of the closure.
    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Option<T> {
        let value = f(self.peek().ok()??)?;
        self.next().ok()?;
        Some(value)
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
                "expected {}, found {}",
                expect, token
            )));
        }
        Ok(())
    }

    /// Peeks the next lexer token if any, without consuming it.
    fn peek(&mut self) -> Result<Option<&Token>> {
        self.lexer
            .peek()
            .map(|result| result.as_ref().map(Some).map_err(|e| e.clone()))
            .unwrap_or(Ok(None))
    }

    /// Parses a SQL statement.
    pub fn parse_statement(&mut self) -> Result<Statement> {
        // Handle parameterized queries first
        if self.peek()? == Some(&Token::Question) {
            return Err(Error::ParseError(
                "Parameterized queries not yet supported at statement level".into(),
            ));
        }

        match self.peek()?.cloned() {
            Some(Token::Keyword(Keyword::Create)) => self.parse_create(),
            Some(Token::Keyword(Keyword::Drop)) => self.parse_drop(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Values)) => self.parse_values(),
            Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
            _ => {
                let token = self.next()?;
                Err(Error::ParseError(format!(
                    "expected statement, found {}",
                    token
                )))
            }
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
}

// Implement TokenHelper trait for Parser
impl TokenHelper for Parser<'_> {
    fn next(&mut self) -> Result<Token> {
        self.next()
    }

    fn next_ident(&mut self) -> Result<String> {
        self.next_ident()
    }

    fn next_ident_or_keyword(&mut self) -> Result<String> {
        self.next_ident_or_keyword()
    }

    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token> {
        self.next_if(predicate)
    }

    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Option<T> {
        self.next_if_map(f)
    }

    fn next_if_keyword(&mut self) -> Option<Keyword> {
        self.next_if_keyword()
    }

    fn next_if_ident_eq(&mut self, expected: &str) -> bool {
        self.next_if_ident_eq(expected)
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
}

// Implement ExpressionParser trait for Parser
impl ExpressionParser for Parser<'_> {
    fn increment_param_count(&mut self) -> u32 {
        let idx = self.param_count;
        self.param_count += 1;
        idx
    }

    fn parse_type(&mut self) -> Result<crate::types::data_type::DataType> {
        TypeParser::parse_type(self)
    }
}

// Implement DmlParser trait for Parser
impl DmlParser for Parser<'_> {
    fn parse_expression(&mut self) -> Result<Expression> {
        ExpressionParser::parse_expression(self)
    }
}

// Implement DdlParser trait for Parser
impl DdlParser for Parser<'_> {
    fn parse_expression(&mut self) -> Result<Expression> {
        ExpressionParser::parse_expression(self)
    }

    fn parse_values_rows(&mut self) -> Result<Vec<Vec<Expression>>> {
        DmlParser::parse_values_rows(self)
    }
}

// Implement TypeParser trait for Parser
impl TypeParser for Parser<'_> {
    // All required methods are provided by TokenHelper trait
}

// Implement LiteralParser trait for Parser
impl LiteralParser for Parser<'_> {
    fn parse_complex_interval_format(
        &mut self,
        interval_str: &str,
        start_unit: &str,
    ) -> Result<Expression> {
        use crate::types::data_type::Interval;

        let start_upper = start_unit.to_uppercase();

        // Check if the whole interval is negative
        let is_negative = interval_str.starts_with('-');

        // Parse the interval string (remove negative sign if present)
        let interval_str = if is_negative {
            &interval_str[1..]
        } else {
            interval_str
        };

        // Parse complex formats like '1-2' for YEAR TO MONTH
        let interval = match start_upper.as_str() {
            "YEAR" => {
                // YEAR TO MONTH format: '1-2' means 1 year and 2 months
                let parts: Vec<&str> = interval_str.split('-').collect();
                if parts.len() != 2 {
                    return Err(Error::ParseError(format!(
                        "Invalid YEAR TO MONTH format: '{}'. Expected 'Y-M'",
                        interval_str
                    )));
                }

                let years: i32 = parts[0].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid year value: '{}'", parts[0]))
                })?;
                let months: i32 = parts[1].parse().map_err(|_| {
                    Error::ParseError(format!("Invalid month value: '{}'", parts[1]))
                })?;

                let total_months = years * 12 + months;
                Interval {
                    months: if is_negative {
                        -total_months
                    } else {
                        total_months
                    },
                    days: 0,
                    microseconds: 0,
                }
            }
            "DAY" => {
                // DAY TO HOUR/MINUTE/SECOND format: 'D HH' or 'D HH:MM' or 'D HH:MM:SS'
                let parts: Vec<&str> = interval_str.splitn(2, ' ').collect();
                if parts.is_empty() {
                    return Err(Error::ParseError(format!(
                        "Invalid DAY interval format: '{}'",
                        interval_str
                    )));
                }

                let days: i32 = parts[0]
                    .parse()
                    .map_err(|_| Error::ParseError(format!("Invalid day value: '{}'", parts[0])))?;

                let mut microseconds = 0i64;
                if parts.len() == 2 {
                    // Parse time component - could be just hours, hours:minutes, or hours:minutes:seconds
                    let time_str = parts[1];

                    // Check if it's just a number (hours only for DAY TO HOUR)
                    if !time_str.contains(':') {
                        // DAY TO HOUR format: just hours
                        let hours: i64 = time_str.parse().map_err(|_| {
                            Error::ParseError(format!("Invalid hour value: '{}'", time_str))
                        })?;
                        microseconds = hours * 3_600_000_000;
                    } else {
                        // Parse time component with colons
                        let time_parts: Vec<&str> = time_str.split(':').collect();

                        let hours: i64 = time_parts[0].parse().map_err(|_| {
                            Error::ParseError(format!("Invalid hour value: '{}'", time_parts[0]))
                        })?;

                        let minutes: i64 = if time_parts.len() > 1 {
                            time_parts[1].parse().map_err(|_| {
                                Error::ParseError(format!(
                                    "Invalid minute value: '{}'",
                                    time_parts[1]
                                ))
                            })?
                        } else {
                            0
                        };

                        let seconds: i64 = if time_parts.len() > 2 {
                            // Handle seconds with possible fractional part
                            let seconds_str = time_parts[2];
                            if let Some(dot_pos) = seconds_str.find('.') {
                                let sec: i64 = seconds_str[..dot_pos].parse().map_err(|_| {
                                    Error::ParseError(format!(
                                        "Invalid second value: '{}'",
                                        seconds_str
                                    ))
                                })?;
                                let fraction_str = &seconds_str[dot_pos + 1..];
                                // Pad or truncate to 6 digits for microseconds
                                let fraction = if fraction_str.len() <= 6 {
                                    let padded = format!("{:0<6}", fraction_str);
                                    padded.parse::<i64>().unwrap_or(0)
                                } else {
                                    fraction_str[..6].parse::<i64>().unwrap_or(0)
                                };
                                microseconds = hours * 3_600_000_000
                                    + minutes * 60_000_000
                                    + sec * 1_000_000
                                    + fraction;
                                return Ok(Literal::Interval(Interval {
                                    months: 0,
                                    days: if is_negative { -days } else { days },
                                    microseconds: if is_negative {
                                        -microseconds
                                    } else {
                                        microseconds
                                    },
                                })
                                .into());
                            } else {
                                seconds_str.parse().map_err(|_| {
                                    Error::ParseError(format!(
                                        "Invalid second value: '{}'",
                                        seconds_str
                                    ))
                                })?
                            }
                        } else {
                            0
                        };

                        microseconds =
                            hours * 3_600_000_000 + minutes * 60_000_000 + seconds * 1_000_000;
                    }
                }

                Interval {
                    months: 0,
                    days: if is_negative { -days } else { days },
                    microseconds: if is_negative {
                        -microseconds
                    } else {
                        microseconds
                    },
                }
            }
            _ => {
                return Err(Error::ParseError(format!(
                    "Complex interval format not supported for unit: {}",
                    start_upper
                )));
            }
        };

        Ok(Literal::Interval(interval).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::ast::{DdlStatement, DmlStatement};
    use crate::types::data_type::DataType;

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Dml(DmlStatement::Insert { .. }) => {}
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_select() {
        let sql = "SELECT * FROM users WHERE id = 1";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Dml(DmlStatement::Select { .. }) => {}
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let sql = "UPDATE users SET name = 'Bob' WHERE id = 1";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Dml(DmlStatement::Update { .. }) => {}
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Dml(DmlStatement::Delete { .. }) => {}
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Ddl(DdlStatement::CreateTable { .. }) => {}
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let sql = "DROP TABLE users";
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            Statement::Ddl(DdlStatement::DropTable { .. }) => {}
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn test_parse_data_types() {
        let test_cases = vec![
            ("INT", DataType::I32),
            ("BIGINT", DataType::I64),
            ("TEXT", DataType::Str),
            ("BOOLEAN", DataType::Bool),
            ("FLOAT", DataType::F64),
            ("DOUBLE", DataType::F64),
        ];

        for (sql_type, expected_type) in test_cases {
            let sql = format!("CREATE TABLE test (col {})", sql_type);
            let stmt = Parser::parse(&sql).unwrap();
            match stmt {
                Statement::Ddl(DdlStatement::CreateTable { columns, .. }) => {
                    assert_eq!(columns[0].datatype, expected_type);
                }
                _ => panic!("Expected CREATE TABLE statement"),
            }
        }
    }
}
