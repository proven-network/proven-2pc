//! Type parser module
//!
//! Handles parsing of SQL data types including simple types (INT, VARCHAR),
//! collection types (ARRAY, LIST, MAP), and complex types (STRUCT).

use super::super::{Keyword, Token};
use crate::error::{Error, Result};
use crate::types::data_type::DataType;

/// Parser trait for type parsing functionality
pub trait TypeParser {
    /// Returns the next token
    fn next(&mut self) -> Result<Token>;

    /// Returns the next identifier or keyword as identifier
    fn next_ident_or_keyword(&mut self) -> Result<String>;

    /// Checks if next token matches predicate without consuming
    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token>;

    /// Returns true if next token matches and consumes it
    fn next_is(&mut self, token: Token) -> bool;

    /// Checks if next identifier matches (case-insensitive)
    fn next_if_ident_eq(&mut self, expected: &str) -> bool;

    /// Expects a specific token or errors
    fn expect(&mut self, expect: Token) -> Result<()>;

    /// Peeks at next token without consuming
    fn peek(&mut self) -> Result<Option<&Token>>;

    /// Parse a data type (can be used recursively for collection types)
    fn parse_type(&mut self) -> Result<DataType> {
        let datatype = match self.next()? {
            // Boolean types
            Token::Keyword(Keyword::Bool | Keyword::Boolean) => DataType::Bool,

            // Integer types (check for UNSIGNED modifier)
            Token::Keyword(Keyword::Tinyint) => {
                if self.next_if_ident_eq("UNSIGNED") {
                    DataType::U8
                } else {
                    DataType::I8
                }
            }
            Token::Keyword(Keyword::Smallint) => {
                if self.next_if_ident_eq("UNSIGNED") {
                    DataType::U16
                } else {
                    DataType::I16
                }
            }
            Token::Keyword(Keyword::Int | Keyword::Integer) => {
                if self.next_if_ident_eq("UNSIGNED") {
                    DataType::U32
                } else {
                    DataType::I32
                }
            }
            Token::Keyword(Keyword::Bigint) => {
                if self.next_if_ident_eq("UNSIGNED") {
                    DataType::U64
                } else {
                    DataType::I64
                }
            }
            Token::Keyword(Keyword::Hugeint) => {
                if self.next_if_ident_eq("UNSIGNED") {
                    DataType::U128
                } else {
                    DataType::I128
                }
            }

            // Floating point types
            Token::Keyword(Keyword::Real) => DataType::F32,
            Token::Keyword(Keyword::Float | Keyword::Double) => DataType::F64,

            // Decimal types
            Token::Keyword(Keyword::Decimal) => DataType::Decimal(Some(38), Some(10)),

            // String types
            Token::Keyword(Keyword::String | Keyword::Text | Keyword::Varchar) => DataType::Str,

            // Special types
            Token::Keyword(Keyword::Uuid) => DataType::Uuid,
            Token::Keyword(Keyword::Date) => DataType::Date,
            Token::Keyword(Keyword::Time) => DataType::Time,
            Token::Keyword(Keyword::Timestamp) => DataType::Timestamp,
            Token::Keyword(Keyword::Interval) => DataType::Interval,

            // Also accept identifiers for compatibility
            Token::Ident(s) if s.to_uppercase() == "DATE" => DataType::Date,
            Token::Ident(s) if s.to_uppercase() == "TIME" => DataType::Time,
            Token::Ident(s) if s.to_uppercase() == "TIMESTAMP" => DataType::Timestamp,
            Token::Ident(s) if s.to_uppercase() == "INTERVAL" => DataType::Interval,
            Token::Ident(s) if s.to_uppercase() == "BLOB" || s.to_uppercase() == "BYTEA" => {
                DataType::Bytea
            }
            Token::Ident(s) if s.to_uppercase() == "INET" => DataType::Inet,
            Token::Ident(s) if s.to_uppercase() == "POINT" => DataType::Point,
            Token::Ident(s) if s.to_uppercase() == "ANY" => DataType::I64, // ANY defaults to I64

            // Collection types can be recursive
            Token::Keyword(Keyword::Array) => DataType::Array(Box::new(DataType::I64), None),
            Token::Keyword(Keyword::List) => DataType::List(Box::new(DataType::I64)),
            Token::Keyword(Keyword::Map) => {
                if self.next_if(|t| t == &Token::OpenParen).is_some() {
                    let key_type = self.parse_type()?;
                    self.expect(Token::Comma)?;
                    let value_type = self.parse_type()?;
                    self.expect(Token::CloseParen)?;
                    DataType::Map(Box::new(key_type), Box::new(value_type))
                } else {
                    DataType::Map(Box::new(DataType::Str), Box::new(DataType::I64))
                }
            }
            Token::Keyword(Keyword::Struct) => {
                if self.next_if(|t| t == &Token::OpenParen).is_some() {
                    let mut fields = Vec::new();
                    if self.peek()? != Some(&Token::CloseParen) {
                        loop {
                            let field_name = self.next_ident_or_keyword()?;
                            let field_type = self.parse_type()?;
                            fields.push((field_name, field_type));
                            if !self.next_is(Token::Comma) {
                                break;
                            }
                        }
                    }
                    self.expect(Token::CloseParen)?;
                    DataType::Struct(fields)
                } else {
                    DataType::Struct(Vec::new())
                }
            }

            token => {
                return Err(Error::ParseError(format!(
                    "unexpected token {}, expected data type",
                    token
                )));
            }
        };

        // Check for array size specification with TYPE[SIZE] or TYPE[SIZE][SIZE] syntax
        // Handle multiple dimensions for multi-dimensional arrays
        let mut result_type = datatype;
        while self.next_if(|t| t == &Token::OpenBracket).is_some() {
            // Check if we have a size or just empty brackets for variable-size list
            let size = if self.next_if(|t| t == &Token::CloseBracket).is_some() {
                // Empty brackets [] means variable-size list
                result_type = DataType::List(Box::new(result_type));
                continue;
            } else if let Token::Number(n) = self.next()? {
                // Fixed size array [N]
                let size_val = n
                    .parse::<usize>()
                    .map_err(|_| Error::ParseError(format!("invalid array size: {}", n)))?;
                self.expect(Token::CloseBracket)?;
                Some(size_val)
            } else {
                return Err(Error::ParseError("expected array size or ]".into()));
            };

            // Wrap the current type in an Array type with fixed size
            result_type = DataType::Array(Box::new(result_type), size);
        }
        Ok(result_type)
    }
}

/// Convert a DataType to a string representation for CAST function
pub fn data_type_to_cast_string(data_type: &DataType) -> String {
    match data_type {
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
        DataType::F64 => "FLOAT".to_string(),
        DataType::Decimal(_, _) => "DECIMAL".to_string(),
        DataType::Bool => "BOOLEAN".to_string(),
        DataType::Str | DataType::Text => "TEXT".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time => "TIME".to_string(),
        DataType::Timestamp => "TIMESTAMP".to_string(),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Uuid => "UUID".to_string(),
        DataType::Bytea => "BYTEA".to_string(),
        DataType::Inet => "INET".to_string(),
        DataType::Point => "POINT".to_string(),
        DataType::List(elem_type) => format!("{}[]", data_type_to_cast_string(elem_type)),
        DataType::Array(elem_type, size) => {
            if let Some(s) = size {
                format!("{}[{}]", data_type_to_cast_string(elem_type), s)
            } else {
                format!("{}[]", data_type_to_cast_string(elem_type))
            }
        }
        DataType::Map(key_type, val_type) => {
            format!(
                "MAP({}, {})",
                data_type_to_cast_string(key_type),
                data_type_to_cast_string(val_type)
            )
        }
        DataType::Struct(fields) => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|(name, dtype)| format!("{} {}", name, data_type_to_cast_string(dtype)))
                .collect();
            format!("STRUCT({})", field_strs.join(", "))
        }
        DataType::Nullable(inner) => {
            // For CAST, nullable types are handled the same as their inner type
            data_type_to_cast_string(inner)
        }
    }
}
