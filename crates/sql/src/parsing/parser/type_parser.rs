//! Type parser module
//!
//! Handles parsing of SQL data types including simple types (INT, VARCHAR),
//! collection types (ARRAY, LIST, MAP), and complex types (STRUCT).

use super::super::{Keyword, Token};
use super::token_helper::TokenHelper;
use crate::error::{Error, Result};
use crate::types::data_type::DataType;

/// Parser trait for type parsing functionality
pub trait TypeParser: TokenHelper {
    /// Parse a data type (can be used recursively for collection types)
    fn parse_type(&mut self) -> Result<DataType> {
        let data_type = match self.next()? {
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
            Token::Ident(s) if s.to_uppercase() == "JSON" => DataType::Json,
            Token::Ident(s) if s.to_uppercase() == "ANY" => DataType::I64, // ANY defaults to I64

            // Collection types - require explicit type parameters
            Token::Keyword(Keyword::Array) => {
                return Err(Error::ParseError(
                    "ARRAY requires element type and size, use TYPE[SIZE] syntax (e.g., INTEGER[10])".to_string()
                ));
            }
            Token::Keyword(Keyword::List) => {
                return Err(Error::ParseError(
                    "LIST requires element type, use TYPE[] syntax (e.g., INTEGER[])".to_string(),
                ));
            }
            Token::Keyword(Keyword::Map) => {
                self.expect(Token::OpenParen)?;
                let key_type = self.parse_type()?;
                self.expect(Token::Comma)?;
                let value_type = self.parse_type()?;
                self.expect(Token::CloseParen)?;
                DataType::Map(Box::new(key_type), Box::new(value_type))
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
        let mut result_type = data_type;
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
