//! Token helper module
//!
//! Provides common token manipulation methods used across all parser modules.

use super::super::{Keyword, Token};
use crate::error::Result;

/// Base trait for token navigation and manipulation.
/// All other parser traits should extend this trait.
pub trait TokenHelper {
    /// Fetches the next lexer token, or errors if none is found.
    fn next(&mut self) -> Result<Token>;

    /// Returns the next identifier, or errors if not found.
    fn next_ident(&mut self) -> Result<String>;

    /// Returns the next identifier or keyword as identifier (for contexts where keywords can be identifiers).
    fn next_ident_or_keyword(&mut self) -> Result<String>;

    /// Returns the next lexer token if it satisfies the predicate.
    fn next_if(&mut self, predicate: impl Fn(&Token) -> bool) -> Option<Token>;

    /// Passes the next lexer token through the closure, consuming it if the
    /// closure returns Some. Returns the result of the closure.
    fn next_if_map<T>(&mut self, f: impl Fn(&Token) -> Option<T>) -> Option<T>;

    /// Returns the next keyword if there is one.
    fn next_if_keyword(&mut self) -> Option<Keyword>;

    /// Returns true if the next token is an identifier matching the given string (case-insensitive)
    fn next_if_ident_eq(&mut self, expected: &str) -> bool;

    /// Consumes the next lexer token if it is the given token, returning true.
    fn next_is(&mut self, token: Token) -> bool;

    /// Consumes the next lexer token if it's the expected token, or errors.
    fn expect(&mut self, expect: Token) -> Result<()>;

    /// Consumes the next lexer token if it is the given token. Equivalent to
    /// next_is(), but expresses intent better.
    fn skip(&mut self, token: Token) {
        self.next_is(token);
    }

    /// Peeks the next lexer token if any, without consuming it.
    fn peek(&mut self) -> Result<Option<&Token>>;
}
