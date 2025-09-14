use std::iter::Peekable;
use std::ops::Add;

use super::{Keyword, Lexer, Token, ast};
use crate::error::{Error, Result};
use crate::types::data_type::DataType;

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
    pub fn parse(statement: &str) -> Result<ast::Statement> {
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
    pub fn parse_expr(expr: &str) -> Result<ast::Expression> {
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
    fn parse_statement(&mut self) -> Result<ast::Statement> {
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
    fn parse_explain(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Explain.into())?;
        if self.next_is(Keyword::Explain.into()) {
            return Err(Error::ParseError("cannot nest EXPLAIN statements".into()));
        }
        Ok(ast::Statement::Explain(Box::new(self.parse_statement()?)))
    }

    /// Parses a CREATE statement (TABLE or INDEX).
    fn parse_create(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Create.into())?;
        match self.peek()? {
            Some(Token::Keyword(Keyword::Table)) => self.parse_create_table_inner(),
            Some(Token::Keyword(Keyword::Unique)) => {
                self.next()?; // consume UNIQUE
                self.expect(Keyword::Index.into())?;
                self.parse_create_index_inner(true)
            }
            Some(Token::Keyword(Keyword::Index)) => {
                self.next()?; // consume INDEX
                self.parse_create_index_inner(false)
            }
            Some(token) => Err(Error::ParseError(format!(
                "expected TABLE or INDEX after CREATE, found {}",
                token
            ))),
            None => Err(Error::ParseError(
                "unexpected end of input after CREATE".into(),
            )),
        }
    }

    /// Parses a DROP statement (TABLE or INDEX).
    fn parse_drop(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Drop.into())?;
        match self.peek()? {
            Some(Token::Keyword(Keyword::Table)) => self.parse_drop_table_inner(),
            Some(Token::Keyword(Keyword::Index)) => {
                self.next()?; // consume INDEX
                self.parse_drop_index_inner()
            }
            Some(token) => Err(Error::ParseError(format!(
                "expected TABLE or INDEX after DROP, found {}",
                token
            ))),
            None => Err(Error::ParseError(
                "unexpected end of input after DROP".into(),
            )),
        }
    }

    /// Parses a CREATE TABLE statement (after CREATE).
    fn parse_create_table_inner(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Table.into())?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.next_is(Keyword::If.into()) {
            self.expect(Keyword::Not.into())?;
            self.expect(Keyword::Exists.into())?;
            true
        } else {
            false
        };

        let name = self.next_ident()?;

        // Check if there's a column list
        let columns = if self.next_is(Token::OpenParen) {
            let mut columns = Vec::new();
            // Check for empty column list
            if !self.next_is(Token::CloseParen) {
                loop {
                    columns.push(self.parse_create_table_column()?);
                    if !self.next_is(Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::CloseParen)?;
            }
            columns
        } else {
            // Table without columns
            Vec::new()
        };
        Ok(ast::Statement::CreateTable {
            name,
            columns,
            if_not_exists,
        })
    }

    /// Parses a CREATE TABLE column definition.
    fn parse_create_table_column(&mut self) -> Result<ast::Column> {
        let name = self.next_ident_or_keyword()?;
        let datatype = match self.next()? {
            // Boolean types
            Token::Keyword(Keyword::Bool | Keyword::Boolean) => DataType::Bool,

            // Integer types - conventional SQL names (check for UNSIGNED modifier)
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
            Token::Keyword(Keyword::Real) => DataType::F32, // SQL standard single precision
            Token::Keyword(Keyword::Float | Keyword::Double) => DataType::F64, // FLOAT defaults to double

            // Decimal types
            Token::Keyword(Keyword::Decimal) => {
                // TODO: Parse precision and scale from DECIMAL(p,s)
                DataType::Decimal(Some(38), Some(10))
            }

            // String types
            Token::Keyword(Keyword::String | Keyword::Text | Keyword::Varchar) => DataType::Str,

            // UUID is now a keyword
            Token::Keyword(Keyword::Uuid) => DataType::Uuid,

            // Other types as identifiers (for now)
            Token::Ident(s) if s.to_uppercase() == "TIMESTAMP" => DataType::Timestamp,
            Token::Ident(s) if s.to_uppercase() == "DATE" => DataType::Date,
            Token::Ident(s) if s.to_uppercase() == "TIME" => DataType::Time,
            Token::Ident(s) if s.to_uppercase() == "INTERVAL" => DataType::Interval,
            Token::Ident(s) if s.to_uppercase() == "BLOB" || s.to_uppercase() == "BYTEA" => {
                DataType::Bytea
            }
            Token::Ident(s) if s.to_uppercase() == "INET" => DataType::Inet,
            Token::Ident(s) if s.to_uppercase() == "POINT" => DataType::Point,

            token => {
                return Err(Error::ParseError(format!(
                    "unexpected token {}, expected data type",
                    token
                )));
            }
        };
        let mut column = ast::Column {
            name,
            datatype,
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
            references: None,
        };
        while let Some(keyword) = self.next_if_keyword() {
            match keyword {
                Keyword::Primary => {
                    self.expect(Keyword::Key.into())?;
                    column.primary_key = true;
                }
                Keyword::Null => {
                    if column.nullable.is_some() {
                        return Err(Error::ParseError(format!(
                            "nullability already set for column {}",
                            column.name
                        )));
                    }
                    column.nullable = Some(true)
                }
                Keyword::Not => {
                    self.expect(Keyword::Null.into())?;
                    if column.nullable.is_some() {
                        return Err(Error::ParseError(format!(
                            "nullability already set for column {}",
                            column.name
                        )));
                    }
                    column.nullable = Some(false)
                }
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Unique => column.unique = true,
                Keyword::Index => column.index = true,
                Keyword::References => column.references = Some(self.next_ident()?),
                keyword => {
                    return Err(Error::ParseError(format!("unexpected keyword {}", keyword)));
                }
            }
        }
        Ok(column)
    }

    /// Parses a CREATE INDEX statement (after CREATE [UNIQUE] INDEX).
    fn parse_create_index_inner(&mut self, unique: bool) -> Result<ast::Statement> {
        let name = self.next_ident()?;
        self.expect(Keyword::On.into())?;
        let table = self.next_ident()?;
        self.expect(Token::OpenParen)?;

        // Parse one or more columns for composite index support
        let mut columns = vec![self.next_ident()?];
        while self.next_is(Token::Comma) {
            columns.push(self.next_ident()?);
        }

        self.expect(Token::CloseParen)?;

        // Parse optional INCLUDE clause for covering indexes
        let included_columns = if self.next_is(Keyword::Include.into()) {
            self.expect(Token::OpenParen)?;
            let mut included = vec![self.next_ident()?];
            while self.next_is(Token::Comma) {
                included.push(self.next_ident()?);
            }
            self.expect(Token::CloseParen)?;
            Some(included)
        } else {
            None
        };

        Ok(ast::Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
            included_columns,
        })
    }

    /// Parses a DROP INDEX statement (after DROP INDEX).
    fn parse_drop_index_inner(&mut self) -> Result<ast::Statement> {
        let mut if_exists = false;
        if self.next_is(Keyword::If.into()) {
            self.expect(Token::Keyword(Keyword::Exists))?;
            if_exists = true;
        }
        let name = self.next_ident()?;
        Ok(ast::Statement::DropIndex { name, if_exists })
    }

    /// Parses a DROP TABLE statement (after DROP).
    fn parse_drop_table_inner(&mut self) -> Result<ast::Statement> {
        self.expect(Token::Keyword(Keyword::Table))?;
        let mut if_exists = false;
        if self.next_is(Keyword::If.into()) {
            self.expect(Token::Keyword(Keyword::Exists))?;
            if_exists = true;
        }

        // Parse one or more table names separated by commas
        let mut names = vec![self.next_ident()?];
        while self.next_is(Token::Comma) {
            names.push(self.next_ident()?);
        }

        Ok(ast::Statement::DropTable { names, if_exists })
    }

    /// Parses a DELETE statement.
    fn parse_delete(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Delete.into())?;
        self.expect(Keyword::From.into())?;
        let table = self.next_ident()?;
        Ok(ast::Statement::Delete {
            table,
            r#where: self.parse_where_clause()?,
        })
    }

    /// Parses an INSERT statement.
    fn parse_insert(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Insert.into())?;
        self.expect(Keyword::Into.into())?;
        let table = self.next_ident()?;

        let mut columns = None;
        if self.next_is(Token::OpenParen) {
            let columns = columns.insert(Vec::new());
            loop {
                columns.push(self.next_ident()?);
                if !self.next_is(Token::Comma) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
        }

        // Check for VALUES or SELECT
        let source = if self.next_is(Keyword::Values.into()) {
            let mut values = Vec::new();
            loop {
                let mut row = Vec::new();
                self.expect(Token::OpenParen)?;
                loop {
                    row.push(self.parse_expression()?);
                    if !self.next_is(Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::CloseParen)?;
                values.push(row);
                if !self.next_is(Token::Comma) {
                    break;
                }
            }
            ast::InsertSource::Values(values)
        } else if matches!(self.peek()?, Some(Token::Keyword(Keyword::Select))) {
            // Parse the SELECT statement - parse_select_clause will consume SELECT
            let select = Box::new(ast::SelectStatement {
                select: self.parse_select_clause()?,
                from: self.parse_from_clause()?,
                r#where: self.parse_where_clause()?,
                group_by: self.parse_group_by_clause()?,
                having: self.parse_having_clause()?,
                order_by: self.parse_order_by_clause()?,
                limit: self.parse_limit_clause()?,
                offset: self.parse_offset_clause()?,
            });
            ast::InsertSource::Select(select)
        } else {
            return Err(Error::ParseError(
                "expected token VALUES or SELECT after INSERT INTO".to_string(),
            ));
        };

        Ok(ast::Statement::Insert {
            table,
            columns,
            source,
        })
    }

    /// Parses an UPDATE statement.
    fn parse_update(&mut self) -> Result<ast::Statement> {
        self.expect(Keyword::Update.into())?;
        let table = self.next_ident()?;
        self.expect(Keyword::Set.into())?;
        let mut set = std::collections::BTreeMap::new();
        loop {
            let column = self.next_ident()?;
            self.expect(Token::Equal)?;
            let expr = (!self.next_is(Keyword::Default.into()))
                .then(|| self.parse_expression())
                .transpose()?;
            if set.contains_key(&column) {
                return Err(Error::ParseError(format!(
                    "column {} set multiple times",
                    column
                )));
            }
            set.insert(column, expr);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(ast::Statement::Update {
            table,
            set,
            r#where: self.parse_where_clause()?,
        })
    }

    /// Parses a SELECT statement.
    fn parse_select(&mut self) -> Result<ast::Statement> {
        Ok(ast::Statement::Select(Box::new(ast::SelectStatement {
            select: self.parse_select_clause()?,
            from: self.parse_from_clause()?,
            r#where: self.parse_where_clause()?,
            group_by: self.parse_group_by_clause()?,
            having: self.parse_having_clause()?,
            order_by: self.parse_order_by_clause()?,
            limit: self.parse_limit_clause()?,
            offset: self.parse_offset_clause()?,
        })))
    }

    /// Parses a SELECT clause, if present.
    fn parse_select_clause(&mut self) -> Result<Vec<(ast::Expression, Option<String>)>> {
        if !self.next_is(Keyword::Select.into()) {
            return Ok(Vec::new());
        }
        let mut select = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            let mut alias = None;
            if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
                if expr == ast::Expression::All {
                    return Err(Error::ParseError("can't alias *".into()));
                }
                alias = Some(self.next_ident()?);
            }
            select.push((expr, alias));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(select)
    }

    /// Parses a FROM clause, if present.
    fn parse_from_clause(&mut self) -> Result<Vec<ast::FromClause>> {
        if !self.next_is(Keyword::From.into()) {
            return Ok(Vec::new());
        }
        let mut from = Vec::new();
        loop {
            let mut from_item = self.parse_from_table()?;
            while let Some(r#type) = self.parse_from_join()? {
                let left = Box::new(from_item);
                let right = Box::new(self.parse_from_table()?);
                let mut predicate = None;
                if r#type != ast::JoinType::Cross {
                    self.expect(Keyword::On.into())?;
                    predicate = Some(self.parse_expression()?)
                }
                from_item = ast::FromClause::Join {
                    left,
                    right,
                    r#type,
                    predicate,
                };
            }
            from.push(from_item);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(from)
    }

    // Parses a FROM table.
    fn parse_from_table(&mut self) -> Result<ast::FromClause> {
        let name = self.next_ident()?;

        // Check for compound object notation (schema.table)
        if matches!(self.peek()?, Some(Token::Period)) {
            self.next()?; // consume the period
            let _object = self.next_ident()?; // consume the object name
            return Err(Error::CompoundObjectNotSupported);
        }

        let mut alias = None;
        if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
            alias = Some(self.next_ident()?)
        };
        Ok(ast::FromClause::Table { name, alias })
    }

    // Parses a FROM JOIN type, if present.
    fn parse_from_join(&mut self) -> Result<Option<ast::JoinType>> {
        if self.next_is(Keyword::Join.into()) {
            return Ok(Some(ast::JoinType::Inner));
        }
        if self.next_is(Keyword::Cross.into()) {
            self.expect(Keyword::Join.into())?;
            return Ok(Some(ast::JoinType::Cross));
        }
        if self.next_is(Keyword::Inner.into()) {
            self.expect(Keyword::Join.into())?;
            return Ok(Some(ast::JoinType::Inner));
        }
        if self.next_is(Keyword::Left.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(ast::JoinType::Left));
        }
        if self.next_is(Keyword::Right.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(ast::JoinType::Right));
        }
        if self.next_is(Keyword::Full.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(ast::JoinType::Full));
        }
        Ok(None)
    }

    /// Parses a WHERE clause, if present.
    fn parse_where_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Where.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses a GROUP BY clause, if present.
    fn parse_group_by_clause(&mut self) -> Result<Vec<ast::Expression>> {
        if !self.next_is(Keyword::Group.into()) {
            return Ok(Vec::new());
        }
        let mut group_by = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            group_by.push(self.parse_expression()?);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(group_by)
    }

    /// Parses a HAVING clause, if present.
    fn parse_having_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Having.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses an ORDER BY clause, if present.
    fn parse_order_by_clause(&mut self) -> Result<Vec<(ast::Expression, ast::Direction)>> {
        if !self.next_is(Keyword::Order.into()) {
            return Ok(Vec::new());
        }
        let mut order_by = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            let expr = self.parse_expression()?;
            let order = self
                .next_if_map(|token| match token {
                    Token::Keyword(Keyword::Asc) => Some(ast::Direction::Ascending),
                    Token::Keyword(Keyword::Desc) => Some(ast::Direction::Descending),
                    _ => None,
                })
                .unwrap_or_default();
            order_by.push((expr, order));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(order_by)
    }

    /// Parses a LIMIT clause, if present.
    fn parse_limit_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Limit.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses an OFFSET clause, if present.
    fn parse_offset_clause(&mut self) -> Result<Option<ast::Expression>> {
        if !self.next_is(Keyword::Offset.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
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
    fn parse_expression(&mut self) -> Result<ast::Expression> {
        self.parse_expression_at(0)
    }

    /// Parses an expression at the given minimum precedence.
    fn parse_expression_at(&mut self, min_precedence: Precedence) -> Result<ast::Expression> {
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
    fn parse_expression_atom(&mut self) -> Result<ast::Expression> {
        Ok(match self.next()? {
            // All columns.
            Token::Asterisk => ast::Expression::All,

            // Literal value.
            Token::Number(n) if n.chars().all(|c| c.is_ascii_digit()) => {
                // Try to parse as i128 first, then as u128 if that fails
                match n.parse::<i128>() {
                    Ok(val) => ast::Literal::Integer(val).into(),
                    Err(_) => {
                        // Try parsing as u128 for large unsigned values
                        match n.parse::<u128>() {
                            Ok(val) => {
                                // We need to handle u128 values that are > i128::MAX
                                // For now, we'll store them as i128 and let coercion handle it
                                // This is a bit of a hack but works for INSERT INTO unsigned columns
                                if val <= i128::MAX as u128 {
                                    ast::Literal::Integer(val as i128).into()
                                } else {
                                    // For values > i128::MAX, use Float to preserve the value
                                    // This will be converted to U128 later during coercion
                                    ast::Literal::Float(val as f64).into()
                                }
                            }
                            Err(e) => {
                                return Err(Error::ParseError(format!("invalid integer: {}", e)));
                            }
                        }
                    }
                }
            }
            Token::Number(n) => ast::Literal::Float(
                n.parse()
                    .map_err(|e| Error::ParseError(format!("invalid float: {}", e)))?,
            )
            .into(),
            Token::String(s) => ast::Literal::String(s).into(),
            Token::HexString(h) => {
                // Convert hex string to bytes
                match hex::decode(&h) {
                    Ok(bytes) => ast::Literal::Bytea(bytes).into(),
                    Err(_) => return Err(Error::ParseError(format!("Invalid hex string: {}", h))),
                }
            }
            Token::Keyword(Keyword::True) => ast::Literal::Boolean(true).into(),
            Token::Keyword(Keyword::False) => ast::Literal::Boolean(false).into(),
            Token::Keyword(Keyword::Infinity) => ast::Literal::Float(f64::INFINITY).into(),
            Token::Keyword(Keyword::NaN) => ast::Literal::Float(f64::NAN).into(),
            Token::Keyword(Keyword::Null) => ast::Literal::Null.into(),

            // CAST expression: CAST(expr AS type)
            Token::Keyword(Keyword::Cast) => {
                self.expect(Token::OpenParen)?;
                let expr = self.parse_expression()?;
                self.expect(Token::Keyword(Keyword::As))?;

                // Parse the target type (with support for UNSIGNED)
                let type_name = match self.next()? {
                    Token::Keyword(Keyword::Tinyint) => {
                        if self.next_if_ident_eq("UNSIGNED") {
                            "TINYINT UNSIGNED"
                        } else {
                            "TINYINT"
                        }
                    }
                    Token::Keyword(Keyword::Smallint) => {
                        if self.next_if_ident_eq("UNSIGNED") {
                            "SMALLINT UNSIGNED"
                        } else {
                            "SMALLINT"
                        }
                    }
                    Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => {
                        if self.next_if_ident_eq("UNSIGNED") {
                            "INT UNSIGNED"
                        } else {
                            "INT"
                        }
                    }
                    Token::Keyword(Keyword::Bigint) => {
                        if self.next_if_ident_eq("UNSIGNED") {
                            "BIGINT UNSIGNED"
                        } else {
                            "BIGINT"
                        }
                    }
                    Token::Keyword(Keyword::Hugeint) => {
                        if self.next_if_ident_eq("UNSIGNED") {
                            "HUGEINT UNSIGNED"
                        } else {
                            "HUGEINT"
                        }
                    }
                    Token::Keyword(Keyword::Real) => "REAL", // REAL is F32
                    Token::Keyword(Keyword::Float) => "FLOAT", // FLOAT is F64
                    Token::Keyword(Keyword::Double) => "DOUBLE",
                    Token::Keyword(Keyword::Decimal) => "DECIMAL",
                    Token::Keyword(Keyword::Text)
                    | Token::Keyword(Keyword::String)
                    | Token::Keyword(Keyword::Varchar) => "TEXT",
                    Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => "BOOLEAN",
                    Token::Keyword(Keyword::Uuid) => "UUID",
                    token => {
                        return Err(Error::ParseError(format!(
                            "expected type name after AS, found {}",
                            token
                        )));
                    }
                };

                self.expect(Token::CloseParen)?;

                // Use the Function expression with a special CAST function name
                // We'll encode the type as the second argument
                ast::Expression::Function(
                    "CAST".to_string(),
                    vec![
                        expr,
                        ast::Expression::Literal(ast::Literal::String(type_name.to_string())),
                    ],
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
                    ast::Expression::Function(format!("{}_DISTINCT", name.to_uppercase()), args)
                } else {
                    ast::Expression::Function(name, args)
                }
            }

            // Column name, either qualified as table.column or unqualified.
            Token::Ident(table) if self.next_is(Token::Period) => {
                ast::Expression::Column(Some(table), self.next_ident()?)
            }
            Token::Ident(column) => ast::Expression::Column(None, column),

            // Parameter placeholder (?)
            Token::Question => {
                let param_idx = self.param_count;
                self.param_count += 1;
                ast::Expression::Parameter(param_idx)
            }

            // Parenthesized expression.
            Token::OpenParen => {
                let expr = self.parse_expression()?;
                self.expect(Token::CloseParen)?;
                expr
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
            if PostfixOperator::Is(ast::Literal::Null).precedence() < min_precedence {
                return Ok(None);
            }
            self.expect(Keyword::Is.into())?;
            let not = self.next_is(Keyword::Not.into());
            let value = match self.next()? {
                Token::Keyword(Keyword::NaN) => ast::Literal::Float(f64::NAN),
                Token::Keyword(Keyword::Null) => ast::Literal::Null,
                token => return Err(Error::ParseError(format!("unexpected token {}", token))),
            };
            let operator = match not {
                false => PostfixOperator::Is(value),
                true => PostfixOperator::IsNot(value),
            };
            return Ok(Some(operator));
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

/// Operator precedence.
type Precedence = u8;

/// Operator associativity.
enum Associativity {
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
enum PrefixOperator {
    Minus, // -a
    Not,   // NOT a
    Plus,  // +a
}

impl PrefixOperator {
    /// The operator precedence.
    fn precedence(&self) -> Precedence {
        match self {
            Self::Not => 3,
            Self::Minus | Self::Plus => 10,
        }
    }

    // The operator associativity. Prefix operators are right-associative by
    // definition.
    fn associativity(&self) -> Associativity {
        Associativity::Right
    }

    /// Builds an AST expression for the operator.
    fn into_expression(self, rhs: ast::Expression) -> ast::Expression {
        let rhs = Box::new(rhs);
        match self {
            Self::Plus => ast::Operator::Identity(rhs).into(),
            Self::Minus => ast::Operator::Negate(rhs).into(),
            Self::Not => ast::Operator::Not(rhs).into(),
        }
    }
}

/// Infix operators.
enum InfixOperator {
    Add,                // a + b
    And,                // a AND b
    Divide,             // a / b
    Equal,              // a = b
    Exponentiate,       // a ^ b
    GreaterThan,        // a > b
    GreaterThanOrEqual, // a >= b
    LessThan,           // a < b
    LessThanOrEqual,    // a <= b
    Like,               // a LIKE b
    Multiply,           // a * b
    NotEqual,           // a != b
    Or,                 // a OR b
    Remainder,          // a % b
    Subtract,           // a - b
}

impl InfixOperator {
    /// The operator precedence.
    ///
    /// Mostly follows Postgres, except IS and LIKE having same precedence as =.
    /// This is similar to SQLite and MySQL.
    fn precedence(&self) -> Precedence {
        match self {
            Self::Or => 1,
            Self::And => 2,
            // Self::Not => 3
            Self::Equal | Self::NotEqual | Self::Like => 4, // also Self::Is
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 5,
            Self::Add | Self::Subtract => 6,
            Self::Multiply | Self::Divide | Self::Remainder => 7,
            Self::Exponentiate => 8,
        }
    }

    /// The operator associativity.
    fn associativity(&self) -> Associativity {
        match self {
            Self::Exponentiate => Associativity::Right,
            _ => Associativity::Left,
        }
    }

    /// Builds an AST expression for the infix operator.
    fn into_expression(self, lhs: ast::Expression, rhs: ast::Expression) -> ast::Expression {
        let (lhs, rhs) = (Box::new(lhs), Box::new(rhs));
        match self {
            Self::Add => ast::Operator::Add(lhs, rhs).into(),
            Self::And => ast::Operator::And(lhs, rhs).into(),
            Self::Divide => ast::Operator::Divide(lhs, rhs).into(),
            Self::Equal => ast::Operator::Equal(lhs, rhs).into(),
            Self::Exponentiate => ast::Operator::Exponentiate(lhs, rhs).into(),
            Self::GreaterThan => ast::Operator::GreaterThan(lhs, rhs).into(),
            Self::GreaterThanOrEqual => ast::Operator::GreaterThanOrEqual(lhs, rhs).into(),
            Self::LessThan => ast::Operator::LessThan(lhs, rhs).into(),
            Self::LessThanOrEqual => ast::Operator::LessThanOrEqual(lhs, rhs).into(),
            Self::Like => ast::Operator::Like(lhs, rhs).into(),
            Self::Multiply => ast::Operator::Multiply(lhs, rhs).into(),
            Self::NotEqual => ast::Operator::NotEqual(lhs, rhs).into(),
            Self::Or => ast::Operator::Or(lhs, rhs).into(),
            Self::Remainder => ast::Operator::Remainder(lhs, rhs).into(),
            Self::Subtract => ast::Operator::Subtract(lhs, rhs).into(),
        }
    }
}

/// Postfix operators.
enum PostfixOperator {
    Factorial,           // a!
    Is(ast::Literal),    // a IS NULL | NAN
    IsNot(ast::Literal), // a IS NOT NULL | NAN
}

impl PostfixOperator {
    // The operator precedence.
    fn precedence(&self) -> Precedence {
        match self {
            Self::Is(_) | Self::IsNot(_) => 4,
            Self::Factorial => 9,
        }
    }

    /// Builds an AST expression for the operator.
    fn into_expression(self, lhs: ast::Expression) -> ast::Expression {
        let lhs = Box::new(lhs);
        match self {
            Self::Factorial => ast::Operator::Factorial(lhs).into(),
            Self::Is(v) => ast::Operator::Is(lhs, v).into(),
            Self::IsNot(v) => ast::Operator::Not(Box::new(ast::Operator::Is(lhs, v).into())).into(),
        }
    }
}
