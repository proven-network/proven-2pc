//! DDL (Data Definition Language) statement parser module
//!
//! Handles parsing of CREATE and DROP statements for tables and indexes.

use super::super::{Keyword, Token};
use super::type_parser::TypeParser;
use crate::error::{Error, Result};
use crate::parsing::ast::Expression;
use crate::parsing::ast::common::Direction;
use crate::parsing::ast::ddl::DdlStatement;
use crate::parsing::ast::dml::ValuesStatement;
use crate::parsing::ast::{Column, IndexColumn, Statement};

/// Parser trait for DDL statements
pub trait DdlParser: TypeParser {
    /// Parses an expression
    fn parse_expression(&mut self) -> Result<Expression>;

    /// Parses VALUES rows - reused from DML parser
    fn parse_values_rows(&mut self) -> Result<Vec<Vec<Expression>>>;

    /// Parses a table-level constraint
    fn parse_table_constraint(
        &mut self,
    ) -> Result<Option<crate::parsing::ast::ddl::ForeignKeyConstraint>>;

    /// Parses a referential action
    fn parse_referential_action(&mut self) -> Result<crate::parsing::ast::ddl::ReferentialAction>;

    /// Parses a CREATE statement (TABLE or INDEX).
    fn parse_create(&mut self) -> Result<Statement> {
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
    fn parse_drop(&mut self) -> Result<Statement> {
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
    fn parse_create_table_inner(&mut self) -> Result<Statement> {
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

        // Check for AS VALUES
        if self.next_is(Keyword::As.into()) {
            // Parse AS VALUES
            self.expect(Keyword::Values.into())?;
            let rows = self.parse_values_rows()?;

            // VALUES doesn't support ORDER BY, LIMIT, OFFSET in CREATE TABLE AS context
            Ok(Statement::Ddl(DdlStatement::CreateTableAsValues {
                name,
                values: ValuesStatement {
                    rows,
                    order_by: Vec::new(),
                    limit: None,
                    offset: None,
                },
                if_not_exists,
            }))
        } else if self.next_is(Token::OpenParen) {
            // Parse column list and constraints
            let mut columns = Vec::new();
            let mut foreign_keys = Vec::new();

            // Check for empty column list
            if !self.next_is(Token::CloseParen) {
                loop {
                    // Check for table-level constraints
                    if self.peek()? == Some(&Token::Keyword(Keyword::Constraint))
                        || self.peek()? == Some(&Token::Keyword(Keyword::Foreign))
                    {
                        // Parse table-level constraint
                        if let Some(fk) = self.parse_table_constraint()? {
                            foreign_keys.push(fk);
                        }
                    } else {
                        // Parse column definition
                        columns.push(self.parse_create_table_column()?);
                    }

                    if !self.next_is(Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::CloseParen)?;
            }
            Ok(Statement::Ddl(DdlStatement::CreateTable {
                name,
                columns,
                foreign_keys,
                if_not_exists,
            }))
        } else {
            // Table without columns
            Ok(Statement::Ddl(DdlStatement::CreateTable {
                name,
                columns: Vec::new(),
                foreign_keys: Vec::new(),
                if_not_exists,
            }))
        }
    }

    /// Parses a CREATE TABLE column definition.
    fn parse_create_table_column(&mut self) -> Result<Column> {
        let name = self.next_ident_or_keyword()?;

        // Use the TypeParser trait method for type parsing
        let data_type = self.parse_type()?;

        let mut column = Column {
            name,
            data_type,
            primary_key: false,
            nullable: None,
            default: None,
            unique: false,
            index: false,
            references: None,
        };

        // Parse column constraints
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
                Keyword::References => {
                    // Parse REFERENCES table_name [(column_name)]
                    let table = self.next_ident()?;
                    // For inline references, we only store the table name
                    // Column-level FK doesn't support ON DELETE/UPDATE actions
                    column.references = Some(table);

                    // Skip optional column specification for now
                    if self.peek()? == Some(&Token::OpenParen) {
                        self.next()?; // consume (
                        self.next_ident()?; // consume column name
                        self.expect(Token::CloseParen)?;
                    }
                }
                keyword => {
                    return Err(Error::ParseError(format!("unexpected keyword {}", keyword)));
                }
            }
        }
        Ok(column)
    }

    /// Parses an index column: an expression optionally followed by ASC or DESC
    fn parse_index_column(&mut self) -> Result<IndexColumn> {
        // Parse the expression (can be a simple column or complex expression)
        let expression = self.parse_expression()?;

        // Check for optional ASC or DESC
        let direction = match self.peek()? {
            Some(Token::Keyword(Keyword::Asc)) => {
                self.next()?;
                Some(Direction::Asc)
            }
            Some(Token::Keyword(Keyword::Desc)) => {
                self.next()?;
                Some(Direction::Desc)
            }
            _ => None,
        };

        Ok(IndexColumn {
            expression,
            direction,
        })
    }

    /// Parses a CREATE INDEX statement (after CREATE [UNIQUE] INDEX).
    fn parse_create_index_inner(&mut self, unique: bool) -> Result<Statement> {
        let name = self.next_ident()?;
        self.expect(Keyword::On.into())?;
        let table = self.next_ident()?;
        self.expect(Token::OpenParen)?;

        // Parse one or more index columns (expressions with optional ASC/DESC)
        let mut columns = vec![self.parse_index_column()?];
        while self.next_is(Token::Comma) {
            columns.push(self.parse_index_column()?);
        }

        self.expect(Token::CloseParen)?;

        // Parse optional INCLUDE clause for covering indexes
        let included_columns = if self.next_is(Keyword::Include.into()) {
            self.expect(Token::OpenParen)?;
            let mut included = vec![self.next_ident_or_keyword()?];
            while self.next_is(Token::Comma) {
                included.push(self.next_ident_or_keyword()?);
            }
            self.expect(Token::CloseParen)?;
            Some(included)
        } else {
            None
        };

        Ok(Statement::Ddl(DdlStatement::CreateIndex {
            name,
            table,
            columns,
            unique,
            included_columns,
        }))
    }

    /// Parses a DROP INDEX statement (after DROP INDEX).
    fn parse_drop_index_inner(&mut self) -> Result<Statement> {
        let mut if_exists = false;
        if self.next_is(Keyword::If.into()) {
            self.expect(Token::Keyword(Keyword::Exists))?;
            if_exists = true;
        }
        let name = self.next_ident()?;
        Ok(Statement::Ddl(DdlStatement::DropIndex { name, if_exists }))
    }

    /// Parses a DROP TABLE statement (after DROP).
    fn parse_drop_table_inner(&mut self) -> Result<Statement> {
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

        // Check for CASCADE keyword
        let cascade = self.next_is(Keyword::Cascade.into());

        Ok(Statement::Ddl(DdlStatement::DropTable {
            names,
            if_exists,
            cascade,
        }))
    }
}
