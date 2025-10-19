//! DML (Data Manipulation Language) statement parser module
//!
//! Handles parsing of SELECT, INSERT, UPDATE, and DELETE statements.

use super::super::{Keyword, Token};
use super::token_helper::TokenHelper;
use crate::error::{Error, Result};
use crate::parsing::ast::common::{Direction, FromClause, JoinType, SubquerySource, TableAlias};
use crate::parsing::ast::dml::{DistinctClause, DmlStatement, ValuesStatement};
use crate::parsing::ast::{Expression, InsertSource, SelectStatement, Statement};
use std::collections::BTreeMap;

/// Type alias for SELECT clause parsing result: (distinct_clause, select_expressions)
type SelectClauseResult = (DistinctClause, Vec<(Expression, Option<String>)>);

/// Parser trait for DML statements
pub trait DmlParser: TokenHelper {
    /// Parses an expression
    fn parse_expression(&mut self) -> Result<Expression>;

    /// Parses a DELETE statement.
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(Keyword::Delete.into())?;
        self.expect(Keyword::From.into())?;
        let table = self.next_ident()?;
        Ok(Statement::Dml(DmlStatement::Delete {
            table,
            r#where: self.parse_where_clause()?,
        }))
    }

    /// Parses an INSERT statement.
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Keyword::Insert.into())?;
        self.expect(Keyword::Into.into())?;
        let table = self.next_ident()?;

        let mut columns = None;
        if self.next_is(Token::OpenParen) {
            let columns = columns.insert(Vec::new());
            loop {
                columns.push(self.next_ident_or_keyword()?);
                if !self.next_is(Token::Comma) {
                    break;
                }
            }
            self.expect(Token::CloseParen)?;
        }

        // Check for DEFAULT VALUES, VALUES, or SELECT
        let source = if self.next_is(Keyword::Default.into()) {
            self.expect(Keyword::Values.into())?;
            InsertSource::DefaultValues
        } else if self.next_is(Keyword::Values.into()) {
            InsertSource::Values(self.parse_values_rows()?)
        } else if matches!(self.peek()?, Some(Token::Keyword(Keyword::Select))) {
            // Parse the SELECT statement - parse_select_clause will consume SELECT
            let (distinct, select) = self.parse_select_clause()?;
            let from = self.parse_from_clause()?;
            let r#where = self.parse_where_clause()?;
            let group_by = self.parse_group_by_clause()?;
            let having = self.parse_having_clause()?;
            let order_by = self.parse_order_by_clause()?;
            let (offset, limit) = self.parse_limit_offset_clause()?;
            let select = Box::new(SelectStatement {
                distinct,
                select,
                from,
                r#where,
                group_by,
                having,
                order_by,
                offset,
                limit,
            });
            InsertSource::Select(select)
        } else {
            return Err(Error::ParseError(
                "expected token VALUES or SELECT after INSERT INTO".to_string(),
            ));
        };

        Ok(Statement::Dml(DmlStatement::Insert {
            table,
            columns,
            source,
        }))
    }

    /// Parses an UPDATE statement.
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(Keyword::Update.into())?;
        let table = self.next_ident()?;
        self.expect(Keyword::Set.into())?;
        let mut set = BTreeMap::new();
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
        Ok(Statement::Dml(DmlStatement::Update {
            table,
            set,
            r#where: self.parse_where_clause()?,
        }))
    }

    /// Parses a SELECT statement.
    fn parse_select(&mut self) -> Result<Statement> {
        let (distinct, select) = self.parse_select_clause()?;
        let from = self.parse_from_clause()?;
        let r#where = self.parse_where_clause()?;
        let group_by = self.parse_group_by_clause()?;
        let having = self.parse_having_clause()?;
        let order_by = self.parse_order_by_clause()?;
        let (offset, limit) = self.parse_limit_offset_clause()?;
        Ok(Statement::Dml(DmlStatement::Select(Box::new(
            SelectStatement {
                distinct,
                select,
                from,
                r#where,
                group_by,
                having,
                order_by,
                offset,
                limit,
            },
        ))))
    }

    /// Parses a SELECT clause, if present.
    fn parse_select_clause(&mut self) -> Result<SelectClauseResult> {
        if !self.next_is(Keyword::Select.into()) {
            return Ok((DistinctClause::None, Vec::new()));
        }

        // Check for DISTINCT keyword
        let distinct = if self.next_is(Keyword::Distinct.into()) {
            // Check for DISTINCT ON (expr_list)
            if self.next_is(Keyword::On.into()) {
                self.expect(Token::OpenParen)?;
                let mut exprs = Vec::new();
                loop {
                    exprs.push(self.parse_expression()?);
                    if !self.next_is(Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::CloseParen)?;
                DistinctClause::On(exprs)
            } else {
                DistinctClause::All
            }
        } else {
            DistinctClause::None
        };

        let mut select = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            let mut alias = None;
            if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
                if expr == Expression::All {
                    return Err(Error::ParseError("can't alias *".into()));
                }
                if matches!(expr, Expression::QualifiedWildcard(_)) {
                    return Err(Error::ParseError("can't alias table.*".into()));
                }
                // Allow keywords as aliases (e.g., AS case)
                alias = Some(self.next_ident_or_keyword()?);
            }
            select.push((expr, alias));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok((distinct, select))
    }

    /// Parses a FROM clause, if present.
    fn parse_from_clause(&mut self) -> Result<Vec<FromClause>> {
        if !self.next_is(Keyword::From.into()) {
            return Ok(Vec::new());
        }
        let mut from_item = self.parse_from_table()?;
        while let Some(r#type) = self.parse_from_join()? {
            let left = Box::new(from_item);
            let right = Box::new(self.parse_from_table()?);
            let mut predicate = None;
            // Check for ON or USING clause
            if self.next_is(Keyword::On.into()) {
                predicate = Some(self.parse_expression()?)
            } else if self.next_is(Keyword::Using.into()) {
                // USING clause not yet fully implemented
                return Err(Error::ParseError(
                    "USING clause not yet supported in JOIN".into(),
                ));
            }
            from_item = FromClause::Join {
                left,
                right,
                r#type,
                predicate,
            };
        }

        // Check for comma-separated tables (not supported)
        if self.next_is(Token::Comma) {
            return Err(Error::ParseError(
                "Multiple tables in FROM clause not supported. Use JOIN syntax instead.".into(),
            ));
        }

        Ok(vec![from_item])
    }

    // Parses a FROM table or subquery.
    fn parse_from_table(&mut self) -> Result<FromClause> {
        // Check if this is a subquery
        if matches!(self.peek()?, Some(Token::OpenParen)) {
            self.next()?; // consume opening paren

            // Check what type of subquery this is
            let source = match self.peek()? {
                Some(Token::Keyword(Keyword::Select)) => {
                    // Parse SELECT subquery - parse_select_clause will consume SELECT
                    let (distinct, select) = self.parse_select_clause()?;
                    let from = self.parse_from_clause()?;
                    let r#where = self.parse_where_clause()?;
                    let group_by = self.parse_group_by_clause()?;
                    let having = self.parse_having_clause()?;
                    let order_by = self.parse_order_by_clause()?;
                    let (offset, limit) = self.parse_limit_offset_clause()?;
                    let select = Box::new(SelectStatement {
                        distinct,
                        select,
                        from,
                        r#where,
                        group_by,
                        having,
                        order_by,
                        offset,
                        limit,
                    });
                    SubquerySource::Select(select)
                }
                Some(Token::Keyword(Keyword::Values)) => {
                    // Parse VALUES subquery
                    self.next()?; // consume VALUES
                    let rows = self.parse_values_rows()?;
                    SubquerySource::Values(ValuesStatement {
                        rows,
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                    })
                }
                _ => {
                    return Err(Error::ParseError(
                        "Expected SELECT or VALUES after opening parenthesis in FROM clause".into(),
                    ));
                }
            };

            self.expect(Token::CloseParen)?;

            // Subqueries require an alias
            if !self.next_is(Keyword::As.into()) {
                return Err(Error::ParseError(
                    "Subquery in FROM clause requires an alias (AS name)".into(),
                ));
            }
            // Allow keywords as alias names
            let name = self.next_ident_or_keyword()?;

            // Check for column aliases: AS name(col1, col2, ...)
            let columns = if self.next_is(Token::OpenParen) {
                let mut cols = Vec::new();
                loop {
                    // Allow keywords as column names in alias lists
                    cols.push(self.next_ident_or_keyword()?);
                    if !self.next_is(Token::Comma) {
                        break;
                    }
                }
                self.expect(Token::CloseParen)?;
                cols
            } else {
                Vec::new()
            };

            Ok(FromClause::Subquery {
                source,
                alias: TableAlias { name, columns },
            })
        } else {
            // Parse regular table reference
            let name = self.next_ident()?;

            // Check for compound object notation (schema.table)
            if matches!(self.peek()?, Some(Token::Period)) {
                self.next()?; // consume the period
                let _object = self.next_ident()?; // consume the object name
                return Err(Error::CompoundObjectNotSupported);
            }

            // Check if this is SERIES(N) table-valued function
            if name.to_uppercase() == "SERIES" && matches!(self.peek()?, Some(Token::OpenParen)) {
                self.next()?; // consume opening paren

                // Parse the size expression
                let size = self.parse_expression()?;

                self.expect(Token::CloseParen)?;

                // Parse optional alias
                let mut alias = None;
                if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_)))
                {
                    // Allow keywords as alias names
                    let alias_name = self.next_ident_or_keyword()?;

                    // Check for column aliases: AS name(col1, col2, ...)
                    let columns = if self.next_is(Token::OpenParen) {
                        let mut cols = Vec::new();
                        loop {
                            // Allow keywords as column names in alias lists
                            cols.push(self.next_ident_or_keyword()?);
                            if !self.next_is(Token::Comma) {
                                break;
                            }
                        }
                        self.expect(Token::CloseParen)?;
                        cols
                    } else {
                        Vec::new()
                    };

                    alias = Some(TableAlias {
                        name: alias_name,
                        columns,
                    });
                };

                return Ok(FromClause::Series { size, alias });
            }

            let mut alias = None;
            if self.next_is(Keyword::As.into()) || matches!(self.peek()?, Some(Token::Ident(_))) {
                // Allow keywords as alias names
                let alias_name = self.next_ident_or_keyword()?;

                // Check for column aliases: AS name(col1, col2, ...)
                let columns = if self.next_is(Token::OpenParen) {
                    let mut cols = Vec::new();
                    loop {
                        // Allow keywords as column names in alias lists
                        cols.push(self.next_ident_or_keyword()?);
                        if !self.next_is(Token::Comma) {
                            break;
                        }
                    }
                    self.expect(Token::CloseParen)?;
                    cols
                } else {
                    Vec::new()
                };

                alias = Some(TableAlias {
                    name: alias_name,
                    columns,
                });
            };
            Ok(FromClause::Table { name, alias })
        }
    }

    // Parses a FROM JOIN type, if present.
    fn parse_from_join(&mut self) -> Result<Option<JoinType>> {
        if self.next_is(Keyword::Join.into()) {
            return Ok(Some(JoinType::Inner));
        }
        if self.next_is(Keyword::Cross.into()) {
            self.expect(Keyword::Join.into())?;
            return Ok(Some(JoinType::Cross));
        }
        if self.next_is(Keyword::Inner.into()) {
            self.expect(Keyword::Join.into())?;
            return Ok(Some(JoinType::Inner));
        }
        if self.next_is(Keyword::Left.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(JoinType::Left));
        }
        if self.next_is(Keyword::Right.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(JoinType::Right));
        }
        if self.next_is(Keyword::Full.into()) {
            self.skip(Keyword::Outer.into());
            self.expect(Keyword::Join.into())?;
            return Ok(Some(JoinType::Full));
        }
        Ok(None)
    }

    /// Parses a WHERE clause, if present.
    fn parse_where_clause(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Where.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses a GROUP BY clause, if present.
    fn parse_group_by_clause(&mut self) -> Result<Vec<Expression>> {
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
    fn parse_having_clause(&mut self) -> Result<Option<Expression>> {
        if !self.next_is(Keyword::Having.into()) {
            return Ok(None);
        }
        Ok(Some(self.parse_expression()?))
    }

    /// Parses an ORDER BY clause, if present.
    fn parse_order_by_clause(&mut self) -> Result<Vec<(Expression, Direction)>> {
        if !self.next_is(Keyword::Order.into()) {
            return Ok(Vec::new());
        }
        let mut order_by = Vec::new();
        self.expect(Keyword::By.into())?;
        loop {
            let expr = self.parse_expression()?;
            let order = self
                .next_if_map(|token| match token {
                    Token::Keyword(Keyword::Asc) => Some(Direction::Asc),
                    Token::Keyword(Keyword::Desc) => Some(Direction::Desc),
                    _ => None,
                })
                .unwrap_or(Direction::Asc);
            order_by.push((expr, order));
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(order_by)
    }

    /// Parses LIMIT and OFFSET clauses, if present.
    /// Supports multiple syntaxes:
    /// - LIMIT n [OFFSET m] (traditional MySQL/PostgreSQL syntax)
    /// - OFFSET n [ROWS] [FETCH FIRST/NEXT m ROW[S] ONLY] (SQL standard syntax)
    ///
    /// Returns (offset, limit) tuple
    fn parse_limit_offset_clause(&mut self) -> Result<(Option<Expression>, Option<Expression>)> {
        let mut offset = None;
        let mut limit = None;

        // Check for LIMIT first (traditional syntax)
        if self.next_is(Keyword::Limit.into()) {
            limit = Some(self.parse_expression()?);

            // Check for OFFSET after LIMIT
            if self.next_is(Keyword::Offset.into()) {
                offset = Some(self.parse_expression()?);
                // Optional ROW or ROWS keyword after OFFSET
                self.skip(Keyword::Row.into());
                self.skip(Keyword::Rows.into());
            }

            return Ok((offset, limit));
        }

        // Check for OFFSET first (SQL standard syntax with FETCH)
        if self.next_is(Keyword::Offset.into()) {
            offset = Some(self.parse_expression()?);

            // Optional ROW or ROWS keyword after OFFSET
            self.skip(Keyword::Row.into());
            self.skip(Keyword::Rows.into());

            // Check for FETCH FIRST/NEXT after OFFSET
            if self.next_is(Keyword::Fetch.into()) {
                // FIRST or NEXT are interchangeable
                if !self.next_is(Keyword::First.into()) && !self.next_is(Keyword::Next.into()) {
                    return Err(Error::ParseError(
                        "expected FIRST or NEXT after FETCH".to_string(),
                    ));
                }

                // Parse the count expression
                limit = Some(self.parse_expression()?);

                // Optional ROW or ROWS keyword (singular or plural)
                self.skip(Keyword::Row.into());
                self.skip(Keyword::Rows.into());

                // Require ONLY keyword
                self.expect(Keyword::Only.into())?;
            }

            return Ok((offset, limit));
        }

        // Check for standalone FETCH FIRST/NEXT (no OFFSET)
        if self.next_is(Keyword::Fetch.into()) {
            // FIRST or NEXT are interchangeable
            if !self.next_is(Keyword::First.into()) && !self.next_is(Keyword::Next.into()) {
                return Err(Error::ParseError(
                    "expected FIRST or NEXT after FETCH".to_string(),
                ));
            }

            // Parse the count expression
            limit = Some(self.parse_expression()?);

            // Optional ROW or ROWS keyword (singular or plural)
            self.skip(Keyword::Row.into());
            self.skip(Keyword::Rows.into());

            // Require ONLY keyword
            self.expect(Keyword::Only.into())?;

            return Ok((offset, limit));
        }

        Ok((offset, limit))
    }

    /// Parses a VALUES statement.
    fn parse_values(&mut self) -> Result<Statement> {
        self.expect(Keyword::Values.into())?;
        let rows = self.parse_values_rows()?;
        let order_by = self.parse_order_by_clause()?;
        let (offset, limit) = self.parse_limit_offset_clause()?;

        Ok(Statement::Dml(DmlStatement::Values(ValuesStatement {
            rows,
            order_by,
            limit,
            offset,
        })))
    }

    /// Parses VALUES rows (used by both VALUES statement and INSERT)
    fn parse_values_rows(&mut self) -> Result<Vec<Vec<Expression>>> {
        let mut rows = Vec::new();
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
            rows.push(row);
            if !self.next_is(Token::Comma) {
                break;
            }
        }
        Ok(rows)
    }
}
