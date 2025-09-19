//! Test foreign key parsing functionality

#[cfg(test)]
mod tests {
    use crate::parsing::Parser;
    use crate::parsing::ast::Statement;
    use crate::parsing::ast::ddl::{DdlStatement, ReferentialAction};

    #[test]
    fn test_parse_table_level_foreign_key() {
        let sql = "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL,
            CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE RESTRICT
        )";

        let parsed = Parser::parse(sql).unwrap();

        if let Statement::Ddl(DdlStatement::CreateTable { foreign_keys, .. }) = parsed {
            assert_eq!(foreign_keys.len(), 1);
            let fk = &foreign_keys[0];
            assert_eq!(fk.name, Some("fk_customer".to_string()));
            assert_eq!(fk.columns, vec!["customer_id"]);
            assert_eq!(fk.referenced_table, "customers");
            assert_eq!(fk.referenced_columns, vec!["id"]);
            assert_eq!(fk.on_delete, ReferentialAction::Cascade);
            assert_eq!(fk.on_update, ReferentialAction::Restrict);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_inline_foreign_key() {
        let sql = "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers (id)
        )";

        let parsed = Parser::parse(sql).unwrap();

        if let Statement::Ddl(DdlStatement::CreateTable { columns, .. }) = parsed {
            let customer_id_col = columns.iter().find(|c| c.name == "customer_id").unwrap();
            assert_eq!(customer_id_col.references, Some("customers".to_string()));
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_drop_table_cascade() {
        let sql = "DROP TABLE users CASCADE";

        let parsed = Parser::parse(sql).unwrap();

        if let Statement::Ddl(DdlStatement::DropTable { names, cascade, .. }) = parsed {
            assert_eq!(names, vec!["users"]);
            assert!(cascade);
        } else {
            panic!("Expected DROP TABLE statement");
        }
    }

    #[test]
    fn test_parse_foreign_key_no_action() {
        let sql = "CREATE TABLE test (
            id INTEGER,
            ref_id INTEGER,
            FOREIGN KEY (ref_id) REFERENCES other (id) ON DELETE NO ACTION
        )";

        let parsed = Parser::parse(sql).unwrap();

        if let Statement::Ddl(DdlStatement::CreateTable { foreign_keys, .. }) = parsed {
            assert_eq!(foreign_keys.len(), 1);
            assert_eq!(foreign_keys[0].on_delete, ReferentialAction::NoAction);
            assert_eq!(foreign_keys[0].on_update, ReferentialAction::NoAction); // default
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }

    #[test]
    fn test_parse_foreign_key_set_null() {
        let sql = "CREATE TABLE test (
            id INTEGER,
            ref_id INTEGER,
            FOREIGN KEY (ref_id) REFERENCES other (id) ON DELETE SET NULL ON UPDATE SET DEFAULT
        )";

        let parsed = Parser::parse(sql).unwrap();

        if let Statement::Ddl(DdlStatement::CreateTable { foreign_keys, .. }) = parsed {
            assert_eq!(foreign_keys.len(), 1);
            assert_eq!(foreign_keys[0].on_delete, ReferentialAction::SetNull);
            assert_eq!(foreign_keys[0].on_update, ReferentialAction::SetDefault);
        } else {
            panic!("Expected CREATE TABLE statement");
        }
    }
}
