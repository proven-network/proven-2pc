#[cfg(test)]
mod tests {
    use crate::parsing::Parser;
    use crate::semantic::analyzer::SemanticAnalyzer;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use std::collections::HashMap;

    fn create_test_schemas() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();

        // Parent table: departments
        let departments_table = Table {
            name: "departments".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    datatype: DataType::Str,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: None,
                    references: None,
                },
            ],
            primary_key: Some(0),
        };

        // Child table: employees with foreign key
        let employees_table = Table {
            name: "employees".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    datatype: DataType::Str,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "dept_id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: true, // Foreign keys need index
                    default: None,
                    references: Some("departments".to_string()), // Foreign key
                },
                Column {
                    name: "manager_id".to_string(),
                    datatype: DataType::I32,
                    nullable: true, // Nullable foreign key
                    primary_key: false,
                    unique: false,
                    index: true,
                    default: None,
                    references: Some("employees".to_string()), // Self-referential foreign key
                },
            ],
            primary_key: Some(0),
        };

        // Table with wrong type foreign key for testing
        let invalid_fk_table = Table {
            name: "invalid_fk".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "dept_name".to_string(),
                    datatype: DataType::Str, // Wrong type - departments.id is I32
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: true,
                    default: None,
                    references: Some("departments".to_string()),
                },
            ],
            primary_key: Some(0),
        };

        // Table referencing non-existent table
        let bad_ref_table = Table {
            name: "bad_ref".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "fake_id".to_string(),
                    datatype: DataType::I32,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: true,
                    default: None,
                    references: Some("nonexistent".to_string()), // Non-existent table
                },
            ],
            primary_key: Some(0),
        };

        schemas.insert("departments".to_string(), departments_table);
        schemas.insert("employees".to_string(), employees_table);
        schemas.insert("invalid_fk".to_string(), invalid_fk_table);
        schemas.insert("bad_ref".to_string(), bad_ref_table);
        schemas
    }

    #[test]
    fn test_valid_foreign_key_insert() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        let sql =
            "INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 10, 5)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_nullable_foreign_key_null() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // manager_id is nullable, so NULL is allowed
        let sql =
            "INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 10, NULL)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        if let Err(e) = &result {
            eprintln!("Unexpected error for nullable foreign key NULL: {}", e);
        }
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_non_nullable_foreign_key_null() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // dept_id is non-nullable foreign key, NULL not allowed
        let sql =
            "INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', NULL, 5)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        // Just check for NULL in error message - it may not specifically mention foreign key
        assert!(err.to_string().contains("NULL") || err.to_string().contains("nullable"));
    }

    #[test]
    fn test_foreign_key_type_mismatch() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Trying to insert string into dept_name which should reference departments.id (I32)
        let sql = "INSERT INTO invalid_fk (id, dept_name) VALUES (1, 'Engineering')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        // Should fail due to type incompatibility
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("incompatible") || err.to_string().contains("type"),
            "Error was: {}",
            err
        );
    }

    #[test]
    fn test_foreign_key_nonexistent_table() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        let sql = "INSERT INTO bad_ref (id, fake_id) VALUES (1, 10)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("non-existent") || err.to_string().contains("nonexistent")
        );
    }

    #[test]
    fn test_update_foreign_key_to_null() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Cannot update non-nullable foreign key to NULL
        let sql = "UPDATE employees SET dept_id = NULL WHERE id = 1";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("NULL"));
    }

    #[test]
    fn test_update_nullable_foreign_key_to_null() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Can update nullable foreign key to NULL
        let sql = "UPDATE employees SET manager_id = NULL WHERE id = 1";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_self_referential_foreign_key() {
        let schemas = create_test_schemas();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Self-referential foreign key (employees.manager_id -> employees.id)
        let sql = "INSERT INTO employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 10, 1)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }
}
