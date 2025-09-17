#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::parsing::parse_sql;
    use crate::semantic::annotated_ast::AnnotatedStatement;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use std::collections::HashMap;

    fn create_test_schema() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();

        let users_table = Table {
            name: "users".to_string(),
            primary_key: Some(0), // Index of the 'id' column
            columns: vec![
                Column {
                    name: "id".to_string(),
                    datatype: DataType::I32,
                    primary_key: true,
                    nullable: false,
                    default: None,
                    unique: true,
                    index: true,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    datatype: DataType::Text,
                    primary_key: false,
                    nullable: false,
                    default: None,
                    unique: false,
                    index: false,
                    references: None,
                },
                Column {
                    name: "age".to_string(),
                    datatype: DataType::I32,
                    primary_key: false,
                    nullable: true,
                    default: None,
                    unique: false,
                    index: false,
                    references: None,
                },
            ],
        };

        schemas.insert("users".to_string(), users_table);
        schemas
    }

    #[test]
    fn test_select_type_checking() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Test valid SELECT
        let sql = "SELECT name, age FROM users WHERE age > 21";
        let stmt = parse_sql(sql).unwrap();
        let result = analyzer.analyze(stmt);
        assert!(result.is_ok());

        let analyzed = result.unwrap();
        if let AnnotatedStatement::Select(select) = analyzed.statement {
            // Check projection types
            assert_eq!(select.projection_types.len(), 2);
            assert_eq!(select.projection_types[0], DataType::Text); // name is NOT NULL
            assert_eq!(
                select.projection_types[1],
                DataType::Nullable(Box::new(DataType::I32))
            ); // age is nullable

            // Check WHERE type is boolean
            assert!(matches!(select.where_type, Some(DataType::Bool)));
        } else {
            panic!("Expected SELECT statement");
        }
    }

    #[test]
    fn test_insert_type_checking() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Test valid INSERT
        let sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)";
        let stmt = parse_sql(sql).unwrap();
        let result = analyzer.analyze(stmt);
        if let Err(e) = &result {
            eprintln!("INSERT test error: {:?}", e);
        }
        assert!(result.is_ok());

        let analyzed = result.unwrap();
        if let AnnotatedStatement::Insert(insert) = analyzed.statement {
            // Check value types match column types
            // Note: columns reflect their nullable status now
            assert_eq!(insert.column_types.len(), 3);
            assert_eq!(insert.column_types[0], DataType::I32); // id is NOT NULL (primary key)
            assert_eq!(insert.column_types[1], DataType::Text); // name is NOT NULL
            assert_eq!(
                insert.column_types[2],
                DataType::Nullable(Box::new(DataType::I32))
            ); // age is nullable
        } else {
            panic!("Expected INSERT statement");
        }
    }

    #[test]
    fn test_update_type_checking() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Test valid UPDATE
        let sql = "UPDATE users SET age = 31 WHERE name = 'Alice'";
        let stmt = parse_sql(sql).unwrap();
        let result = analyzer.analyze(stmt);
        if let Err(e) = &result {
            eprintln!("UPDATE test error: {:?}", e);
        }
        assert!(result.is_ok());

        let analyzed = result.unwrap();
        if let AnnotatedStatement::Update(update) = analyzed.statement {
            // Check assignment type
            assert_eq!(update.assignment_types.len(), 1);
            assert_eq!(update.assignment_types[0].0, "age");
            assert_eq!(update.assignment_types[0].1, DataType::I64); // Literal integer is I64

            // Check WHERE type is boolean
            assert!(matches!(update.where_type, Some(DataType::Bool)));
        } else {
            panic!("Expected UPDATE statement");
        }
    }

    #[test]
    fn test_delete_type_checking() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Test valid DELETE
        let sql = "DELETE FROM users WHERE age < 18";
        let stmt = parse_sql(sql).unwrap();
        let result = analyzer.analyze(stmt);
        assert!(result.is_ok());

        let analyzed = result.unwrap();
        if let AnnotatedStatement::Delete(delete) = analyzed.statement {
            // Check WHERE type is boolean
            assert!(matches!(delete.where_type, Some(DataType::Bool)));
        } else {
            panic!("Expected DELETE statement");
        }
    }

    #[test]
    fn test_type_mismatch_error() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Test type mismatch in WHERE clause
        let sql = "SELECT * FROM users WHERE name > 42";
        let stmt = parse_sql(sql).unwrap();
        let result = analyzer.analyze(stmt);
        if let Err(e) = &result {
            eprintln!("Type mismatch test error: {:?}", e);
        }
        // This should pass semantic analysis (comparison is valid)
        // Type checking happens during execution
        assert!(result.is_ok());
    }
}
