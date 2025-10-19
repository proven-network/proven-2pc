#[cfg(test)]
mod tests {
    use crate::parsing::Parser;
    use crate::semantic::analyzer::SemanticAnalyzer;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use std::collections::HashMap;

    fn create_test_schema() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();

        let users_table = Table {
            name: "users".to_string(),
            schema_version: 1,
            foreign_keys: Vec::new(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: None,
                    references: None,
                },
                Column {
                    name: "email".to_string(),
                    data_type: DataType::Str,
                    nullable: true,
                    primary_key: false,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
            ],
            primary_key: Some(0), // id column is at index 0
        };

        schemas.insert("users".to_string(), users_table);
        schemas
    }

    #[test]
    fn test_insert_column_count_mismatch() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // Too many values
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice', 'extra')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("3 values but 2 columns"));
    }

    #[test]
    fn test_insert_column_count_too_few() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // Too few values
        let sql = "INSERT INTO users VALUES (1, 'Alice')"; // Missing email
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("2 values but 3 columns"));
    }

    #[test]
    fn test_insert_column_count_valid() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // Correct number of values
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_all_columns_valid() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // All columns with VALUES
        let sql = "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_type_mismatch() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // String value for integer column
        let sql = "INSERT INTO users (id, name) VALUES ('not_an_int', 'Alice')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        // This should fail due to type mismatch
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Type") || err.to_string().contains("type"));
    }

    #[test]
    fn test_update_null_constraint() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas, HashMap::new());

        // Try to set non-nullable column to NULL
        let sql = "UPDATE users SET name = NULL";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("non-nullable") || err.to_string().contains("NULL"));
    }
}
