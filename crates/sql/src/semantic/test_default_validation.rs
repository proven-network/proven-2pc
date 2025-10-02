#[cfg(test)]
mod tests {
    use crate::parsing::Parser;
    use crate::semantic::analyzer::SemanticAnalyzer;
    use crate::types::data_type::DataType;
    use crate::types::expression::DefaultExpression;
    use crate::types::schema::{Column, Table};
    use crate::types::value::Value;
    use std::collections::HashMap;

    fn create_test_schema() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();

        let users_table = Table {
            name: "users".to_string(),
            foreign_keys: Vec::new(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::I32,
                    nullable: false,
                    primary_key: true,
                    unique: true,
                    index: false,
                    default: None, // No default, must be specified
                    references: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: None, // No default, must be specified
                    references: None,
                },
                Column {
                    name: "status".to_string(),
                    data_type: DataType::Str,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: Some(DefaultExpression::Constant(Value::Str(
                        "active".to_string(),
                    ))), // Has default
                    references: None,
                },
                Column {
                    name: "created_at".to_string(),
                    data_type: DataType::I64,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    index: false,
                    default: Some(DefaultExpression::Constant(Value::I64(0))), // Has default (e.g., epoch time)
                    references: None,
                },
                Column {
                    name: "email".to_string(),
                    data_type: DataType::Str,
                    nullable: true, // Nullable, can be omitted
                    primary_key: false,
                    unique: true,
                    index: false,
                    default: None,
                    references: None,
                },
            ],
            primary_key: Some(0),
        };

        schemas.insert("users".to_string(), users_table);
        schemas
    }

    #[test]
    fn test_insert_with_all_columns() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Valid: All columns specified
        let sql = "INSERT INTO users (id, name, status, created_at, email) VALUES (1, 'Alice', 'active', 1234567890, 'alice@example.com')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_omit_columns_with_defaults() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Valid: Omitting status and created_at which have defaults
        let sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_omit_nullable_column() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Valid: Omitting email which is nullable
        let sql = "INSERT INTO users (id, name, status, created_at) VALUES (1, 'Alice', 'active', 1234567890)";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_missing_required_column_no_default() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Invalid: Missing 'name' which is required and has no default
        let sql = "INSERT INTO users (id, status) VALUES (1, 'active')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("'name'"));
        assert!(err.to_string().contains("required") || err.to_string().contains("no default"));
    }

    #[test]
    fn test_insert_missing_primary_key() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Invalid: Missing 'id' which is primary key
        let sql = "INSERT INTO users (name, status) VALUES ('Alice', 'active')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("'id'"));
        assert!(err.to_string().contains("required") || err.to_string().contains("no default"));
    }

    #[test]
    fn test_insert_all_defaults_and_nullable() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Valid: Only required columns without defaults
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_values_without_column_list() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Valid when all columns are provided in order
        let sql =
            "INSERT INTO users VALUES (1, 'Alice', 'active', 1234567890, 'alice@example.com')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_values_without_column_list_missing_values() {
        let schemas = create_test_schema();
        let analyzer = SemanticAnalyzer::new(schemas);

        // Invalid: Not enough values when column list is omitted
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("2 values but 5 columns"));
    }
}
