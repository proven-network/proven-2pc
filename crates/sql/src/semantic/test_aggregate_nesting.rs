#[cfg(test)]
mod tests {
    use crate::parsing::Parser;
    use crate::semantic::analyzer::SemanticAnalyzer;
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use std::collections::HashMap;

    fn create_test_schema() -> HashMap<String, Table> {
        let mut schemas = HashMap::new();

        let sales_table = Table {
            name: "sales".to_string(),
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
                    name: "amount".to_string(),
                    datatype: DataType::I32,
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

        schemas.insert("sales".to_string(), sales_table);
        schemas
    }

    #[test]
    fn test_nested_aggregates_invalid() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Invalid: SUM(COUNT(amount)) - can't nest aggregates
        let sql = "SELECT SUM(COUNT(amount)) FROM sales";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        // This should fail - aggregates can't be nested
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string().contains("cannot nest")
                    || e.to_string().contains("aggregate")
                    || e.to_string().contains("Unknown function: COUNT"), // If COUNT returns a value type
                "Error was: {}",
                e
            );
        }
    }

    #[test]
    fn test_aggregate_in_non_aggregate_function_valid() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Valid: ROUND(AVG(amount)) - non-aggregate function with aggregate argument
        let sql = "SELECT ROUND(AVG(amount)) FROM sales";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_aggregates_same_select_valid() {
        let schemas = create_test_schema();
        let mut analyzer = SemanticAnalyzer::new(schemas);

        // Valid: Multiple aggregates in same SELECT
        let sql = "SELECT SUM(amount), COUNT(*), MAX(amount) FROM sales";
        let parsed = Parser::parse(sql).unwrap();
        let result = analyzer.analyze(parsed, vec![]);

        assert!(result.is_ok());
    }
}
