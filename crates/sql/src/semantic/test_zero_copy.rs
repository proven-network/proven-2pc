//! Tests for zero-copy parameter architecture

#[cfg(test)]
mod tests {
    use crate::parsing::parse_sql;
    use crate::semantic::statement::SqlContext;
    use crate::semantic::{analyzer::SemanticAnalyzer, bind_parameters};
    use crate::types::data_type::DataType;
    use crate::types::schema::{Column, Table};
    use crate::types::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_parameter_analysis() {
        // Parse a query with parameters
        let sql = "SELECT * FROM users WHERE id = ? AND name = ?";
        let ast = parse_sql(sql).unwrap();

        // Create a test schema with users table
        let mut schemas = HashMap::new();
        let mut id_col = Column::new("id".to_string(), DataType::I64);
        id_col.nullable = false;
        let mut name_col = Column::new("name".to_string(), DataType::Text);
        name_col.nullable = true;
        let mut email_col = Column::new("email".to_string(), DataType::Text);
        email_col.nullable = true;

        let users_table =
            Table::new("users".to_string(), vec![id_col, name_col, email_col]).unwrap();
        schemas.insert("users".to_string(), users_table);

        // Analyze the query
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Verify parameters were detected
        assert_eq!(analyzed.parameter_count(), 2);
        assert!(analyzed.has_parameters());

        // Check parameter slots
        assert_eq!(analyzed.parameter_slots.len(), 2);
        assert_eq!(analyzed.parameter_slots[0].index, 0);
        assert_eq!(analyzed.parameter_slots[1].index, 1);
    }

    #[test]
    fn test_parameter_binding() {
        // Parse a query with parameters
        let sql = "SELECT * FROM users WHERE id = ? AND active = ?";
        let ast = parse_sql(sql).unwrap();

        // Create a test schema
        let mut schemas = HashMap::new();
        let mut id_col = Column::new("id".to_string(), DataType::I64);
        id_col.nullable = false;
        let mut active_col = Column::new("active".to_string(), DataType::Bool);
        active_col.nullable = false;

        let users_table = Table::new("users".to_string(), vec![id_col, active_col]).unwrap();
        schemas.insert("users".to_string(), users_table);

        // Analyze the query
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Bind parameters
        let values = vec![Value::integer(42), Value::boolean(true)];
        let bound = bind_parameters(&analyzed, values).unwrap();

        // Verify binding
        assert_eq!(bound.values.len(), 2);
        assert!(bound.validated);
        assert_eq!(bound.get(0), Some(&Value::integer(42)));
        assert_eq!(bound.get(1), Some(&Value::boolean(true)));
    }

    #[test]
    fn test_arc_ast_sharing() {
        // Parse a query
        let sql = "SELECT name FROM users WHERE id = ?";
        let ast = parse_sql(sql).unwrap();

        // Create a test schema
        let mut schemas = HashMap::new();
        let mut id_col = Column::new("id".to_string(), DataType::I64);
        id_col.nullable = false;
        let mut name_col = Column::new("name".to_string(), DataType::Text);
        name_col.nullable = true;

        let users_table = Table::new("users".to_string(), vec![id_col, name_col]).unwrap();
        schemas.insert("users".to_string(), users_table);

        // Analyze the query
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Clone the analyzed statement (should share Arc)
        let analyzed_clone = analyzed.clone();

        // Verify Arc is shared
        assert!(std::sync::Arc::ptr_eq(&analyzed.ast, &analyzed_clone.ast));
    }

    #[test]
    fn test_parameter_validation_error() {
        // Parse a SELECT query with parameters (doesn't require table to exist)
        let sql = "SELECT ? AS col1, ? AS col2";
        let ast = parse_sql(sql).unwrap();

        // Analyze the query
        let schemas = HashMap::new();
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Try to bind with wrong number of parameters
        let values = vec![Value::integer(42)]; // Only one value for two parameters
        let result = bind_parameters(&analyzed, values);

        // Should fail validation
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("Parameter count mismatch"));
        }
    }

    #[test]
    fn test_function_parameter_context() {
        // Parse a query with parameters in functions
        let sql = "SELECT LENGTH(?) AS len, ABS(?) AS absolute";
        let ast = parse_sql(sql).unwrap();

        // Create an empty schema (no tables needed for this test)
        let schemas = HashMap::new();
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Check that parameters have correct context
        assert_eq!(analyzed.parameter_slots.len(), 2);

        // First parameter should be in LENGTH function context
        let first = &analyzed.parameter_slots[0];
        assert_eq!(first.index, 0);
        if let SqlContext::FunctionArgument {
            ref function_name,
            arg_index,
        } = first.coercion_context.sql_context
        {
            assert_eq!(function_name, "length");
            assert_eq!(arg_index, 0);
        } else {
            panic!("First parameter not in function context");
        }

        // Second parameter should be in ABS function context
        let second = &analyzed.parameter_slots[1];
        assert_eq!(second.index, 1);
        if let SqlContext::FunctionArgument {
            ref function_name,
            arg_index,
        } = second.coercion_context.sql_context
        {
            assert_eq!(function_name, "abs");
            assert_eq!(arg_index, 0);
        } else {
            panic!("Second parameter not in function context");
        }
    }

    #[test]
    fn test_function_validation_errors() {
        // Test that semantic analysis fails for invalid function arguments
        let sql = "SELECT ABS('not a number')";
        let ast = parse_sql(sql).unwrap();

        let schemas = HashMap::new();
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let result = analyzer.analyze(ast);

        // Should fail because ABS doesn't accept strings
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("numeric") || error_msg.contains("ABS"));
        }
    }

    #[test]
    fn test_function_arg_count_validation() {
        // Test min args validation
        let sql = "SELECT ABS()";
        let ast = parse_sql(sql).unwrap();

        let schemas = HashMap::new();
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let result = analyzer.analyze(ast);

        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("requires at least 1 argument"));
        }

        // Test max args validation
        let sql2 = "SELECT ABS(1, 2)";
        let ast2 = parse_sql(sql2).unwrap();

        let mut analyzer2 = SemanticAnalyzer::new(HashMap::new());
        let result2 = analyzer2.analyze(ast2);

        assert!(result2.is_err());
        if let Err(e) = result2 {
            let error_msg = format!("{}", e);
            assert!(error_msg.contains("accepts at most 1 argument"));
        }
    }

    #[test]
    fn test_function_with_parameter_validation() {
        // Test that parameters in functions work
        let sql = "SELECT ABS(?)";
        let ast = parse_sql(sql).unwrap();

        let schemas = HashMap::new();
        let mut analyzer = SemanticAnalyzer::new(schemas);
        let analyzed = analyzer.analyze(ast).unwrap();

        // Should have one parameter
        assert_eq!(analyzed.parameter_count(), 1);

        // Parameter should be in function context
        let param = &analyzed.parameter_slots[0];
        if let SqlContext::FunctionArgument {
            ref function_name,
            arg_index,
        } = param.coercion_context.sql_context
        {
            assert_eq!(function_name, "abs");
            assert_eq!(arg_index, 0);
        } else {
            panic!("Parameter not in function context");
        }
    }
}
