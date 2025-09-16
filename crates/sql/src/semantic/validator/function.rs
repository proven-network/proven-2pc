//! Function signature validation

use crate::error::{Error, Result};
use crate::parsing::ast::Expression;
use crate::semantic::annotated_ast::AnnotatedStatement;
use crate::semantic::context::AnalysisContext;
use crate::types::data_type::DataType;

/// Function signature information
struct FunctionSignature {
    /// Function name
    name: &'static str,
    /// Minimum number of arguments
    min_args: usize,
    /// Maximum number of arguments (None for variadic)
    max_args: Option<usize>,
    /// Expected argument types (None for any type)
    arg_types: Vec<Option<DataType>>,
    /// Return type
    return_type: DataType,
    /// Whether this is an aggregate function
    is_aggregate: bool,
}

/// Validator for function calls
pub struct FunctionValidator {
    /// Known function signatures
    signatures: Vec<FunctionSignature>,
}

impl FunctionValidator {
    /// Create a new function validator
    pub fn new() -> Self {
        let signatures = vec![
            // Aggregate functions
            FunctionSignature {
                name: "COUNT",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Any type
                return_type: DataType::I64,
                is_aggregate: true,
            },
            FunctionSignature {
                name: "SUM",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Numeric types
                return_type: DataType::F64,
                is_aggregate: true,
            },
            FunctionSignature {
                name: "AVG",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Numeric types
                return_type: DataType::F64,
                is_aggregate: true,
            },
            FunctionSignature {
                name: "MIN",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Any comparable type
                return_type: DataType::Nullable(Box::new(DataType::Text)), // Same as input
                is_aggregate: true,
            },
            FunctionSignature {
                name: "MAX",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Any comparable type
                return_type: DataType::Nullable(Box::new(DataType::Text)), // Same as input
                is_aggregate: true,
            },
            // Scalar functions
            FunctionSignature {
                name: "UPPER",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![Some(DataType::Text)],
                return_type: DataType::Text,
                is_aggregate: false,
            },
            FunctionSignature {
                name: "LOWER",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![Some(DataType::Text)],
                return_type: DataType::Text,
                is_aggregate: false,
            },
            FunctionSignature {
                name: "LENGTH",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![Some(DataType::Text)],
                return_type: DataType::I32,
                is_aggregate: false,
            },
            FunctionSignature {
                name: "COALESCE",
                min_args: 1,
                max_args: None,                                            // Variadic
                arg_types: vec![],                                         // All same type
                return_type: DataType::Nullable(Box::new(DataType::Text)), // Same as inputs
                is_aggregate: false,
            },
            FunctionSignature {
                name: "CAST",
                min_args: 1,
                max_args: Some(1),
                arg_types: vec![None], // Any type
                return_type: DataType::Nullable(Box::new(DataType::Text)), // Depends on cast target
                is_aggregate: false,
            },
        ];

        Self { signatures }
    }

    /// Validate all function calls in a statement
    pub fn validate_statement(
        &self,
        statement: &AnnotatedStatement,
        context: &mut AnalysisContext,
    ) -> Result<()> {
        // TODO: Walk the statement tree and validate all function calls
        Ok(())
    }

    /// Validate a function call
    pub fn validate_function(
        &self,
        name: &str,
        args: &[Expression],
        context: &mut AnalysisContext,
    ) -> Result<DataType> {
        let upper_name = name.to_uppercase();

        // Find matching signature
        let signature = self
            .signatures
            .iter()
            .find(|sig| sig.name == upper_name.as_str());

        let signature = match signature {
            Some(sig) => sig,
            None => {
                // Unknown function - we'll allow it but can't validate
                return Ok(DataType::Nullable(Box::new(DataType::Text)));
            }
        };

        // Check argument count
        if args.len() < signature.min_args {
            return Err(Error::ExecutionError(format!(
                "Function {} requires at least {} arguments, got {}",
                name,
                signature.min_args,
                args.len()
            )));
        }

        if let Some(max) = signature.max_args
            && args.len() > max
        {
            return Err(Error::ExecutionError(format!(
                "Function {} accepts at most {} arguments, got {}",
                name,
                max,
                args.len()
            )));
        }

        // Check argument types
        for (i, arg) in args.iter().enumerate() {
            if i < signature.arg_types.len()
                && let Some(expected_type) = &signature.arg_types[i]
            {
                // TODO: Type check the argument expression
                // This requires the type checker
            }
        }

        // Track if this is an aggregate function
        if signature.is_aggregate {
            context.set_deterministic(false);
        }

        Ok(signature.return_type.clone())
    }

    /// Check if a function is an aggregate
    pub fn is_aggregate(&self, name: &str) -> bool {
        let upper_name = name.to_uppercase();
        self.signatures
            .iter()
            .find(|sig| sig.name == upper_name.as_str())
            .map(|sig| sig.is_aggregate)
            .unwrap_or(false)
    }

    /// Get the return type of a function
    pub fn return_type(&self, name: &str, arg_types: &[DataType]) -> DataType {
        let upper_name = name.to_uppercase();
        self.signatures
            .iter()
            .find(|sig| sig.name == upper_name.as_str())
            .map(|sig| sig.return_type.clone())
            .unwrap_or(DataType::Nullable(Box::new(DataType::Text)))
    }
}
