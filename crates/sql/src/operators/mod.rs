//! SQL operator implementations
//!
//! This module provides a unified implementation for all SQL operators,
//! ensuring consistency between type checking and execution.

use crate::parsing::ast::Operator;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

pub mod helpers;
pub mod traits;

// Arithmetic operators
mod add;
mod divide;
mod exponentiate;
mod multiply;
mod remainder;
mod subtract;

// Comparison operators
mod equal;
mod greater_than;
mod greater_than_equal;
mod less_than;
mod less_than_equal;
mod not_equal;

// Logical operators
mod and;
mod not;
mod or;

// Unary operators
mod factorial;
mod identity;
mod negate;

// Pattern matching operators
mod like;

// Re-export commonly used items
pub use traits::{BinaryOperator, SpecialOperator, UnaryOperator};

/// Registry for all SQL operators
struct OperatorRegistry {
    binary_ops: HashMap<String, Arc<dyn BinaryOperator>>,
    unary_ops: HashMap<String, Arc<dyn UnaryOperator>>,
    special_ops: HashMap<String, Arc<dyn SpecialOperator>>,
}

impl OperatorRegistry {
    /// Create a new operator registry with all operators registered
    fn new() -> Self {
        let mut registry = Self {
            binary_ops: HashMap::new(),
            unary_ops: HashMap::new(),
            special_ops: HashMap::new(),
        };

        // Arithmetic operators
        registry.register_binary("add", Arc::new(add::AddOperator));
        registry.register_binary("subtract", Arc::new(subtract::SubtractOperator));
        registry.register_binary("multiply", Arc::new(multiply::MultiplyOperator));
        registry.register_binary("divide", Arc::new(divide::DivideOperator));
        registry.register_binary("remainder", Arc::new(remainder::RemainderOperator));
        registry.register_binary("exponentiate", Arc::new(exponentiate::ExponentiateOperator));

        // Comparison operators
        registry.register_binary("equal", Arc::new(equal::EqualOperator));
        registry.register_binary("not_equal", Arc::new(not_equal::NotEqualOperator));
        registry.register_binary("less_than", Arc::new(less_than::LessThanOperator));
        registry.register_binary(
            "less_than_equal",
            Arc::new(less_than_equal::LessThanEqualOperator),
        );
        registry.register_binary("greater_than", Arc::new(greater_than::GreaterThanOperator));
        registry.register_binary(
            "greater_than_equal",
            Arc::new(greater_than_equal::GreaterThanEqualOperator),
        );

        // Logical operators
        registry.register_binary("and", Arc::new(and::AndOperator));
        registry.register_binary("or", Arc::new(or::OrOperator));
        registry.register_unary("not", Arc::new(not::NotOperator));

        // Unary arithmetic operators
        registry.register_unary("negate", Arc::new(negate::NegateOperator));
        registry.register_unary("identity", Arc::new(identity::IdentityOperator));
        registry.register_unary("factorial", Arc::new(factorial::FactorialOperator));

        // Pattern matching operators
        registry.register_binary("like", Arc::new(like::LikeOperator));

        registry
    }

    /// Register a binary operator
    fn register_binary(&mut self, name: &str, op: Arc<dyn BinaryOperator>) {
        self.binary_ops.insert(name.to_string(), op);
    }

    /// Register a unary operator
    fn register_unary(&mut self, name: &str, op: Arc<dyn UnaryOperator>) {
        self.unary_ops.insert(name.to_string(), op);
    }

    /// Register a special operator
    #[allow(dead_code)]
    fn register_special(&mut self, name: &str, op: Arc<dyn SpecialOperator>) {
        self.special_ops.insert(name.to_string(), op);
    }
}

// Global static registry
static REGISTRY: LazyLock<OperatorRegistry> = LazyLock::new(OperatorRegistry::new);

/// Map AST operator to binary operator name
fn operator_to_binary_name(op: &Operator) -> Option<&'static str> {
    use Operator::*;
    Some(match op {
        Add(..) => "add",
        Subtract(..) => "subtract",
        Multiply(..) => "multiply",
        Divide(..) => "divide",
        Remainder(..) => "remainder",
        Exponentiate(..) => "exponentiate",
        Equal(..) => "equal",
        NotEqual(..) => "not_equal",
        LessThan(..) => "less_than",
        LessThanOrEqual(..) => "less_than_equal",
        GreaterThan(..) => "greater_than",
        GreaterThanOrEqual(..) => "greater_than_equal",
        And(..) => "and",
        Or(..) => "or",
        Like(..) => "like",
        _ => return None,
    })
}

/// Map AST operator to unary operator name
fn operator_to_unary_name(op: &Operator) -> Option<&'static str> {
    use Operator::*;
    Some(match op {
        Not(..) => "not",
        Negate(..) => "negate",
        Identity(..) => "identity",
        Factorial(..) => "factorial",
        _ => return None,
    })
}

/// Map AST operator to special operator name
fn operator_to_special_name(op: &Operator) -> Option<&'static str> {
    use Operator::*;
    Some(match op {
        Is(..) => "is",
        Between { .. } => "between",
        InList { .. } => "in",
        _ => return None,
    })
}

/// Get a binary operator from the global registry
pub fn get_binary_operator(op: &Operator) -> Option<&'static dyn BinaryOperator> {
    let name = operator_to_binary_name(op)?;
    REGISTRY.binary_ops.get(name).map(|arc| arc.as_ref())
}

/// Get a unary operator from the global registry
pub fn get_unary_operator(op: &Operator) -> Option<&'static dyn UnaryOperator> {
    let name = operator_to_unary_name(op)?;
    REGISTRY.unary_ops.get(name).map(|arc| arc.as_ref())
}

/// Get a special operator from the global registry
#[allow(dead_code)]
pub fn get_special_operator(op: &Operator) -> Option<&'static dyn SpecialOperator> {
    let name = operator_to_special_name(op)?;
    REGISTRY.special_ops.get(name).map(|arc| arc.as_ref())
}
