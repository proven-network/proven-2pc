//! SQL function definitions and registry
//!
//! This module provides a trait-based architecture for SQL functions,
//! separating metadata/validation from execution.

use crate::error::{Error, Result};
use crate::types::Value;
use crate::types::context::ExecutionContext;
use crate::types::data_type::DataType;
use std::collections::HashMap;

// String functions
mod concat;
mod length;
mod lower;
mod replace;
mod substr;
mod trim;
mod upper;

// Aggregate functions
mod avg;
mod avg_distinct;
mod count;
mod count_distinct;
mod max;
mod max_distinct;
mod min;
mod min_distinct;
mod stdev;
mod stdev_distinct;
mod sum;
mod sum_distinct;
mod variance;
mod variance_distinct;

// Type functions
mod cast;
pub(crate) mod coalesce;
mod ifnull;
mod nullif;

// Math functions
mod abs;
mod ceil;
mod greatest;
mod least;
mod math;
mod round;
mod sqrt;

// System functions
mod generate_uuid;
pub(crate) mod now;

// Time/Date functions
mod current_date;
mod current_time;
mod current_timestamp;
mod extract;
mod to_date;

// Collection functions
mod append;
mod contains;
mod dedup;
mod distinct;
mod entries;
mod fields;
mod find_idx;
mod flatten;
mod from_entries;
mod is_empty;
mod keys;
mod merge;
mod prepend;
mod reverse;
mod skip;
mod slice;
mod sort;
mod splice;
mod take;
mod unwrap;
mod values;

// Geometry functions
mod calc_distance;
mod get_x;
mod get_y;

/// Metadata about a function's signature
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Function name (uppercase)
    pub name: &'static str,
    /// Whether this is an aggregate function
    pub is_aggregate: bool,
}

/// Trait for SQL functions
pub trait Function: Send + Sync {
    /// Get the function's signature
    fn signature(&self) -> &FunctionSignature;

    /// Validate arguments and return the expected return type
    fn validate(&self, arg_types: &[DataType]) -> Result<DataType>;

    /// Execute the function with runtime values
    fn execute(&self, args: &[Value], context: &ExecutionContext) -> Result<Value>;
}

use std::sync::LazyLock;

/// Registry of all available SQL functions
pub struct FunctionRegistry {
    functions: HashMap<String, Box<dyn Function>>,
}

impl FunctionRegistry {
    /// Create a new function registry with all builtin functions
    fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };

        // Register string functions
        upper::register(&mut registry);
        lower::register(&mut registry);
        length::register(&mut registry);
        concat::register(&mut registry);
        substr::register(&mut registry);
        trim::register(&mut registry);
        replace::register(&mut registry);

        // Register aggregate functions
        count::register(&mut registry);
        count_distinct::register(&mut registry);
        sum::register(&mut registry);
        sum_distinct::register(&mut registry);
        avg::register(&mut registry);
        avg_distinct::register(&mut registry);
        min::register(&mut registry);
        min_distinct::register(&mut registry);
        max::register(&mut registry);
        max_distinct::register(&mut registry);
        stdev::register(&mut registry);
        stdev_distinct::register(&mut registry);
        variance::register(&mut registry);
        variance_distinct::register(&mut registry);

        // Register type functions
        cast::register(&mut registry);
        coalesce::register(&mut registry);
        ifnull::register(&mut registry);
        nullif::register(&mut registry);

        // Register math functions
        abs::register(&mut registry);
        round::register(&mut registry);
        ceil::register(&mut registry);
        sqrt::register(&mut registry);
        greatest::register(&mut registry);
        least::register(&mut registry);
        math::register(&mut registry);

        // Register system functions
        now::register(&mut registry);
        generate_uuid::register(&mut registry);

        // Register time/date functions
        current_timestamp::register(&mut registry);
        current_date::register(&mut registry);
        current_time::register(&mut registry);
        extract::register(&mut registry);
        to_date::register(&mut registry);

        // Register collection functions
        is_empty::register(&mut registry);
        contains::register(&mut registry);
        keys::register(&mut registry);
        values::register(&mut registry);
        unwrap::register(&mut registry);
        append::register(&mut registry);
        prepend::register(&mut registry);
        sort::register(&mut registry);
        reverse::register(&mut registry);
        dedup::register(&mut registry);
        distinct::register(&mut registry);
        slice::register(&mut registry);
        splice::register(&mut registry);
        take::register(&mut registry);
        skip::register(&mut registry);
        find_idx::register(&mut registry);
        flatten::register(&mut registry);
        entries::register(&mut registry);
        fields::register(&mut registry);
        merge::register(&mut registry);
        from_entries::register(&mut registry);

        // Register geometry functions
        get_x::register(&mut registry);
        get_y::register(&mut registry);
        calc_distance::register(&mut registry);

        registry
    }

    /// Register a function
    fn register(&mut self, function: Box<dyn Function>) {
        let name = function.signature().name.to_string();
        self.functions.insert(name, function);
    }
}

// Global static registry
static REGISTRY: LazyLock<FunctionRegistry> = LazyLock::new(FunctionRegistry::new);

/// Look up a function by name
pub fn get_function(name: &str) -> Option<&'static dyn Function> {
    REGISTRY
        .functions
        .get(&name.to_uppercase())
        .map(|f| f.as_ref())
}

/// Check if a function is an aggregate
pub fn is_aggregate(name: &str) -> bool {
    get_function(name)
        .map(|f| f.signature().is_aggregate)
        .unwrap_or(false)
}

/// Validate function arguments and return the expected return type
pub fn validate_function(name: &str, arg_types: &[DataType]) -> Result<DataType> {
    if let Some(func) = get_function(name) {
        func.validate(arg_types)
    } else {
        Err(Error::ExecutionError(format!("Unknown function: {}", name)))
    }
}

/// Execute a function with runtime values
pub fn execute_function(name: &str, args: &[Value], context: &ExecutionContext) -> Result<Value> {
    if let Some(func) = get_function(name) {
        func.execute(args, context)
    } else {
        Err(Error::ExecutionError(format!("Unknown function: {}", name)))
    }
}
