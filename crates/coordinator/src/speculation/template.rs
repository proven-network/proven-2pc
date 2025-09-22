//! Template extraction and instantiation for pattern learning

use crate::speculation::args::{ArgumentFlattener, FlattenedArgs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// A template with JSONPath placeholders
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Template {
    /// The template pattern with placeholders like "{{$[0].user}}"
    pub pattern: Value,

    /// Which JSONPaths from args are referenced
    pub required_paths: HashSet<String>,

    /// Whether this template represents a write operation
    pub is_write: bool,

    /// The stream this operation targets
    pub stream: String,

    /// Maps placeholder paths to their original types (for type preservation)
    /// e.g., "$[0].amount" -> "string" means the operation had a string
    #[serde(default)]
    pub original_types: HashMap<String, String>,
}

/// Extracts templates from operations by finding correlations with arguments
#[derive(Debug, Clone)]
pub struct TemplateExtractor {
    flattener: ArgumentFlattener,
    /// Placeholder prefix and suffix
    placeholder_prefix: String,
    placeholder_suffix: String,
}

impl Default for TemplateExtractor {
    fn default() -> Self {
        Self {
            flattener: ArgumentFlattener::new(),
            placeholder_prefix: "{{".to_string(),
            placeholder_suffix: "}}".to_string(),
        }
    }
}

impl TemplateExtractor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Extract a template from an operation by finding values that appear in args
    pub fn extract(
        &self,
        stream: &str,
        operation: &Value,
        args: &[Value],
        is_write: bool,
    ) -> Option<Template> {
        // Flatten args to get all available values
        let flattened = self.flattener.flatten(args);

        // Clone operation and replace values found in args with placeholders
        let mut pattern = operation.clone();
        let mut required_paths = HashSet::new();
        let mut original_types = HashMap::new();

        self.replace_with_placeholders(
            &mut pattern,
            &flattened,
            &mut required_paths,
            &mut original_types,
        );

        // Only create template if we found at least one correlation
        if required_paths.is_empty() {
            return None;
        }

        Some(Template {
            pattern,
            required_paths,
            is_write,
            stream: stream.to_string(),
            original_types,
        })
    }

    /// Recursively replace values in the pattern that match flattened args
    fn replace_with_placeholders(
        &self,
        value: &mut Value,
        flattened: &FlattenedArgs,
        required_paths: &mut HashSet<String>,
        original_types: &mut HashMap<String, String>,
    ) {
        match value {
            Value::String(s) => {
                // First check for exact string match
                if let Some(path) = self
                    .flattener
                    .find_value(flattened, &Value::String(s.clone()))
                {
                    // Replace with placeholder
                    *s = format!(
                        "{}{}{}",
                        self.placeholder_prefix, path, self.placeholder_suffix
                    );
                    required_paths.insert(path.clone());
                    original_types.insert(path, "string".to_string());
                    return;
                }

                // If the string looks like a number, also check for numeric match
                // This handles cases where operation has "10" but args has 10
                if let Ok(n_val) = serde_json::from_str::<serde_json::Value>(s) {
                    if n_val.is_number() {
                        if let Some(path) = self.flattener.find_value(flattened, &n_val) {
                            // Replace with placeholder - track that original was string!
                            *s = format!(
                                "{}{}{}",
                                self.placeholder_prefix, path, self.placeholder_suffix
                            );
                            required_paths.insert(path.clone());
                            original_types.insert(path, "string".to_string());
                            return;
                        }
                    }
                }

                // If no exact match, try substring detection
                let original = s.clone();
                let mut modified = original.clone();
                let mut found_any = false;

                // Try to find arg values as substrings
                for (path, arg_val) in flattened {
                    if let Value::String(arg_str) = arg_val {
                        if !arg_str.is_empty() && modified.contains(arg_str.as_str()) {
                            // Replace all occurrences of this arg value with placeholder
                            let placeholder = format!(
                                "{}{}{}",
                                self.placeholder_prefix, path, self.placeholder_suffix
                            );
                            modified = modified.replace(arg_str.as_str(), &placeholder);
                            required_paths.insert(path.clone());
                            found_any = true;
                        }
                    } else if let Value::Number(n) = arg_val {
                        let num_str = n.to_string();
                        if modified.contains(&num_str) {
                            let placeholder = format!(
                                "{}{}{}",
                                self.placeholder_prefix, path, self.placeholder_suffix
                            );
                            modified = modified.replace(&num_str, &placeholder);
                            required_paths.insert(path.clone());
                            found_any = true;
                        }
                    }
                }

                if found_any {
                    *s = modified;
                }
            }
            Value::Number(n) => {
                // Check if this number exists in flattened args
                if let Some(path) = self
                    .flattener
                    .find_value(flattened, &Value::Number(n.clone()))
                {
                    // Replace with placeholder (as string) - track original was number
                    *value = Value::String(format!(
                        "{}{}{}",
                        self.placeholder_prefix, path, self.placeholder_suffix
                    ));
                    required_paths.insert(path.clone());
                    original_types.insert(path, "number".to_string());
                }
            }
            Value::Bool(b) => {
                // Check if this boolean exists in flattened args
                if let Some(path) = self.flattener.find_value(flattened, &Value::Bool(*b)) {
                    *value = Value::String(format!(
                        "{}{}{}",
                        self.placeholder_prefix, path, self.placeholder_suffix
                    ));
                    required_paths.insert(path.clone());
                    original_types.insert(path, "bool".to_string());
                }
            }
            Value::Object(map) => {
                // Recursively process all values in the object
                for (_, v) in map.iter_mut() {
                    self.replace_with_placeholders(v, flattened, required_paths, original_types);
                }
            }
            Value::Array(arr) => {
                // Recursively process all elements in the array
                for v in arr.iter_mut() {
                    self.replace_with_placeholders(v, flattened, required_paths, original_types);
                }
            }
            Value::Null => {
                // Null values are left as-is
            }
        }
    }

    /// Check if two templates are structurally similar
    pub fn templates_similar(&self, t1: &Template, t2: &Template) -> bool {
        if t1.stream != t2.stream || t1.is_write != t2.is_write {
            return false;
        }

        // Templates must use the same required paths to be considered similar
        if t1.required_paths != t2.required_paths {
            return false;
        }

        self.values_structurally_equal(&t1.pattern, &t2.pattern)
    }

    fn values_structurally_equal(&self, v1: &Value, v2: &Value) -> bool {
        match (v1, v2) {
            (Value::String(s1), Value::String(s2)) => {
                // Both placeholders or both not
                let is_placeholder1 = s1.starts_with(&self.placeholder_prefix);
                let is_placeholder2 = s2.starts_with(&self.placeholder_prefix);
                is_placeholder1 == is_placeholder2
            }
            (Value::Object(m1), Value::Object(m2)) => {
                m1.len() == m2.len()
                    && m1.keys().all(|k| m2.contains_key(k))
                    && m1.iter().all(|(k, v1)| {
                        m2.get(k)
                            .map_or(false, |v2| self.values_structurally_equal(v1, v2))
                    })
            }
            (Value::Array(a1), Value::Array(a2)) => {
                a1.len() == a2.len()
                    && a1
                        .iter()
                        .zip(a2.iter())
                        .all(|(v1, v2)| self.values_structurally_equal(v1, v2))
            }
            _ => v1 == v2,
        }
    }
}

/// Instantiates templates by replacing placeholders with actual values
#[derive(Debug, Clone)]
pub struct TemplateInstantiator {
    flattener: ArgumentFlattener,
    placeholder_prefix: String,
    placeholder_suffix: String,
}

impl Default for TemplateInstantiator {
    fn default() -> Self {
        Self {
            flattener: ArgumentFlattener::new(),
            placeholder_prefix: "{{".to_string(),
            placeholder_suffix: "}}".to_string(),
        }
    }
}

impl TemplateInstantiator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Instantiate a template with actual argument values
    pub fn instantiate(&self, template: &Template, args: &[Value]) -> Result<Value, String> {
        // Flatten args
        let flattened = self.flattener.flatten(args);

        // Check that all required paths exist
        for path in &template.required_paths {
            if !flattened.contains_key(path) {
                return Err(format!("Missing required argument path: {}", path));
            }
        }

        // Clone pattern and replace placeholders
        let mut result = template.pattern.clone();
        self.replace_placeholders(&mut result, &flattened, &template.original_types)?;

        Ok(result)
    }

    /// Recursively replace placeholders with actual values
    fn replace_placeholders(
        &self,
        value: &mut Value,
        flattened: &FlattenedArgs,
        original_types: &HashMap<String, String>,
    ) -> Result<(), String> {
        match value {
            Value::String(s) => {
                // Check if this string contains any placeholders
                let mut result = s.clone();
                let mut found_any = false;

                // Find all placeholders in the string
                let mut current = result.clone();
                let mut is_entire_string_placeholder = false;

                // Check if the entire string is just a single placeholder
                if result.starts_with(&self.placeholder_prefix)
                    && result.ends_with(&self.placeholder_suffix)
                {
                    let middle = &result[self.placeholder_prefix.len()
                        ..result.len() - self.placeholder_suffix.len()];
                    if !middle.contains(&self.placeholder_prefix) {
                        is_entire_string_placeholder = true;
                    }
                }

                while let Some(start_idx) = current.find(&self.placeholder_prefix) {
                    let after_prefix = &current[start_idx + self.placeholder_prefix.len()..];
                    if let Some(end_idx) = after_prefix.find(&self.placeholder_suffix) {
                        // Extract the path from the placeholder
                        let path = &after_prefix[..end_idx];

                        // Get the value from flattened args
                        if let Some(arg_value) = flattened.get(path) {
                            if is_entire_string_placeholder {
                                // Check what the original type was
                                if let Some(orig_type) = original_types.get(path) {
                                    match orig_type.as_str() {
                                        "string" => {
                                            // Original was string, keep as string
                                            let string_value = match arg_value {
                                                Value::String(s) => s.clone(),
                                                Value::Number(n) => n.to_string(),
                                                Value::Bool(b) => b.to_string(),
                                                Value::Null => "null".to_string(),
                                                _ => arg_value.to_string(),
                                            };
                                            *s = string_value;
                                        }
                                        "number" | "bool" => {
                                            // Original was number/bool, use arg value directly
                                            *value = arg_value.clone();
                                        }
                                        _ => {
                                            // Unknown type, use arg value
                                            *value = arg_value.clone();
                                        }
                                    }
                                } else {
                                    // No type info, use arg value
                                    *value = arg_value.clone();
                                }
                                return Ok(());
                            } else {
                                // Otherwise, do substring replacement (always as string)
                                let string_value = match arg_value {
                                    Value::String(s) => s.clone(),
                                    Value::Number(n) => n.to_string(),
                                    Value::Bool(b) => b.to_string(),
                                    Value::Null => "null".to_string(),
                                    _ => arg_value.to_string(),
                                };

                                let placeholder = format!(
                                    "{}{}{}",
                                    self.placeholder_prefix, path, self.placeholder_suffix
                                );
                                result = result.replace(&placeholder, &string_value);
                                found_any = true;
                            }
                        } else {
                            return Err(format!("Path not found in args: {}", path));
                        }

                        // Move past this placeholder for the next iteration
                        current = result.clone();
                    } else {
                        break;
                    }
                }

                if found_any {
                    *s = result;
                }
            }
            Value::Object(map) => {
                // Recursively process all values
                for (_, v) in map.iter_mut() {
                    self.replace_placeholders(v, flattened, original_types)?;
                }
            }
            Value::Array(arr) => {
                // Recursively process all elements
                for v in arr.iter_mut() {
                    self.replace_placeholders(v, flattened, original_types)?;
                }
            }
            _ => {
                // Other types are left as-is
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_template_extraction() {
        let extractor = TemplateExtractor::new();

        let args = vec![json!({
            "user_id": "alice",
            "amount": 100
        })];

        let operation = json!({
            "Transfer": {
                "from": "alice",
                "to": "merchant",
                "amount": 100
            }
        });

        let template = extractor
            .extract("payment_stream", &operation, &args, true)
            .unwrap();

        // Check that placeholders were created
        let pattern_str = template.pattern.to_string();
        assert!(pattern_str.contains("{{$[0].user_id}}"));
        assert!(pattern_str.contains("{{$[0].amount}}"));

        // Check required paths
        assert!(template.required_paths.contains("$[0].user_id"));
        assert!(template.required_paths.contains("$[0].amount"));
    }

    #[test]
    fn test_template_instantiation() {
        let extractor = TemplateExtractor::new();
        let instantiator = TemplateInstantiator::new();

        // Create a template from first transaction
        let args1 = vec![json!({
            "user_id": "alice",
            "amount": 100
        })];

        let operation1 = json!({
            "Transfer": {
                "from": "alice",
                "to": "merchant",
                "amount": 100
            }
        });

        let template = extractor
            .extract("payment_stream", &operation1, &args1, true)
            .unwrap();

        // Instantiate with different args
        let args2 = vec![json!({
            "user_id": "bob",
            "amount": 200
        })];

        let result = instantiator.instantiate(&template, &args2).unwrap();

        // Check that values were substituted
        assert_eq!(
            result,
            json!({
                "Transfer": {
                    "from": "bob",
                    "to": "merchant",
                    "amount": 200
                }
            })
        );
    }

    #[test]
    fn test_substring_pattern_detection() {
        let extractor = TemplateExtractor::new();
        let instantiator = TemplateInstantiator::new();

        // Args with simple values
        let args = vec![json!({
            "user_id": "alice",
            "amount": 100
        })];

        // Operations with composite keys that contain arg values
        let operation = json!({
            "Get": {"key": "user:alice:profile"}
        });

        let template = extractor
            .extract("kv_stream", &operation, &args, false)
            .unwrap();

        // Check that placeholder was created with substring
        let pattern_str = template.pattern.to_string();
        assert!(
            pattern_str.contains("{{$[0].user_id}}"),
            "Should detect user_id as substring"
        );
        assert!(
            pattern_str.contains("user:{{$[0].user_id}}:profile"),
            "Should preserve surrounding text"
        );

        // Test instantiation with different values
        let new_args = vec![json!({
            "user_id": "bob",
            "amount": 200
        })];

        let result = instantiator.instantiate(&template, &new_args).unwrap();
        assert_eq!(
            result,
            json!({"Get": {"key": "user:bob:profile"}}),
            "Should substitute substring correctly"
        );
    }

    #[test]
    fn test_positional_args() {
        let extractor = TemplateExtractor::new();
        let instantiator = TemplateInstantiator::new();

        let args = vec![json!("alice"), json!(100), json!("transfer")];

        let operation = json!({
            "type": "transfer",
            "user": "alice",
            "amount": 100
        });

        let template = extractor
            .extract("stream", &operation, &args, false)
            .unwrap();

        // Should find correlations with positional args
        assert!(template.required_paths.contains("$[0]")); // alice
        assert!(template.required_paths.contains("$[1]")); // 100
        assert!(template.required_paths.contains("$[2]")); // transfer

        // Test instantiation with new values
        let new_args = vec![json!("bob"), json!(200), json!("transfer")];
        let result = instantiator.instantiate(&template, &new_args).unwrap();

        assert_eq!(
            result,
            json!({
                "type": "transfer",
                "user": "bob",
                "amount": 200  // Number because operation had number
            })
        );
    }
}
