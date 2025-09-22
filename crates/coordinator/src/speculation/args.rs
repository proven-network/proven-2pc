//! Argument flattening and JSONPath utilities

use serde_json::Value;
use std::collections::HashMap;

/// Type alias for flattened arguments (JSONPath -> Value)
pub type FlattenedArgs = HashMap<String, Value>;

/// Handles flattening of arguments into JSONPath mappings
#[derive(Debug, Clone)]
pub struct ArgumentFlattener {
    /// Whether to include intermediate objects in the flattened output
    include_intermediate: bool,
}

impl Default for ArgumentFlattener {
    fn default() -> Self {
        Self {
            include_intermediate: true,
        }
    }
}

impl ArgumentFlattener {
    pub fn new() -> Self {
        Self::default()
    }

    /// Flatten an array of arguments into a JSONPath -> Value mapping
    ///
    /// # Examples
    /// ```
    /// // Positional: [123, "alice"] -> {"$[0]": 123, "$[1]": "alice"}
    /// // Named: [{"user": "alice"}] -> {"$[0]": {...}, "$[0].user": "alice"}
    /// // Mixed: [123, {"user": "alice"}] -> {"$[0]": 123, "$[1]": {...}, "$[1].user": "alice"}
    /// ```
    pub fn flatten(&self, args: &[Value]) -> FlattenedArgs {
        let mut flattened = HashMap::new();

        for (idx, arg) in args.iter().enumerate() {
            let root_path = format!("$[{}]", idx);
            self.flatten_value(&root_path, arg, &mut flattened);
        }

        flattened
    }

    /// Recursively flatten a value at the given path
    fn flatten_value(&self, path: &str, value: &Value, out: &mut FlattenedArgs) {
        // Always store the value at this path
        if self.include_intermediate || !matches!(value, Value::Object(_) | Value::Array(_)) {
            out.insert(path.to_string(), value.clone());
        }

        // Recursively flatten nested structures
        match value {
            Value::Object(map) => {
                // Store the object itself if we're including intermediates
                if self.include_intermediate {
                    out.insert(path.to_string(), value.clone());
                }

                // Flatten each field
                for (key, val) in map {
                    let field_path = format!("{}.{}", path, key);
                    self.flatten_value(&field_path, val, out);
                }
            }
            Value::Array(arr) => {
                // Store the array itself if we're including intermediates
                if self.include_intermediate {
                    out.insert(path.to_string(), value.clone());
                }

                // Flatten each element
                for (idx, val) in arr.iter().enumerate() {
                    let elem_path = format!("{}[{}]", path, idx);
                    self.flatten_value(&elem_path, val, out);
                }
            }
            _ => {
                // Leaf values are already stored above
            }
        }
    }

    /// Find a value in the flattened args and return its JSONPath
    pub fn find_value(&self, flattened: &FlattenedArgs, target: &Value) -> Option<String> {
        // First try exact match
        for (path, value) in flattened {
            if value == target {
                return Some(path.clone());
            }
        }

        // For strings, we could do fuzzy matching here
        if let Value::String(target_str) = target {
            for (path, value) in flattened {
                if let Value::String(s) = value
                    && self.strings_similar(target_str, s)
                {
                    return Some(path.clone());
                }
            }
        }

        None
    }

    /// Check if two strings are similar enough to be considered the same
    fn strings_similar(&self, s1: &str, s2: &str) -> bool {
        // For now, just exact match
        // Could add fuzzy matching, pattern detection, etc.
        s1 == s2
    }

    /// Extract a value from args using a JSONPath
    pub fn extract_path(args: &[Value], path: &str) -> Option<Value> {
        // Parse path like "$[0].user.name" or "$[1][2].field"
        if !path.starts_with("$[") {
            return None;
        }

        // Find the first ] to get the array index
        let close_bracket = path.find(']')?;
        let index_str = &path[2..close_bracket];
        let index: usize = index_str.parse().ok()?;

        // Get the arg at this index
        let mut current = args.get(index)?;

        // Process the rest of the path
        let rest = &path[close_bracket + 1..];
        if rest.is_empty() {
            return Some(current.clone());
        }

        // Parse remaining path segments
        let segments = Self::parse_path_segments(rest);

        for segment in segments {
            match segment {
                PathSegment::Field(field) => {
                    current = current.get(field)?;
                }
                PathSegment::Index(idx) => {
                    current = current.get(idx)?;
                }
            }
        }

        Some(current.clone())
    }

    /// Parse path segments like ".field[0].other"
    fn parse_path_segments(path: &str) -> Vec<PathSegment> {
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut in_bracket = false;

        for ch in path.chars() {
            match ch {
                '.' if !in_bracket => {
                    if !current.is_empty() {
                        segments.push(PathSegment::Field(current.clone()));
                        current.clear();
                    }
                }
                '[' => {
                    if !current.is_empty() {
                        segments.push(PathSegment::Field(current.clone()));
                        current.clear();
                    }
                    in_bracket = true;
                }
                ']' => {
                    if in_bracket {
                        if let Ok(idx) = current.parse::<usize>() {
                            segments.push(PathSegment::Index(idx));
                        }
                        current.clear();
                        in_bracket = false;
                    }
                }
                _ => {
                    current.push(ch);
                }
            }
        }

        if !current.is_empty() {
            segments.push(PathSegment::Field(current));
        }

        segments
    }
}

#[derive(Debug, Clone)]
enum PathSegment {
    Field(String),
    Index(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flatten_positional_args() {
        let flattener = ArgumentFlattener::new();
        let args = vec![json!(123), json!("alice"), json!(true)];
        let flattened = flattener.flatten(&args);

        assert_eq!(flattened.get("$[0]"), Some(&json!(123)));
        assert_eq!(flattened.get("$[1]"), Some(&json!("alice")));
        assert_eq!(flattened.get("$[2]"), Some(&json!(true)));
    }

    #[test]
    fn test_flatten_named_args() {
        let flattener = ArgumentFlattener::new();
        let args = vec![json!({
            "user": "alice",
            "amount": 100,
            "nested": {
                "flag": true
            }
        })];
        let flattened = flattener.flatten(&args);

        assert_eq!(flattened.get("$[0].user"), Some(&json!("alice")));
        assert_eq!(flattened.get("$[0].amount"), Some(&json!(100)));
        assert_eq!(flattened.get("$[0].nested.flag"), Some(&json!(true)));
    }

    #[test]
    fn test_flatten_mixed_args() {
        let flattener = ArgumentFlattener::new();
        let args = vec![
            json!(123),
            json!({"user": "alice", "active": true}),
            json!(["a", "b", "c"]),
        ];
        let flattened = flattener.flatten(&args);

        assert_eq!(flattened.get("$[0]"), Some(&json!(123)));
        assert_eq!(flattened.get("$[1].user"), Some(&json!("alice")));
        assert_eq!(flattened.get("$[1].active"), Some(&json!(true)));
        assert_eq!(flattened.get("$[2][0]"), Some(&json!("a")));
        assert_eq!(flattened.get("$[2][1]"), Some(&json!("b")));
        assert_eq!(flattened.get("$[2][2]"), Some(&json!("c")));
    }

    #[test]
    fn test_find_value() {
        let flattener = ArgumentFlattener::new();
        let args = vec![json!({
            "user": "alice",
            "amount": 100
        })];
        let flattened = flattener.flatten(&args);

        assert_eq!(
            flattener.find_value(&flattened, &json!("alice")),
            Some("$[0].user".to_string())
        );
        assert_eq!(
            flattener.find_value(&flattened, &json!(100)),
            Some("$[0].amount".to_string())
        );
    }

    #[test]
    fn test_extract_path() {
        let args = vec![json!({
            "user": {
                "name": "alice",
                "scores": [10, 20, 30]
            }
        })];

        assert_eq!(
            ArgumentFlattener::extract_path(&args, "$[0].user.name"),
            Some(json!("alice"))
        );
        assert_eq!(
            ArgumentFlattener::extract_path(&args, "$[0].user.scores[1]"),
            Some(json!(20))
        );
    }
}
