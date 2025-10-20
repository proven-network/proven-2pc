//! Collection type coercions (List, Array, Map, Struct)

use crate::error::{Error, Result};
use crate::types::{DataType, Value};
use std::collections::HashMap;

// Forward declare the main coercion function from value module
// This will be used recursively for coercing nested collection elements
use super::value::coerce_value_impl;

/// Coerce Map with element type coercion
pub fn coerce_map(map: HashMap<String, Value>, value_type: &DataType) -> Result<Value> {
    // Empty maps can be coerced to any map type
    if map.is_empty() {
        return Ok(Value::Map(HashMap::new()));
    }

    let mut coerced_map = HashMap::new();
    for (key, val) in map {
        // Coerce the value to the expected type
        let coerced_val = coerce_value_impl(val, value_type)?;
        coerced_map.insert(key, coerced_val);
    }
    Ok(Value::Map(coerced_map))
}

/// Coerce List to Array with size checking
pub fn coerce_list_to_array(
    items: Vec<Value>,
    elem_type: &DataType,
    expected_size: Option<usize>,
) -> Result<Value> {
    // Check size constraint if specified
    if let Some(size) = expected_size
        && items.len() != size
    {
        return Err(Error::ArraySizeMismatch {
            expected: size,
            found: items.len(),
        });
    }

    // Coerce each element to the expected type
    let mut coerced_items = Vec::new();
    for item in items {
        coerced_items.push(coerce_value_impl(item, elem_type)?);
    }
    Ok(Value::Array(coerced_items))
}

/// Coerce Array to List
pub fn coerce_array_to_list(items: Vec<Value>, elem_type: &DataType) -> Result<Value> {
    // Coerce each element to the expected type
    let mut coerced_items = Vec::new();
    for item in items {
        coerced_items.push(coerce_value_impl(item, elem_type)?);
    }
    Ok(Value::List(coerced_items))
}

/// Coerce List with element type conversion
pub fn coerce_list(items: Vec<Value>, elem_type: &DataType) -> Result<Value> {
    // Coerce each element to the expected type
    let mut coerced_items = Vec::new();
    for item in items {
        coerced_items.push(coerce_value_impl(item, elem_type)?);
    }
    Ok(Value::List(coerced_items))
}

/// Coerce Map to Struct based on schema
pub fn coerce_map_to_struct(
    map: HashMap<String, Value>,
    schema_fields: &[(String, DataType)],
) -> Result<Value> {
    let mut fields = Vec::new();
    let mut used_keys = Vec::new();

    for (field_name, field_type) in schema_fields {
        if let Some(val) = map.get(field_name) {
            used_keys.push(field_name.clone());
            // Coerce the value to the expected field type
            let coerced_val = coerce_value_impl(val.clone(), field_type).map_err(|e| {
                // If coercion fails, provide more specific error for struct fields
                if let Error::TypeMismatch { expected, found } = e {
                    Error::StructFieldTypeMismatch {
                        field: field_name.clone(),
                        expected,
                        found,
                    }
                } else {
                    e
                }
            })?;
            fields.push((field_name.clone(), coerced_val));
        } else {
            return Err(Error::StructFieldMissing(field_name.clone()));
        }
    }

    // Check for extra fields
    let all_keys: Vec<String> = map.keys().cloned().collect();
    let extra_fields: Vec<String> = all_keys
        .into_iter()
        .filter(|k| !used_keys.contains(k))
        .collect();

    if !extra_fields.is_empty() {
        return Err(Error::TypeMismatch {
            expected: "STRUCT".into(),
            found: format!("extra fields: {:?}", extra_fields),
        });
    }

    Ok(Value::Struct(fields))
}

/// Coerce Struct to Struct with field type coercion
pub fn coerce_struct(
    fields: Vec<(String, Value)>,
    schema_fields: &[(String, DataType)],
) -> Result<Value> {
    // First check if the struct is already compatible (considering NULLs)
    if fields.len() == schema_fields.len() {
        let mut all_match = true;
        for ((field_name, _field_val), (schema_name, _schema_type)) in
            fields.iter().zip(schema_fields.iter())
        {
            if field_name != schema_name {
                all_match = false;
                break;
            }
            // NULL values are compatible with any type - don't check their types
            // Non-NULL values will be coerced below if needed
        }
        if all_match {
            // Struct fields match by name, return as-is
            // NULL values are acceptable in any field
            return Ok(Value::Struct(fields));
        }
    }

    // Coerce each field to match the schema
    let mut coerced_fields = Vec::new();
    for (field_name, field_type) in schema_fields {
        // Find the corresponding field in the struct
        let field_value = fields
            .iter()
            .find(|(name, _)| name == field_name)
            .map(|(_, val)| val.clone())
            .ok_or_else(|| Error::StructFieldMissing(field_name.clone()))?;

        // Coerce the field value to the expected type
        let coerced_val = coerce_value_impl(field_value, field_type).map_err(|e| {
            // If coercion fails, provide more specific error for struct fields
            if let Error::TypeMismatch { expected, found } = e {
                Error::StructFieldTypeMismatch {
                    field: field_name.clone(),
                    expected,
                    found,
                }
            } else {
                e
            }
        })?;
        coerced_fields.push((field_name.clone(), coerced_val));
    }

    // Check that we don't have extra fields
    if fields.len() != schema_fields.len() {
        return Err(Error::TypeMismatch {
            expected: "STRUCT".into(),
            found: format!(
                "struct with {} fields, expected {}",
                fields.len(),
                schema_fields.len()
            ),
        });
    }

    Ok(Value::Struct(coerced_fields))
}

/// Parse JSON array string to List
pub fn parse_json_to_list(s: &str, elem_type: &DataType) -> Result<Value> {
    match Value::parse_json_array(s) {
        Ok(Value::List(items)) => {
            // Special handling: if elem_type is I64 (the default) and coercion fails,
            // just use the values as-is (type inference from data)
            if *elem_type == DataType::I64 && !items.is_empty() {
                // Try to coerce to I64 first
                let mut coerced_items = Vec::new();
                let mut all_coercible = true;
                for item in &items {
                    match coerce_value_impl(item.clone(), elem_type) {
                        Ok(coerced) => coerced_items.push(coerced),
                        Err(_) => {
                            all_coercible = false;
                            break;
                        }
                    }
                }

                if all_coercible {
                    Ok(Value::List(coerced_items))
                } else {
                    // Can't coerce to I64, use values as-is (inferred type)
                    Ok(Value::List(items))
                }
            } else {
                // Not the default type, strict coercion
                let mut coerced_items = Vec::new();
                for item in items {
                    coerced_items.push(coerce_value_impl(item, elem_type)?);
                }
                Ok(Value::List(coerced_items))
            }
        }
        Ok(_) => Err(Error::TypeMismatch {
            expected: "LIST".into(),
            found: format!("Invalid JSON array: '{}'", s),
        }),
        Err(e) => Err(Error::InvalidValue(e)),
    }
}

/// Parse JSON array string to Array
pub fn parse_json_to_array(
    s: &str,
    elem_type: &DataType,
    expected_size: Option<usize>,
) -> Result<Value> {
    match Value::parse_json_array(s) {
        Ok(Value::List(items)) => {
            // Check size constraint if specified
            if let Some(size) = expected_size
                && items.len() != size
            {
                return Err(Error::ArraySizeMismatch {
                    expected: size,
                    found: items.len(),
                });
            }
            // Coerce each element to the expected type
            let mut coerced_items = Vec::new();
            for item in items {
                coerced_items.push(coerce_value_impl(item, elem_type)?);
            }
            Ok(Value::Array(coerced_items))
        }
        Ok(_) => Err(Error::TypeMismatch {
            expected: "ARRAY".into(),
            found: format!("Invalid JSON array: '{}'", s),
        }),
        Err(e) => Err(Error::InvalidValue(e)),
    }
}

/// Parse JSON object string to Map
pub fn parse_json_to_map(s: &str, _key_type: &DataType, value_type: &DataType) -> Result<Value> {
    match Value::parse_json_object(s) {
        Ok(Value::Map(mut map)) => {
            // Coerce map values to match expected types
            let mut coerced_map = HashMap::new();
            for (key, val) in map.drain() {
                // Coerce the value to the expected type
                let coerced_val = coerce_value_impl(val, value_type)?;
                coerced_map.insert(key, coerced_val);
            }
            Ok(Value::Map(coerced_map))
        }
        Ok(_) => Err(Error::TypeMismatch {
            expected: format!("MAP({:?}, {:?})", _key_type, value_type),
            found: format!("Invalid JSON object: '{}'", s),
        }),
        Err(e) => Err(Error::InvalidValue(e)),
    }
}

/// Parse JSON object string to Struct
pub fn parse_json_to_struct(s: &str, schema_fields: &[(String, DataType)]) -> Result<Value> {
    match Value::parse_json_object(s) {
        Ok(Value::Map(mut map)) => {
            // Convert Map to Struct with type coercion based on schema
            let mut fields = Vec::new();
            for (field_name, field_type) in schema_fields {
                if let Some(val) = map.remove(field_name) {
                    // Coerce the value to the expected field type
                    let coerced_val = coerce_value_impl(val.clone(), field_type).map_err(|e| {
                        // If coercion fails, provide more specific error for struct fields
                        match e {
                            Error::TypeMismatch { expected, found } => {
                                Error::StructFieldTypeMismatch {
                                    field: field_name.clone(),
                                    expected,
                                    found,
                                }
                            }
                            Error::ExecutionError(msg) => {
                                // Convert ExecutionError to StructFieldTypeMismatch
                                Error::StructFieldTypeMismatch {
                                    field: field_name.clone(),
                                    expected: field_type.to_string(),
                                    found: format!("conversion failed: {}", msg),
                                }
                            }
                            _ => e,
                        }
                    })?;
                    fields.push((field_name.clone(), coerced_val));
                } else {
                    return Err(Error::StructFieldMissing(field_name.clone()));
                }
            }
            // Check for extra fields
            if !map.is_empty() {
                let extra_fields: Vec<String> = map.keys().cloned().collect();
                return Err(Error::TypeMismatch {
                    expected: "STRUCT".into(),
                    found: format!("extra fields: {:?}", extra_fields),
                });
            }
            Ok(Value::Struct(fields))
        }
        Ok(_) => Err(Error::TypeMismatch {
            expected: "STRUCT".into(),
            found: format!("Invalid JSON object: '{}'", s),
        }),
        Err(e) => Err(Error::InvalidValue(e)),
    }
}
