//! Special type coercions (UUID, INET, Point, Bytea, JSON)

use crate::error::{Error, Result};
use crate::types::Value;
use std::net::IpAddr;

/// Parse string to UUID
pub fn parse_string_to_uuid(s: &str) -> Result<Value> {
    use uuid::Uuid;
    // Try to parse the UUID string (supports standard, URN, and hex formats)
    Uuid::parse_str(s)
        .map(Value::Uuid)
        .map_err(|_| Error::InvalidValue(format!("Failed to parse UUID: {}", s)))
}

/// Parse string to INET (IP address)
pub fn parse_string_to_inet(s: &str) -> Result<Value> {
    // Try to parse the IP address string
    s.parse::<IpAddr>()
        .map(Value::Inet)
        .map_err(|_| Error::InvalidValue(format!("Failed to parse IP address: {}", s)))
}

/// Coerce integer to INET (IPv4 or IPv6)
pub fn coerce_int_to_inet(value: u128) -> Result<Value> {
    use std::net::{Ipv4Addr, Ipv6Addr};

    if value <= u32::MAX as u128 {
        // Fits in IPv4
        Ok(Value::Inet(IpAddr::V4(Ipv4Addr::from(value as u32))))
    } else {
        // Use IPv6
        Ok(Value::Inet(IpAddr::V6(Ipv6Addr::from(value))))
    }
}

/// Parse string to Point (geometry)
pub fn parse_string_to_point(s: &str) -> Result<Value> {
    use crate::types::Point;

    // Parse POINT string format: "POINT(x y)" or "POINT(x, y)"
    let s = s.trim();
    if !s.to_uppercase().starts_with("POINT(") || !s.ends_with(')') {
        return Err(Error::InvalidValue(format!("Failed to parse POINT: {}", s)));
    }

    // Extract coordinates from "POINT(x y)" or "POINT(x, y)"
    let coords = &s[6..s.len() - 1]; // Remove "POINT(" and ")"
    let parts: Vec<&str> = coords
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .collect();

    if parts.len() != 2 {
        return Err(Error::InvalidValue(format!(
            "Failed to parse POINT: expected 2 coordinates, found {}",
            parts.len()
        )));
    }

    let x = parts[0].parse::<f64>().map_err(|_| {
        Error::InvalidValue(format!("Failed to parse POINT x coordinate: {}", parts[0]))
    })?;
    let y = parts[1].parse::<f64>().map_err(|_| {
        Error::InvalidValue(format!("Failed to parse POINT y coordinate: {}", parts[1]))
    })?;

    Ok(Value::Point(Point::new(x, y)))
}

/// Parse string to Bytea (binary data)
/// Supports hex encoding (e.g., "\\x48656c6c6f" or "48656c6c6f")
pub fn parse_string_to_bytea(s: &str) -> Result<Value> {
    let s = s.trim();

    // Handle PostgreSQL-style hex prefix
    let hex_str = if s.starts_with("\\x")
        || s.starts_with("\\X")
        || s.starts_with("0x")
        || s.starts_with("0X")
    {
        &s[2..]
    } else {
        s
    };

    // Decode hex string to bytes
    let bytes = hex::decode(hex_str).map_err(|e| {
        Error::InvalidValue(format!(
            "Failed to parse BYTEA from hex string '{}': {}",
            s, e
        ))
    })?;

    Ok(Value::Bytea(bytes))
}

/// Coerce Bytea to string (hex encoding)
pub fn coerce_bytea_to_string(bytes: &[u8]) -> Result<Value> {
    // Encode as hex with \\x prefix (PostgreSQL style)
    let hex_string = format!("\\x{}", hex::encode(bytes));
    Ok(Value::Str(hex_string))
}

/// Parse string to JSON
pub fn parse_string_to_json(s: &str) -> Result<Value> {
    serde_json::from_str(s)
        .map(Value::Json)
        .map_err(|e| Error::InvalidValue(format!("Invalid JSON: {}", e)))
}

/// Coerce JSON to string (serialize)
pub fn coerce_json_to_string(json: &serde_json::Value) -> Result<Value> {
    Ok(Value::Str(json.to_string()))
}
