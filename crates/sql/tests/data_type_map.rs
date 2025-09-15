//! MAP data type tests (key-value pairs)
//! Maps store dynamic key-value pairs, similar to dictionaries or hash maps

mod common;

use common::setup_test;

#[test]
fn test_create_table_with_map_column() {
    let mut ctx = setup_test();

    // MAP with key and value types
    ctx.exec("CREATE TABLE Settings (id INT, config MAP(VARCHAR, VARCHAR))");
    ctx.commit();
}

#[test]
fn test_insert_map_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE UserPreferences (id INT, prefs MAP(VARCHAR, VARCHAR))");

    // Insert map values using object notation
    ctx.exec(r#"INSERT INTO UserPreferences VALUES (1, '{"theme": "dark", "language": "en", "timezone": "UTC"}')"#);
    ctx.exec(r#"INSERT INTO UserPreferences VALUES (2, '{"theme": "light", "language": "fr"}')"#);
    ctx.exec(
        r#"INSERT INTO UserPreferences VALUES (3, '{"notifications": "on", "privacy": "strict"}')"#,
    );

    let results = ctx.query("SELECT id, prefs FROM UserPreferences ORDER BY id");
    assert_eq!(results.len(), 3);

    assert!(results[0].get("prefs").unwrap().contains("Map"));
    assert!(results[1].get("prefs").unwrap().contains("Map"));
    assert!(results[2].get("prefs").unwrap().contains("Map"));

    ctx.commit();
}

#[test]
fn test_map_key_access() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Config (id INT, settings MAP(VARCHAR, VARCHAR))");

    ctx.exec(
        r#"INSERT INTO Config VALUES (1, '{"host": "localhost", "port": "5432", "db": "mydb"}')"#,
    );
    ctx.exec(r#"INSERT INTO Config VALUES (2, '{"host": "remote.server", "port": "3306", "ssl": "true"}')"#);

    // Access map values by key using bracket notation
    let results = ctx.query(
        "SELECT id, settings['host'] AS host, settings['port'] AS port FROM Config ORDER BY id",
    );
    assert_eq!(results.len(), 2);

    assert_eq!(results[0].get("host").unwrap(), "Str(localhost)");
    assert_eq!(results[0].get("port").unwrap(), "Str(5432)");
    assert_eq!(results[1].get("host").unwrap(), "Str(remote.server)");
    assert_eq!(results[1].get("port").unwrap(), "Str(3306)");

    ctx.commit();
}

#[test]
#[ignore = "ANY type is not supported - maps must have homogeneous value types"]
fn test_map_with_different_value_types() {
    let mut ctx = setup_test();

    // Map with mixed value types
    ctx.exec("CREATE TABLE Metadata (id INT, data MAP(VARCHAR, ANY))");

    ctx.exec(r#"INSERT INTO Metadata VALUES (1, '{"count": 42, "active": true, "name": "test", "ratio": 3.14}')"#);

    let results = ctx.query("SELECT data FROM Metadata");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("data").unwrap().contains("Map"));

    ctx.commit();
}

#[test]
fn test_map_in_where_clause() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Products (id INT, attributes MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Products VALUES (1, '{"color": "red", "size": "large", "material": "cotton"}')"#);
    ctx.exec(r#"INSERT INTO Products VALUES (2, '{"color": "blue", "size": "medium", "material": "silk"}')"#);
    ctx.exec(r#"INSERT INTO Products VALUES (3, '{"color": "red", "size": "small", "material": "wool"}')"#);

    // Filter by map value
    let results =
        ctx.query("SELECT id FROM Products WHERE attributes['color'] = 'red' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    // Filter by multiple map values
    let results = ctx.query("SELECT id FROM Products WHERE attributes['size'] = 'large' AND attributes['material'] = 'cotton'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");

    ctx.commit();
}

#[test]
fn test_map_with_nulls() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Features (id INT, flags MAP(VARCHAR, BOOL))");

    ctx.exec(r#"INSERT INTO Features VALUES (1, '{"feature_a": true, "feature_b": false}')"#);
    ctx.exec("INSERT INTO Features VALUES (2, NULL)");
    ctx.exec(r#"INSERT INTO Features VALUES (3, '{"feature_c": true}')"#);

    let results = ctx.query("SELECT id FROM Features WHERE flags IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
fn test_map_key_not_found() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Config (id INT, settings MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Config VALUES (1, '{"host": "localhost", "port": "5432"}')"#);

    // Accessing non-existent key should return NULL
    let results = ctx.query("SELECT id, settings['database'] AS db FROM Config");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("db").unwrap(), "Null");

    ctx.commit();
}

#[test]
fn test_empty_map() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE EmptyMaps (id INT, data MAP(VARCHAR, VARCHAR))");

    ctx.exec("INSERT INTO EmptyMaps VALUES (1, '{}')"); // Empty map
    ctx.exec(r#"INSERT INTO EmptyMaps VALUES (2, '{"key": "value"}')"#);

    let results = ctx.query("SELECT id, data FROM EmptyMaps ORDER BY id");
    assert_eq!(results.len(), 2);
    assert!(results[0].get("data").unwrap().contains("Map"));

    ctx.commit();
}

#[test]
fn test_map_comparison() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Tags (id INT, labels MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Tags VALUES (1, '{"env": "prod", "version": "1.0"}')"#);
    ctx.exec(r#"INSERT INTO Tags VALUES (2, '{"env": "dev", "version": "2.0"}')"#);
    ctx.exec(r#"INSERT INTO Tags VALUES (3, '{"env": "prod", "version": "1.0"}')"#); // Duplicate

    // Maps can be compared for equality
    let results = ctx.query(
        r#"SELECT id FROM Tags WHERE labels = '{"env": "prod", "version": "1.0"}' ORDER BY id"#,
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), "I32(1)");
    assert_eq!(results[1].get("id").unwrap(), "I32(3)");

    ctx.commit();
}

#[test]
fn test_map_update() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Settings (id INT PRIMARY KEY, config MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Settings VALUES (1, '{"theme": "dark", "lang": "en"}')"#);
    ctx.exec(r#"INSERT INTO Settings VALUES (2, '{"theme": "light", "lang": "fr"}')"#);

    // Update map value
    ctx.exec(r#"UPDATE Settings SET config = '{"theme": "auto", "lang": "en", "new_key": "new_value"}' WHERE id = 1"#);

    let results = ctx.query("SELECT config FROM Settings WHERE id = 1");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("config").unwrap().contains("Map"));

    ctx.commit();
}

#[test]
fn test_map_keys_function() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Configs (id INT, settings MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Configs VALUES (1, '{"a": "1", "b": "2", "c": "3"}')"#);

    // KEYS function to get all keys
    let results = ctx.query("SELECT id, KEYS(settings) AS keys FROM Configs");
    assert_eq!(results.len(), 1);
    // Should return a list of keys: ["a", "b", "c"]
    assert!(results[0].get("keys").unwrap().contains("List"));

    ctx.commit();
}

#[test]
fn test_map_values_function() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Configs (id INT, settings MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Configs VALUES (1, '{"a": "1", "b": "2", "c": "3"}')"#);

    // MAP_VALUES function to get all values
    let results = ctx.query("SELECT id, MAP_VALUES(settings) AS vals FROM Configs");
    assert_eq!(results.len(), 1);
    // Should return a list of values: ["1", "2", "3"]
    assert!(results[0].get("vals").unwrap().contains("List"));

    ctx.commit();
}

#[test]
fn test_nested_maps() {
    let mut ctx = setup_test();

    // Map containing other maps
    ctx.exec("CREATE TABLE NestedConfig (id INT, config MAP(VARCHAR, MAP(VARCHAR, VARCHAR)))");

    ctx.exec(
        r#"INSERT INTO NestedConfig VALUES (1, '{
        "database": {"host": "localhost", "port": "5432"},
        "cache": {"host": "redis", "port": "6379"}
    }')"#,
    );

    let results = ctx.query("SELECT config FROM NestedConfig");
    assert_eq!(results.len(), 1);
    assert!(results[0].get("config").unwrap().contains("Map"));

    ctx.commit();
}

#[test]
#[ignore = "GROUP BY with MAP not yet implemented"]
fn test_group_by_map() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE Events (id INT, metadata MAP(VARCHAR, VARCHAR))");

    ctx.exec(r#"INSERT INTO Events VALUES (1, '{"type": "click", "page": "home"}')"#);
    ctx.exec(r#"INSERT INTO Events VALUES (2, '{"type": "view", "page": "about"}')"#);
    ctx.exec(r#"INSERT INTO Events VALUES (3, '{"type": "click", "page": "home"}')"#); // Duplicate

    // GROUP BY map column
    let results =
        ctx.query("SELECT metadata, COUNT(*) as cnt FROM Events GROUP BY metadata ORDER BY cnt");
    assert_eq!(results.len(), 2);

    ctx.commit();
}
