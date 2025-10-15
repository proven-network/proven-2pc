//! INET data type tests
//! Based on gluesql/test-suite/src/data_type/inet.rs

mod common;

use common::setup_test;
use proven_value::Value;
use std::net::IpAddr;

#[test]
fn test_create_table_with_inet_column() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    // Insert basic IPv4 and IPv6 addresses
    ctx.exec("INSERT INTO computer VALUES ('::1')");
    ctx.exec("INSERT INTO computer VALUES ('127.0.0.1')");
    ctx.exec("INSERT INTO computer VALUES ('0.0.0.0')");

    let results = ctx.query("SELECT * FROM computer ORDER BY ip");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
fn test_insert_ipv4_addresses() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    // Test various IPv4 addresses
    ctx.exec("INSERT INTO computer VALUES ('127.0.0.1')");
    ctx.exec("INSERT INTO computer VALUES ('0.0.0.0')");
    ctx.exec("INSERT INTO computer VALUES ('192.168.1.1')");
    ctx.exec("INSERT INTO computer VALUES ('255.255.255.255')");

    let results = ctx.query("SELECT ip FROM computer ORDER BY ip");
    assert_eq!(results.len(), 4);

    // Verify they're stored as Inet values
    for result in &results {
        let ip_val = result.get("ip").unwrap();
        match ip_val {
            Value::Inet(_) => { /* Expected */ }
            _ => panic!("Expected Inet value, got {:?}", ip_val),
        }
    }

    ctx.commit();
}

#[test]
fn test_insert_ipv6_addresses() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    // Test various IPv6 addresses
    ctx.exec("INSERT INTO computer VALUES ('::1')");
    ctx.exec("INSERT INTO computer VALUES ('2001:db8::1')");
    ctx.exec("INSERT INTO computer VALUES ('fe80::1')");

    let results = ctx.query("SELECT ip FROM computer ORDER BY ip");
    assert_eq!(results.len(), 3);

    // Verify they're stored as Inet values
    for result in &results {
        let ip_val = result.get("ip").unwrap();
        match ip_val {
            Value::Inet(_) => { /* Expected */ }
            _ => panic!("Expected Inet value, got {:?}", ip_val),
        }
    }

    ctx.commit();
}

#[test]
fn test_insert_integer_as_ipv4() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    // Integers can be converted to IPv4 addresses
    ctx.exec("INSERT INTO computer VALUES (0)"); // 0.0.0.0
    ctx.exec("INSERT INTO computer VALUES (4294967295)"); // 255.255.255.255
    ctx.exec("INSERT INTO computer VALUES (9876543210)"); // Wraps to IPv6

    let results = ctx.query("SELECT ip FROM computer ORDER BY ip");
    assert_eq!(results.len(), 3);

    ctx.commit();
}

#[test]
#[should_panic(expected = "Failed to parse")]
fn test_insert_invalid_ip_should_error() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    // Invalid IP format with too many octets
    ctx.exec("INSERT INTO computer VALUES ('127.0.0.0.1')");
}

#[test]
fn test_select_inet_values() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    ctx.exec(
        "INSERT INTO computer VALUES
        ('::1'),
        ('127.0.0.1'),
        ('0.0.0.0'),
        (4294967295),
        (9876543210)",
    );

    let results = ctx.query("SELECT * FROM computer ORDER BY ip");
    assert_eq!(results.len(), 5);

    // Check the values match expected IP addresses
    let expected_ips = [
        "0.0.0.0",
        "127.0.0.1",
        "255.255.255.255",
        "::1",
        "::2:4cb0:16ea",
    ];

    for (i, result) in results.iter().enumerate() {
        let ip_val = result.get("ip").unwrap();
        if let Value::Inet(addr) = ip_val {
            let expected: IpAddr = expected_ips[i].parse().unwrap();
            assert_eq!(addr, &expected, "IP at index {} doesn't match", i);
        } else {
            panic!("Expected Inet value at index {}", i);
        }
    }

    ctx.commit();
}

#[test]
fn test_inet_comparisons() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    ctx.exec(
        "INSERT INTO computer VALUES
        ('::1'),
        ('127.0.0.1'),
        ('0.0.0.0'),
        (4294967295),
        (9876543210)",
    );

    // Test equality
    let results = ctx.query("SELECT * FROM computer WHERE ip = '127.0.0.1'");
    assert_eq!(results.len(), 1);

    // Test greater than
    let results = ctx.query("SELECT * FROM computer WHERE ip > '127.0.0.1' ORDER BY ip");
    assert_eq!(results.len(), 3);

    // Verify the results are the expected IPs (in order)
    let expected_ips = ["255.255.255.255", "::1", "::2:4cb0:16ea"];

    for (i, result) in results.iter().enumerate() {
        let ip_val = result.get("ip").unwrap();
        if let Value::Inet(addr) = ip_val {
            let expected: IpAddr = expected_ips[i].parse().unwrap();
            assert_eq!(addr, &expected);
        }
    }

    ctx.commit();
}

#[test]
fn test_inet_mixed_ipv4_ipv6() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (id INT, ip INET)");

    ctx.exec("INSERT INTO computer VALUES (1, '192.168.1.1')");
    ctx.exec("INSERT INTO computer VALUES (2, '::1')");
    ctx.exec("INSERT INTO computer VALUES (3, '10.0.0.1')");
    ctx.exec("INSERT INTO computer VALUES (4, '2001:db8::8a2e:370:7334')");

    let results = ctx.query("SELECT id, ip FROM computer ORDER BY id");
    assert_eq!(results.len(), 4);

    // Verify mixed IPv4 and IPv6 addresses
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(2));
    assert_eq!(results[2].get("id").unwrap(), &Value::I32(3));
    assert_eq!(results[3].get("id").unwrap(), &Value::I32(4));

    ctx.commit();
}

#[test]
fn test_inet_with_null() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (id INT, ip INET)");

    ctx.exec("INSERT INTO computer VALUES (1, '127.0.0.1')");
    ctx.exec("INSERT INTO computer VALUES (2, NULL)");
    ctx.exec("INSERT INTO computer VALUES (3, '::1')");

    let results = ctx.query("SELECT id FROM computer WHERE ip IS NOT NULL ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(1));
    assert_eq!(results[1].get("id").unwrap(), &Value::I32(3));

    let results = ctx.query("SELECT id FROM computer WHERE ip IS NULL");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id").unwrap(), &Value::I32(2));

    ctx.commit();
}

#[test]
fn test_update_inet_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    ctx.exec("INSERT INTO computer VALUES ('127.0.0.1')");
    ctx.exec("UPDATE computer SET ip = '192.168.1.1' WHERE ip = '127.0.0.1'");

    let results = ctx.query("SELECT ip FROM computer");
    assert_eq!(results.len(), 1);

    if let Value::Inet(addr) = results[0].get("ip").unwrap() {
        let expected: IpAddr = "192.168.1.1".parse().unwrap();
        assert_eq!(addr, &expected);
    }

    ctx.commit();
}

#[test]
fn test_delete_inet_value() {
    let mut ctx = setup_test();

    ctx.exec("CREATE TABLE computer (ip INET)");

    ctx.exec("INSERT INTO computer VALUES ('127.0.0.1')");
    ctx.exec("INSERT INTO computer VALUES ('192.168.1.1')");
    ctx.exec("INSERT INTO computer VALUES ('::1')");

    ctx.exec("DELETE FROM computer WHERE ip = '127.0.0.1'");

    let results = ctx.query("SELECT ip FROM computer ORDER BY ip");
    assert_eq!(results.len(), 2);

    ctx.commit();
}
