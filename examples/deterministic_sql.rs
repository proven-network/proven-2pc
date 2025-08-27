//! Demonstration of deterministic SQL functions with transaction context

use proven_sql::{
    error::Result,
    hlc::{HlcClock, HlcTimestamp, NodeId},
    sql::functions::evaluate_function,
    transaction_id::TransactionContext,
    types::Value,
};

fn main() -> Result<()> {
    println!("=== Deterministic SQL Functions Demo ===\n");

    // Create a transaction context with a known timestamp
    let timestamp = HlcTimestamp::new(
        1_700_000_000_000_000, // microseconds since epoch (Nov 14, 2023)
        0,
        NodeId::new(1),
    );
    let context = TransactionContext::new(timestamp);

    println!("Transaction Context:");
    println!("  Global ID: {}", context.tx_id);
    println!("  Timestamp: {} microseconds since epoch\n", timestamp.physical);

    // Demonstrate deterministic time functions
    println!("--- Time Functions (Deterministic) ---");
    
    let now = evaluate_function("NOW", &[], &context)?;
    println!("  NOW() = {:?}", now);
    
    let current_timestamp = evaluate_function("CURRENT_TIMESTAMP", &[], &context)?;
    println!("  CURRENT_TIMESTAMP() = {:?}", current_timestamp);
    assert_eq!(now, current_timestamp, "NOW and CURRENT_TIMESTAMP should be identical");
    
    let current_date = evaluate_function("CURRENT_DATE", &[], &context)?;
    println!("  CURRENT_DATE() = {:?}", current_date);
    
    let current_time = evaluate_function("CURRENT_TIME", &[], &context)?;
    println!("  CURRENT_TIME() = {:?}", current_time);
    
    // All calls within the same transaction return the same value
    let now2 = evaluate_function("NOW", &[], &context)?;
    assert_eq!(now, now2, "Multiple NOW() calls should return the same value");
    println!("  ✓ Multiple NOW() calls return identical values within transaction");

    // Demonstrate deterministic UUID generation
    println!("\n--- UUID Functions (Deterministic) ---");
    
    let uuid1 = evaluate_function("UUID", &[], &context)?;
    let uuid2 = evaluate_function("UUID", &[], &context)?;
    assert_eq!(uuid1, uuid2, "UUIDs with same sequence should be identical");
    println!("  UUID() = {:?}", uuid1);
    println!("  UUID() = {:?} (identical!)", uuid2);
    
    let uuid_seq1 = evaluate_function("UUID", &[Value::Integer(1)], &context)?;
    let uuid_seq2 = evaluate_function("UUID", &[Value::Integer(2)], &context)?;
    assert_ne!(uuid_seq1, uuid_seq2, "Different sequences should produce different UUIDs");
    println!("  UUID(1) = {:?}", uuid_seq1);
    println!("  UUID(2) = {:?} (different!)", uuid_seq2);
    
    println!("  ✓ UUID generation is deterministic based on transaction ID and sequence");

    // Demonstrate sub-transaction contexts
    println!("\n--- Sub-Transaction Contexts ---");
    
    let sub_context1 = context.sub_transaction(1);
    let sub_context2 = context.sub_transaction(2);
    
    println!("  Main transaction: {}", context.tx_id);
    println!("  Sub-transaction 1: {}", sub_context1.tx_id);
    println!("  Sub-transaction 2: {}", sub_context2.tx_id);
    
    // Sub-transactions share the same timestamp
    let sub_now1 = evaluate_function("NOW", &[], &sub_context1)?;
    let sub_now2 = evaluate_function("NOW", &[], &sub_context2)?;
    assert_eq!(now, sub_now1);
    assert_eq!(now, sub_now2);
    println!("  ✓ Sub-transactions share the same timestamp as parent");
    
    // But generate different UUIDs due to different sub_seq
    let sub_uuid1 = evaluate_function("UUID", &[], &sub_context1)?;
    let sub_uuid2 = evaluate_function("UUID", &[], &sub_context2)?;
    assert_ne!(sub_uuid1, sub_uuid2);
    println!("  ✓ Sub-transactions generate different UUIDs");

    // Demonstrate deterministic math and string functions
    println!("\n--- Other Deterministic Functions ---");
    
    let abs_result = evaluate_function("ABS", &[Value::Integer(-42)], &context)?;
    println!("  ABS(-42) = {:?}", abs_result);
    
    let round_result = evaluate_function("ROUND", &[Value::Decimal("3.14159".parse().unwrap())], &context)?;
    println!("  ROUND(3.14159) = {:?}", round_result);
    
    let upper_result = evaluate_function("UPPER", &[Value::String("hello".to_string())], &context)?;
    println!("  UPPER('hello') = {:?}", upper_result);
    
    let length_result = evaluate_function("LENGTH", &[Value::String("proven-sql".to_string())], &context)?;
    println!("  LENGTH('proven-sql') = {:?}", length_result);

    println!("\n=== All deterministic functions working correctly! ===");
    Ok(())
}