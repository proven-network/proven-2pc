//! IN list expression tests
//! Based on gluesql/test-suite/src/expr/in_list.rs
//!
//! NOTE: This file needs to be rewritten to use the actual test framework.
//! The test_query helper doesn't exist in the common module.

mod common;

macro_rules! skip_test {
    ($name:ident) => {
        #[test]
        #[ignore = "test_query helper not available - file needs rewriting"]
        fn $name() {
            unimplemented!("test_query helper not available - needs rewriting")
        }
    };
}

skip_test!(test_in_with_integer_list);
skip_test!(test_in_with_string_list);
skip_test!(test_in_with_float_list);
skip_test!(test_not_in_with_list);
skip_test!(test_in_with_null_values);
skip_test!(test_in_with_single_value);
skip_test!(test_in_with_empty_list);
skip_test!(test_in_with_duplicate_values);
skip_test!(test_in_with_expressions);
skip_test!(test_in_with_different_data_types);

#[test]
#[ignore = "IN with subquery not yet implemented"]
fn test_in_with_subquery() {
    unimplemented!("IN with subquery not yet implemented")
}
