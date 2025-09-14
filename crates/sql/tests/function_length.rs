//! LENGTH function tests
//! Based on gluesql/test-suite/src/function/length.rs

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_string() {
    // TODO: Test SELECT LENGTH('Hello.') should return 6
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_list() {
    // TODO: Test SELECT LENGTH(CAST('[1, 2, 3]' AS LIST)) should return 3
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_map() {
    // TODO: Test SELECT LENGTH(CAST('{"a": 1, "b": 5, "c": 9, "d": 10}' AS MAP)) should return 4
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_korean_characters() {
    // TODO: Test SELECT LENGTH('한글') should return 2
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_mixed_korean_ascii() {
    // TODO: Test SELECT LENGTH('한글 abc') should return 6
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_accented_characters() {
    // TODO: Test SELECT LENGTH('é') should return 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_emoji_simple() {
    // TODO: Test SELECT LENGTH('🧑') should return 1
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_emoji_with_modifier() {
    // TODO: Test SELECT LENGTH('❤️') should return 2 (heart emoji with variation selector)
}

#[ignore = "not yet implemented"]
#[test]
fn test_length_with_emoji_compound() {
    // TODO: Test SELECT LENGTH('👩‍🔬') should return 3 (woman scientist emoji with zero-width joiner)
}
