pub fn to_lowercase_non_whitespace(value: &str) -> String {
    value.to_lowercase().split_whitespace().collect()
}
