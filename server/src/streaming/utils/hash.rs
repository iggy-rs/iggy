pub fn calculate(data: &[u8]) -> u128 {
    fastmurmur3::hash(data)
}
