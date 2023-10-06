use xxhash_rust::xxh32::xxh32;

pub fn calculate_32(data: &[u8]) -> u32 {
    xxh32(data, 0)
}

pub fn calculate_256(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn given_same_input_calculate_should_produce_same_output() {
        let input = "hello world".as_bytes();
        let output1 = super::calculate_32(input);
        let output2 = super::calculate_32(input);
        assert_eq!(output1, output2);
    }
}
