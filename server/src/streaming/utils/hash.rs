use xxhash_rust::xxh32::xxh32;

pub fn calculate(data: &[u8]) -> u32 {
    xxh32(data, 0)
}

#[cfg(test)]
mod tests {
    #[test]
    fn given_same_input_calculate_should_produce_same_output() {
        let input = "hello world".as_bytes();
        let output1 = super::calculate(input);
        let output2 = super::calculate(input);
        assert_eq!(output1, output2);
    }
}
