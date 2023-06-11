pub fn get(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}
