use murmur3::murmur3_32;

pub fn hash_string(s: &str) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(s), 0)
}

pub fn hash_bytes(bytes: &[u8]) -> std::io::Result<u32> {
    murmur3_32(&mut std::io::Cursor::new(bytes), 0)
}
