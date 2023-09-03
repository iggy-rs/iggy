use std::time::{SystemTime, UNIX_EPOCH};

pub fn get() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}
