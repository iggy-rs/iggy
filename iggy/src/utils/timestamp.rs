use chrono::{DateTime, Utc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn get() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

pub fn to_string(timestamp: u64, format: &str) -> String {
    let system_time = UNIX_EPOCH + Duration::from_micros(timestamp);
    let date_time = DateTime::<Utc>::from(system_time);

    date_time.format(format).to_string()
}
