use chrono::{DateTime, Local, Utc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct TimeStamp(SystemTime);

impl Default for TimeStamp {
    fn default() -> Self {
        Self(SystemTime::now())
    }
}

impl TimeStamp {
    pub fn now() -> Self {
        TimeStamp::default()
    }

    pub fn to_secs(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_secs()
    }

    pub fn to_micros(&self) -> u64 {
        self.0.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
    }

    pub fn to_string(&self, format: &str) -> String {
        DateTime::<Utc>::from(self.0).format(format).to_string()
    }

    pub fn to_local(&self, format: &str) -> String {
        DateTime::<Local>::from(self.0).format(format).to_string()
    }
}

impl From<u64> for TimeStamp {
    fn from(timestamp: u64) -> Self {
        TimeStamp(UNIX_EPOCH + Duration::from_micros(timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_get() {
        let timestamp = TimeStamp::now();
        assert!(timestamp.to_micros() > 0);
    }

    #[test]
    fn test_timestamp_to_micros() {
        let timestamp = TimeStamp::from(1663472051111);
        assert_eq!(timestamp.to_micros(), 1663472051111);
    }

    #[test]
    fn test_timestamp_to_string() {
        let timestamp = TimeStamp::from(1694968446131680);
        assert_eq!(
            timestamp.to_string("%Y-%m-%d %H:%M:%S"),
            "2023-09-17 16:34:06"
        );
    }

    #[test]
    fn test_timestamp_from_u64() {
        let timestamp = TimeStamp::from(1663472051111);
        assert_eq!(timestamp.to_micros(), 1663472051111);
    }
}
