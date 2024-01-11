use chrono::{DateTime, Local, Utc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A struct that represents a timestamp.
///
/// This struct uses `SystemTime` from `std::time` crate.
///
/// # Example
///
/// ```
/// use iggy::utils::timestamp::IggyTimestamp;
///
/// let timestamp = IggyTimestamp::from(1694968446131680);
/// assert_eq!(timestamp.to_string("%Y-%m-%d %H:%M:%S"), "2023-09-17 16:34:06");
/// assert_eq!(timestamp.to_micros(), 1694968446131680);
/// ```
#[derive(Debug)]
pub struct IggyTimestamp(SystemTime);

impl Default for IggyTimestamp {
    fn default() -> Self {
        Self(SystemTime::now())
    }
}

impl IggyTimestamp {
    pub fn now() -> Self {
        IggyTimestamp::default()
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

impl From<u64> for IggyTimestamp {
    fn from(timestamp: u64) -> Self {
        IggyTimestamp(UNIX_EPOCH + Duration::from_micros(timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_get() {
        let timestamp = IggyTimestamp::now();
        assert!(timestamp.to_micros() > 0);
    }

    #[test]
    fn test_timestamp_to_micros() {
        let timestamp = IggyTimestamp::from(1663472051111);
        assert_eq!(timestamp.to_micros(), 1663472051111);
    }

    #[test]
    fn test_timestamp_to_string() {
        let timestamp = IggyTimestamp::from(1694968446131680);
        assert_eq!(
            timestamp.to_string("%Y-%m-%d %H:%M:%S"),
            "2023-09-17 16:34:06"
        );
    }

    #[test]
    fn test_timestamp_from_u64() {
        let timestamp = IggyTimestamp::from(1663472051111);
        assert_eq!(timestamp.to_micros(), 1663472051111);
    }
}
