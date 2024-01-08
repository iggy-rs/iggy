use crate::error::Error;
use byte_unit::{Byte, UnitType};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use super::duration::IggyDuration;

/// A struct for representing byte sizes with various utility functions.
///
/// This struct is part of the `crate` module and uses `Byte` from `byte_unit` crate.
/// It also implements serialization and deserialization via the `serde` crate.
///
/// # Examples
///
/// ```
/// use your_crate::IggyByteSize;
///
/// let size = IggyByteSize::from(1024_u64);
/// println!("Size in bytes: {}", size.as_bytes_u64());
/// println!("Human-readable size: {}", size.to_human_string());
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct IggyByteSize(Byte);

impl Default for IggyByteSize {
    fn default() -> Self {
        Self(Byte::from_u64(0))
    }
}

impl IggyByteSize {
    /// Returns the byte size as a `u64`.
    pub fn as_bytes_u64(&self) -> u64 {
        self.0.as_u64()
    }

    /// Returns a human-readable string representation of the byte size using decimal units.
    pub fn to_human_string(&self) -> String {
        self.0.get_appropriate_unit(UnitType::Decimal).to_string()
    }

    /// Returns a human-readable string representation of the byte size.
    /// Returns "unlimited" if the size is zero.
    pub fn to_human_string_with_special_zero(&self) -> String {
        if self.as_bytes_u64() == 0 {
            return "unlimited".to_string();
        }
        self.0.get_appropriate_unit(UnitType::Decimal).to_string()
    }

    /// Calculates the throughput based on the provided duration and returns a human-readable string.
    ///
    /// # Arguments
    ///
    /// * `duration` - A reference to `IggyDuration`.
    pub fn to_human_throughput_string(&self, duration: &IggyDuration) -> String {
        if duration.as_secs() == 0 {
            return "0 B/s".to_string();
        }

        let bytes_per_second = Self::from(self.0.as_u64() / duration.as_secs() as u64);
        format!("{}/s", bytes_per_second)
    }
}

impl From<u64> for IggyByteSize {
    fn from(byte_size: u64) -> Self {
        IggyByteSize(Byte::from_u64(byte_size))
    }
}

impl FromStr for IggyByteSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(IggyByteSize(Byte::from_str(s)?))
    }
}

impl fmt::Display for IggyByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_human_string())
    }
}

impl PartialEq<u64> for IggyByteSize {
    fn eq(&self, other: &u64) -> bool {
        self.as_bytes_u64() == *other
    }
}

impl PartialOrd<u64> for IggyByteSize {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.as_bytes_u64().partial_cmp(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_u64_ok() {
        let byte_size = IggyByteSize::from(123456789);
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_u64_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_ok() {
        let byte_size = IggyByteSize::from_str("123456789").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 123456789);
    }

    #[test]
    fn test_from_str_zero() {
        let byte_size = IggyByteSize::from_str("0").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 0);
    }

    #[test]
    fn test_from_str_invalid() {
        let byte_size = IggyByteSize::from_str("invalid");
        assert!(byte_size.is_err());
    }

    #[test]
    fn test_from_str_gigabytes() {
        let byte_size = IggyByteSize::from_str("1.073 GB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1_073_000_000);
    }

    #[test]
    fn test_from_str_megabytes() {
        let byte_size = IggyByteSize::from_str("1 MB").unwrap();
        assert_eq!(byte_size.as_bytes_u64(), 1_000_000);
    }

    #[test]
    fn test_to_human_string_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        assert_eq!(byte_size.to_string(), "1.073 GB");
    }

    #[test]
    fn test_to_human_string_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.to_human_string(), "0 B");
    }

    #[test]
    fn test_to_human_string_special_zero() {
        let byte_size = IggyByteSize::from(0);
        assert_eq!(byte_size.to_human_string_with_special_zero(), "unlimited");
    }

    #[test]
    fn test_throughput_ok() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(
            byte_size.to_human_throughput_string(&duration),
            "107.3 MB/s"
        );
    }

    #[test]
    fn test_throughput_zero_size() {
        let byte_size = IggyByteSize::from(0);
        let duration = IggyDuration::from_str("10s").unwrap();
        assert_eq!(byte_size.to_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_zero_duration() {
        let byte_size = IggyByteSize::from(1_073_000_000);
        let duration = IggyDuration::from_str("0s").unwrap();
        assert_eq!(byte_size.to_human_throughput_string(&duration), "0 B/s");
    }

    #[test]
    fn test_throughput_very_low() {
        let byte_size = IggyByteSize::from(8);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(byte_size.to_human_throughput_string(&duration), "8 B/s");
    }

    #[test]
    fn test_throughput_very_high() {
        let byte_size = IggyByteSize::from(u64::MAX);
        let duration = IggyDuration::from_str("1s").unwrap();
        assert_eq!(
            byte_size.to_human_throughput_string(&duration),
            "18.446744073709553 EB/s"
        );
    }
}
