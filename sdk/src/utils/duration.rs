use humantime::format_duration;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
    time::Duration,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct IggyDuration {
    duration: Duration,
}

impl IggyDuration {
    pub fn new(duration: Duration) -> IggyDuration {
        IggyDuration { duration }
    }

    pub fn as_human_time_string(&self) -> String {
        format!("{}", format_duration(self.duration))
    }

    pub fn as_secs(&self) -> u32 {
        self.duration.as_secs() as u32
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    pub fn as_micros(&self) -> u64 {
        self.duration.as_micros() as u64
    }

    pub fn get_duration(&self) -> Duration {
        self.duration
    }

    pub fn is_zero(&self) -> bool {
        self.duration.as_secs() == 0
    }

    pub fn abs_diff(&self, other: IggyDuration) -> IggyDuration {
        let diff = self.duration.as_nanos().abs_diff(other.duration.as_nanos());
        IggyDuration {
            duration: Duration::from_nanos(diff as u64),
        }
    }

    pub fn as_nanos(&self) -> u128 {
        self.duration.as_nanos()
    }
}

impl FromStr for IggyDuration {
    type Err = humantime::DurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = &s.to_lowercase();
        if s == "0" || s == "unlimited" || s == "disabled" || s == "none" {
            Ok(IggyDuration {
                duration: Duration::new(0, 0),
            })
        } else {
            Ok(IggyDuration {
                duration: humantime::parse_duration(s)?,
            })
        }
    }
}

impl From<Option<u64>> for IggyDuration {
    fn from(byte_size: Option<u64>) -> Self {
        match byte_size {
            Some(value) => IggyDuration {
                duration: Duration::from_secs(value),
            },
            None => IggyDuration {
                duration: Duration::new(0, 0),
            },
        }
    }
}

impl From<u64> for IggyDuration {
    fn from(value: u64) -> Self {
        IggyDuration {
            duration: Duration::from_secs(value),
        }
    }
}

impl From<IggyDuration> for u64 {
    fn from(iggy_duration: IggyDuration) -> u64 {
        iggy_duration.duration.as_secs()
    }
}

impl Default for IggyDuration {
    fn default() -> Self {
        IggyDuration {
            duration: Duration::new(0, 0),
        }
    }
}

impl Display for IggyDuration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_human_time_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new() {
        let duration = Duration::new(60, 0); // 60 seconds
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(iggy_duration.as_secs(), 60);
    }

    #[test]
    fn test_as_human_time_string() {
        let duration = Duration::new(3661, 0); // 1 hour, 1 minute and 1 second
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(iggy_duration.as_human_time_string(), "1h 1m 1s");
    }

    #[test]
    fn test_long_duration_as_human_time_string() {
        let duration = Duration::new(36611233, 0); // 1year 1month 28days 1hour 13minutes 37seconds
        let iggy_duration = IggyDuration::new(duration);
        assert_eq!(
            iggy_duration.as_human_time_string(),
            "1year 1month 28days 1h 13m 37s"
        );
    }

    #[test]
    fn test_from_str() {
        let iggy_duration: IggyDuration = "1h 1m 1s".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 3661);
    }

    #[test]
    fn test_display() {
        let duration = Duration::new(3661, 0);
        let iggy_duration = IggyDuration::new(duration);
        let duration_string = format!("{}", iggy_duration);
        assert_eq!(duration_string, "1h 1m 1s");
    }

    #[test]
    fn test_invalid_duration() {
        let result: Result<IggyDuration, _> = "1 hour and 30 minutes".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_seconds_duration() {
        let iggy_duration: IggyDuration = "0s".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_zero_duration() {
        let iggy_duration: IggyDuration = "0".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_unlimited() {
        let iggy_duration: IggyDuration = "unlimited".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }

    #[test]
    fn test_disabled() {
        let iggy_duration: IggyDuration = "disabled".parse().unwrap();
        assert_eq!(iggy_duration.as_secs(), 0);
    }
}
