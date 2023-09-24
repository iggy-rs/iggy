use humantime::format_duration;
use humantime::Duration as HumanDuration;
use std::fmt::Display;
use std::iter::Sum;
use std::ops::Add;
use std::time::Duration;
use std::{convert::From, str::FromStr};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum MessageExpiry {
    /// Set message expiry time to given value
    ExpireDuration(Duration),
    /// Never expire messages
    NeverExpire,
}

impl MessageExpiry {
    pub(crate) fn new(values: Option<Vec<MessageExpiry>>) -> Option<Self> {
        values.map(|items| items.iter().cloned().sum())
    }
}

impl From<&MessageExpiry> for Option<u32> {
    fn from(value: &MessageExpiry) -> Self {
        match value {
            MessageExpiry::ExpireDuration(value) => Some(value.as_secs() as u32),
            MessageExpiry::NeverExpire => None,
        }
    }
}

impl Display for MessageExpiry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverExpire => write!(f, "none"),
            Self::ExpireDuration(value) => write!(f, "{}", format_duration(*value)),
        }
    }
}

impl Sum for MessageExpiry {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.into_iter()
            .fold(MessageExpiry::NeverExpire, |acc, x| acc + x)
    }
}

impl Add for MessageExpiry {
    type Output = MessageExpiry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MessageExpiry::NeverExpire, MessageExpiry::NeverExpire) => MessageExpiry::NeverExpire,
            (MessageExpiry::NeverExpire, message_expiry) => message_expiry,
            (message_expiry, MessageExpiry::NeverExpire) => message_expiry,
            (
                MessageExpiry::ExpireDuration(lhs_duration),
                MessageExpiry::ExpireDuration(rhs_duration),
            ) => MessageExpiry::ExpireDuration(lhs_duration + rhs_duration),
        }
    }
}

impl FromStr for MessageExpiry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "none" => MessageExpiry::NeverExpire,
            value => {
                let duration = value.parse::<HumanDuration>().map_err(|e| format!("{e}"))?;

                if duration.as_secs() > u32::MAX as u64 {
                    return Err(format!(
                        "Value too big for message expiry time, maximum value is {}",
                        format_duration(Duration::from_secs(u32::MAX as u64))
                    ));
                }

                MessageExpiry::ExpireDuration(Duration::from_secs(duration.as_secs()))
            }
        };

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_message_expiry() {
        assert_eq!(
            MessageExpiry::from_str("none").unwrap(),
            MessageExpiry::NeverExpire
        );
        assert_eq!(
            MessageExpiry::from_str("15days").unwrap(),
            MessageExpiry::ExpireDuration(Duration::from_secs(60 * 60 * 24 * 15))
        );
        assert_eq!(
            MessageExpiry::from_str("2min").unwrap(),
            MessageExpiry::ExpireDuration(Duration::from_secs(60 * 2))
        );
        assert_eq!(
            MessageExpiry::from_str("1s").unwrap(),
            MessageExpiry::ExpireDuration(Duration::from_secs(1))
        );
        assert_eq!(
            MessageExpiry::from_str("15days 2min 2s").unwrap(),
            MessageExpiry::ExpireDuration(Duration::from_secs(60 * 60 * 24 * 15 + 60 * 2 + 2))
        );
    }

    #[test]
    fn should_fail_parsing_message_expiry() {
        let x = MessageExpiry::from_str("15se");
        assert!(x.is_err());
        assert_eq!(
            x.unwrap_err(),
            "unknown time unit \"se\", supported units: ns, us, ms, sec, min, hours, days, weeks, months, years (and few variations)"
        );
    }

    #[test]
    fn should_sum_message_expiry() {
        assert_eq!(
            MessageExpiry::NeverExpire + MessageExpiry::NeverExpire,
            MessageExpiry::NeverExpire
        );
        assert_eq!(
            MessageExpiry::NeverExpire + MessageExpiry::ExpireDuration(Duration::from_secs(3)),
            MessageExpiry::ExpireDuration(Duration::from_secs(3))
        );
        assert_eq!(
            MessageExpiry::ExpireDuration(Duration::from_secs(5)) + MessageExpiry::NeverExpire,
            MessageExpiry::ExpireDuration(Duration::from_secs(5))
        );
        assert_eq!(
            MessageExpiry::ExpireDuration(Duration::from_secs(5))
                + MessageExpiry::ExpireDuration(Duration::from_secs(3)),
            MessageExpiry::ExpireDuration(Duration::from_secs(8))
        );
    }

    #[test]
    fn should_sum_message_expiry_from_vec() {
        assert_eq!(
            vec![MessageExpiry::NeverExpire]
                .into_iter()
                .sum::<MessageExpiry>(),
            MessageExpiry::NeverExpire
        );
        let x = vec![
            MessageExpiry::NeverExpire,
            MessageExpiry::ExpireDuration(Duration::from_secs(333)),
            MessageExpiry::NeverExpire,
            MessageExpiry::ExpireDuration(Duration::from_secs(123)),
        ];
        assert_eq!(
            x.into_iter().sum::<MessageExpiry>(),
            MessageExpiry::ExpireDuration(Duration::from_secs(456))
        );
    }

    #[test]
    fn should_check_display_message_expiry() {
        assert_eq!(MessageExpiry::NeverExpire.to_string(), "none");
        assert_eq!(
            MessageExpiry::ExpireDuration(Duration::from_secs(333333)).to_string(),
            "3days 20h 35m 33s"
        );
    }

    #[test]
    fn should_calculate_none_from_never_message_expiry() {
        let message_expiry = MessageExpiry::NeverExpire;
        let result: Option<u32> = From::from(&message_expiry);
        assert_eq!(result, None);
    }

    #[test]
    fn should_calculate_some_seconds_from_message_expire() {
        let duration = std::time::Duration::new(42, 0);
        let message_expiry = MessageExpiry::ExpireDuration(duration);
        let result: Option<u32> = From::from(&message_expiry);
        assert_eq!(result, Some(42));
    }

    #[test]
    fn should_create_new_message_expiry_from_vec() {
        let some_values = vec![
            MessageExpiry::NeverExpire,
            MessageExpiry::ExpireDuration(Duration::from_secs(3)),
            MessageExpiry::ExpireDuration(Duration::from_secs(2)),
            MessageExpiry::ExpireDuration(Duration::from_secs(1)),
        ];
        assert_eq!(
            MessageExpiry::new(Some(some_values)),
            Some(MessageExpiry::ExpireDuration(Duration::from_secs(6)))
        );
        assert_eq!(MessageExpiry::new(None), None);
        let none_values = vec![MessageExpiry::NeverExpire; 10];

        assert_eq!(
            MessageExpiry::new(Some(none_values)),
            Some(MessageExpiry::NeverExpire)
        );
    }
}
