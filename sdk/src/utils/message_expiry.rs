use super::duration::IggyDuration;
use core::fmt;
use humantime::Duration as HumanDuration;
use serde::de;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::iter::Sum;
use std::ops::Add;
use std::{convert::From, str::FromStr};

pub const MAX_EXPIRY_SECS: u32 = u32::MAX;

/// Message expiry time for a topic.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum MessageExpiry {
    /// Use message expiry time from server configuration.
    #[default]
    FromServerConfig,
    /// Set message expiry time to given value
    ExpireDuration(IggyDuration),
    /// Never expire messages
    Unlimited,
}

impl MessageExpiry {
    pub fn new(values: Option<Vec<MessageExpiry>>) -> Option<Self> {
        values.map(|items| items.iter().cloned().sum())
    }

    pub fn as_micros(&self) -> u64 {
        match self {
            MessageExpiry::FromServerConfig => 0,
            MessageExpiry::Unlimited => u32::MAX as u64 * 1_000_000,
            MessageExpiry::ExpireDuration(value) => value.as_micros(),
        }
    }

    pub fn as_secs(&self) -> u32 {
        match self {
            MessageExpiry::FromServerConfig => 0,
            MessageExpiry::Unlimited => u32::MAX,
            MessageExpiry::ExpireDuration(value) => value.as_secs(),
        }
    }
}

impl From<&MessageExpiry> for Option<u32> {
    fn from(value: &MessageExpiry) -> Self {
        match value {
            MessageExpiry::FromServerConfig => Some(0),
            MessageExpiry::Unlimited => Some(u32::MAX),
            MessageExpiry::ExpireDuration(value) => Some(value.as_secs()),
        }
    }
}

impl From<MessageExpiry> for u32 {
    fn from(value: MessageExpiry) -> Self {
        match value {
            MessageExpiry::FromServerConfig => 0,
            MessageExpiry::Unlimited => u32::MAX,
            MessageExpiry::ExpireDuration(value) => value.as_secs(),
        }
    }
}

impl TryFrom<u64> for MessageExpiry {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value > u32::MAX as u64 {
            return Err(format!(
                "Value {value} is too big for message expiry time, maximum value is u32::MAX: {MAX_EXPIRY_SECS}",
            ));
        }
        Ok(MessageExpiry::from(value as u32))
    }
}

impl From<u32> for MessageExpiry {
    fn from(value: u32) -> Self {
        match value {
            0 => MessageExpiry::FromServerConfig,
            value if value == u32::MAX => MessageExpiry::Unlimited,
            value => MessageExpiry::ExpireDuration(IggyDuration::from(value)),
        }
    }
}

impl From<Option<u32>> for MessageExpiry {
    fn from(value: Option<u32>) -> Self {
        match value {
            Some(0) => MessageExpiry::FromServerConfig,
            Some(value) if value == u32::MAX => MessageExpiry::Unlimited,
            Some(value) => MessageExpiry::ExpireDuration(IggyDuration::from(value)),
            None => MessageExpiry::Unlimited,
        }
    }
}

impl FromStr for MessageExpiry {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unlimited" | "Unlimited" | "none" | "None" | "never" | "Never" | "disabled"
            | "Disabled" => Ok(MessageExpiry::Unlimited),
            value => {
                if let Ok(secs) = value.parse::<u32>() {
                    Ok(MessageExpiry::from(secs))
                } else {
                    Ok(MessageExpiry::try_from(
                        value
                            .parse::<HumanDuration>()
                            .map_err(|e| format!("{e}"))?
                            .as_secs(),
                    )
                    .map_err(|e| e.to_string())?)
                }
            }
        }
    }
}

impl From<Vec<MessageExpiry>> for MessageExpiry {
    fn from(values: Vec<MessageExpiry>) -> Self {
        let mut result = MessageExpiry::Unlimited;
        for value in values {
            result = result + value;
        }
        result
    }
}

impl Display for MessageExpiry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FromServerConfig => write!(f, "from_server_config"),
            Self::Unlimited => write!(f, "unlimited"),
            Self::ExpireDuration(value) => write!(f, "{}", value),
        }
    }
}

impl Sum for MessageExpiry {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.into_iter()
            .fold(MessageExpiry::Unlimited, |acc, x| acc + x)
    }
}

impl Add for MessageExpiry {
    type Output = MessageExpiry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (MessageExpiry::Unlimited, MessageExpiry::Unlimited) => MessageExpiry::Unlimited,
            (MessageExpiry::Unlimited, message_expiry) => message_expiry,
            (message_expiry, MessageExpiry::Unlimited) => message_expiry,
            (
                MessageExpiry::ExpireDuration(lhs_duration),
                MessageExpiry::ExpireDuration(rhs_duration),
            ) => MessageExpiry::ExpireDuration(lhs_duration + rhs_duration),
            (MessageExpiry::FromServerConfig, _) => MessageExpiry::FromServerConfig,
            (_, MessageExpiry::FromServerConfig) => MessageExpiry::FromServerConfig,
        }
    }
}

impl Serialize for MessageExpiry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match *self {
            MessageExpiry::FromServerConfig => 0,
            MessageExpiry::Unlimited => u32::MAX,
            MessageExpiry::ExpireDuration(ref dur) => dur.as_secs(),
        };
        serializer.serialize_u32(value)
    }
}

struct MessageExpiryVisitor;

impl<'de> Visitor<'de> for MessageExpiryVisitor {
    type Value = MessageExpiry;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a duration in seconds, humantime string or 'none', 'unlimited'")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value > u32::MAX as u64 {
            return Err(E::custom(format!(
                "Value {value} is too big for message expiry time, maximum value is u32::MAX: {MAX_EXPIRY_SECS}",
            )));
        }
        Ok(MessageExpiry::from(value as u32))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        MessageExpiry::from_str(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for MessageExpiry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(MessageExpiryVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_message_expiry() {
        assert_eq!(
            MessageExpiry::from_str("none").unwrap(),
            MessageExpiry::Unlimited
        );
        assert_eq!(
            MessageExpiry::from_str("15days").unwrap(),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(60 * 60 * 24 * 15))
        );
        assert_eq!(
            MessageExpiry::from_str("2min").unwrap(),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(60 * 2))
        );
        assert_eq!(
            MessageExpiry::from_str("1s").unwrap(),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(1))
        );
        assert_eq!(
            MessageExpiry::from_str("15days 2min 2s").unwrap(),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(60 * 60 * 24 * 15 + 60 * 2 + 2))
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
            MessageExpiry::Unlimited + MessageExpiry::Unlimited,
            MessageExpiry::Unlimited
        );
        assert_eq!(
            MessageExpiry::Unlimited + MessageExpiry::ExpireDuration(IggyDuration::from_secs(3)),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(3))
        );
        assert_eq!(
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(5)) + MessageExpiry::Unlimited,
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(5))
        );
        assert_eq!(
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(5))
                + MessageExpiry::ExpireDuration(IggyDuration::from_secs(3)),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(8))
        );
    }

    #[test]
    fn should_sum_message_expiry_from_vec() {
        assert_eq!(
            vec![MessageExpiry::Unlimited]
                .into_iter()
                .sum::<MessageExpiry>(),
            MessageExpiry::Unlimited
        );
        let x = vec![
            MessageExpiry::Unlimited,
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(333)),
            MessageExpiry::Unlimited,
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(123)),
        ];
        assert_eq!(
            x.into_iter().sum::<MessageExpiry>(),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(456))
        );
    }

    #[test]
    fn should_check_display_message_expiry() {
        assert_eq!(MessageExpiry::Unlimited.to_string(), "unlimited");
        assert_eq!(
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(333333)).to_string(),
            "3days 20h 35m 33s"
        );
    }

    #[test]
    fn should_calculate_never_message_expiry() {
        let message_expiry = MessageExpiry::Unlimited;
        let result: Option<u32> = From::from(&message_expiry);
        assert_eq!(result, Some(u32::MAX));
    }

    #[test]
    fn should_calculate_some_seconds_from_message_expire() {
        let duration = IggyDuration::from_str("42s").unwrap();
        let message_expiry = MessageExpiry::ExpireDuration(duration);
        let result: Option<u32> = From::from(&message_expiry);
        assert_eq!(result, Some(42));
    }

    #[test]
    fn should_create_new_message_expiry_from_vec() {
        let some_values = vec![
            MessageExpiry::Unlimited,
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(3)),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(2)),
            MessageExpiry::ExpireDuration(IggyDuration::from_secs(1)),
        ];
        assert_eq!(
            MessageExpiry::new(Some(some_values)),
            Some(MessageExpiry::ExpireDuration(IggyDuration::from_secs(6)))
        );
        assert_eq!(MessageExpiry::new(None), None);
        let none_values = vec![MessageExpiry::Unlimited; 10];

        assert_eq!(
            MessageExpiry::new(Some(none_values)),
            Some(MessageExpiry::Unlimited)
        );
    }
}
