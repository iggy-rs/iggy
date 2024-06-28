use core::fmt;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::str::FromStr;

use super::byte_size::IggyByteSize;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum MaxTopicSize {
    #[default]
    /// Use the default size set by the server
    ServerDefault,
    /// Use a custom size
    Custom(IggyByteSize),
    /// Use an unlimited size
    Unlimited,
}

impl MaxTopicSize {
    pub fn new(value: Option<IggyByteSize>) -> Self {
        match value {
            Some(value) => match value.as_bytes_u64() {
                0 => MaxTopicSize::ServerDefault,
                u64::MAX => MaxTopicSize::Unlimited,
                _ => MaxTopicSize::Custom(value),
            },
            None => MaxTopicSize::Unlimited,
        }
    }

    pub fn as_bytes_u64(&self) -> u64 {
        match self {
            MaxTopicSize::ServerDefault => 0,
            MaxTopicSize::Unlimited => u64::MAX,
            MaxTopicSize::Custom(iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
        }
    }
}

impl From<IggyByteSize> for MaxTopicSize {
    fn from(value: IggyByteSize) -> Self {
        match value.as_bytes_u64() {
            0 => MaxTopicSize::ServerDefault,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Custom(value),
        }
    }
}

impl From<u64> for MaxTopicSize {
    fn from(value: u64) -> Self {
        match value {
            0 => MaxTopicSize::ServerDefault,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Custom(IggyByteSize::from(value)),
        }
    }
}

impl From<MaxTopicSize> for u64 {
    fn from(value: MaxTopicSize) -> u64 {
        match value {
            MaxTopicSize::ServerDefault => 0,
            MaxTopicSize::Unlimited => u64::MAX,
            MaxTopicSize::Custom(iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
        }
    }
}

impl From<Option<IggyByteSize>> for MaxTopicSize {
    fn from(value: Option<IggyByteSize>) -> Self {
        match value {
            Some(value) => match value.as_bytes_u64() {
                0 => MaxTopicSize::ServerDefault,
                u64::MAX => MaxTopicSize::Unlimited,
                _ => MaxTopicSize::Custom(value),
            },
            None => MaxTopicSize::ServerDefault,
        }
    }
}

impl FromStr for MaxTopicSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "0" | "server_default" => MaxTopicSize::ServerDefault,
            value => {
                let size = value.parse::<IggyByteSize>().map_err(|e| format!("{e}"))?;
                match size.as_bytes_u64() {
                    0 => MaxTopicSize::ServerDefault,
                    u64::MAX => MaxTopicSize::Unlimited,
                    _ => MaxTopicSize::Custom(size),
                }
            }
        };

        Ok(result)
    }
}

impl Serialize for MaxTopicSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = match *self {
            MaxTopicSize::ServerDefault => 0,
            MaxTopicSize::Unlimited => u64::MAX,
            MaxTopicSize::Custom(ref iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
        };
        serializer.serialize_u64(value)
    }
}

struct MaxTopicSizeVisitor;

impl<'de> Visitor<'de> for MaxTopicSizeVisitor {
    type Value = MaxTopicSize;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a u64 bytes representing a MaxTopicSize")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let result = match value {
            0 => MaxTopicSize::ServerDefault,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Custom(IggyByteSize::from(value)),
        };
        Ok(result)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        MaxTopicSize::from_str(value)
            .map_err(|e| de::Error::custom(format!("Invalid MaxTopicSize: {}", e)))
    }
}

impl<'de> Deserialize<'de> for MaxTopicSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_u64(MaxTopicSizeVisitor)
    }
}

impl fmt::Display for MaxTopicSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MaxTopicSize::Custom(value) => write!(f, "{}", value),
            MaxTopicSize::Unlimited => write!(f, "unlimited"),
            MaxTopicSize::ServerDefault => write!(f, "server_default"),
        }
    }
}
