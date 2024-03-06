use core::fmt;
use std::str::FromStr;

use byte_unit::Byte;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use super::byte_size::IggyByteSize;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum MaxTopicSize {
    #[default]
    FromServerConfig,
    Value(IggyByteSize),
    Unlimited,
}

impl MaxTopicSize {
    pub fn new(value: Option<IggyByteSize>) -> Self {
        match value {
            Some(value) if value == IggyByteSize::from(u64::MAX) => MaxTopicSize::Unlimited,
            Some(value) => MaxTopicSize::Value(value),
            None => MaxTopicSize::FromServerConfig,
        }
    }

    pub fn as_bytes_u64(&self) -> u64 {
        match self {
            MaxTopicSize::FromServerConfig => 0,
            MaxTopicSize::Value(iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
            MaxTopicSize::Unlimited => u64::MAX,
        }
    }
}

impl From<IggyByteSize> for MaxTopicSize {
    fn from(value: IggyByteSize) -> Self {
        match value.as_bytes_u64() {
            0 => MaxTopicSize::FromServerConfig,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Value(value),
        }
    }
}

impl From<u64> for MaxTopicSize {
    fn from(value: u64) -> Self {
        match value {
            0 => MaxTopicSize::FromServerConfig,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Value(IggyByteSize::from(value)),
        }
    }
}

impl From<MaxTopicSize> for u64 {
    fn from(value: MaxTopicSize) -> u64 {
        match value {
            MaxTopicSize::FromServerConfig => 0,
            MaxTopicSize::Value(iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
            MaxTopicSize::Unlimited => u64::MAX,
        }
    }
}

impl From<Option<IggyByteSize>> for MaxTopicSize {
    fn from(value: Option<IggyByteSize>) -> Self {
        match value {
            Some(value) if value == IggyByteSize::from(u64::MAX) => MaxTopicSize::Unlimited,
            Some(value) => MaxTopicSize::Value(value),
            None => MaxTopicSize::FromServerConfig,
        }
    }
}

impl FromStr for MaxTopicSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let result = match s {
            "unlimited" | "none" | "None" | "Unlimited" => MaxTopicSize::Unlimited,
            "from_server_config" => MaxTopicSize::FromServerConfig,
            value => {
                let size = value.parse::<Byte>().map_err(|e| format!("{e}"))?;
                if size == 0u64 {
                    MaxTopicSize::FromServerConfig
                } else if size == u64::MAX {
                    MaxTopicSize::Unlimited
                } else {
                    MaxTopicSize::Value(size.as_u64().into())
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
            MaxTopicSize::FromServerConfig => 0,
            MaxTopicSize::Value(ref iggy_byte_size) => iggy_byte_size.as_bytes_u64(),
            MaxTopicSize::Unlimited => u64::MAX,
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
            0 => MaxTopicSize::FromServerConfig,
            u64::MAX => MaxTopicSize::Unlimited,
            _ => MaxTopicSize::Value(IggyByteSize::from(value)),
        };
        Ok(result)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        IggyByteSize::from_str(value)
            .map_err(|e| de::Error::custom(format!("Invalid MaxTopicSize: {}", e)))
            .map(MaxTopicSize::Value)
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
            MaxTopicSize::FromServerConfig => write!(f, "from_server_config"),
            MaxTopicSize::Value(value) => write!(f, "{}", value),
            MaxTopicSize::Unlimited => write!(f, "unlimited"),
        }
    }
}
