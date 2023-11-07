use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use crate::error::Error;

// for now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka) in addition to that
// we should consider brotli as well.
#[derive(Debug, PartialEq, Clone)]
pub enum CompressionKind {
    None,
    Gzip,
}

impl FromStr for CompressionKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Gzip" | "gzip" => Ok(CompressionKind::Gzip),
            "None" | "none" => Ok(CompressionKind::None),
            _ => Err(format!("Unknown compression type: {}", s)),
        }
    }
}

impl CompressionKind {
    pub fn as_code(&self) -> u8 {
        match self {
            CompressionKind::None => 1,
            CompressionKind::Gzip => 2,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(CompressionKind::None),
            2 => Ok(CompressionKind::Gzip),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for CompressionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionKind::None => write!(f, "none"),
            CompressionKind::Gzip => write!(f, "gzip"),
        }
    }
}

impl Serialize for CompressionKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionKind::None => serializer.serialize_str("none"),
            CompressionKind::Gzip => serializer.serialize_str("gzip"),
        }
    }
}

impl From<CompressionKind> for String {
    fn from(value: CompressionKind) -> Self {
        match value {
            CompressionKind::None => "none".to_string(),
            CompressionKind::Gzip => "gzip".to_string(),
        }
    }
}
struct CompressionKindVisitor;

impl<'de> Visitor<'de> for CompressionKindVisitor {
    type Value = CompressionKind;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid compression type, check documentation for more information.")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CompressionKind::from_str(value).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CompressionKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CompressionKindVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from() {
        let none_alg = CompressionKind::from_str("none");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionKind::None);

        let none_alg = CompressionKind::from_str("None");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionKind::None);

        let gzip_alg = CompressionKind::from_str("gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionKind::Gzip);

        let gzip_alg = CompressionKind::from_str("Gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionKind::Gzip);
    }

    #[test]
    fn test_into() {
        let none: CompressionKind = CompressionKind::None;
        let none_string: String = none.into();

        assert_eq!(none_string, "none".to_string());

        let gzip: CompressionKind = CompressionKind::Gzip;
        let gzip_string: String = gzip.into();

        assert_eq!(gzip_string, "gzip".to_string());
    }
    #[test]
    fn test_as_code() {
        let none = CompressionKind::None;
        let none_code = none.as_code();
        assert_eq!(none_code, 1);

        let gzip = CompressionKind::Gzip;
        let gzip_code = gzip.as_code();
        assert_eq!(gzip_code, 2);
    }
    #[test]
    fn test_from_code() {
        let none = CompressionKind::from_code(1);
        assert!(none.is_ok());
        assert_eq!(none.unwrap(), CompressionKind::None);

        let gzip = CompressionKind::from_code(2);
        assert!(gzip.is_ok());
        assert_eq!(gzip.unwrap(), CompressionKind::Gzip);
    }
}
