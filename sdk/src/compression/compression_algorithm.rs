use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use crate::error::IggyError;

// for now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka) in addition to that
// we should consider brotli as well.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum CompressionAlgorithm {
    None,
    Gzip,
    Lz4,
    Zstd,
}
impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(CompressionAlgorithm::Gzip),
            "lz4" => Ok(CompressionAlgorithm::Lz4),
            "zstd" => Ok(CompressionAlgorithm::Zstd),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(format!("Unknown compression type: {}", s)),
        }
    }
}

impl CompressionAlgorithm {
    pub fn as_code(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 1,
            CompressionAlgorithm::Gzip => 2,
            CompressionAlgorithm::Lz4 => 3,
            CompressionAlgorithm::Zstd => 4,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(CompressionAlgorithm::None),
            2 => Ok(CompressionAlgorithm::Gzip),
            3 => Ok(CompressionAlgorithm::Lz4),
            4 => Ok(CompressionAlgorithm::Zstd),
            _ => Err(IggyError::InvalidCommand),
        }
    }
    pub fn min_data_size(&self) -> usize {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 150,
            CompressionAlgorithm::Lz4 => 200,
            CompressionAlgorithm::Zstd => 200,
        }
    }
}

impl Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Gzip => write!(f, "gzip"),
            CompressionAlgorithm::Lz4 => write!(f, "lz4"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
        }
    }
}

impl Serialize for CompressionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionAlgorithm::None => serializer.serialize_str("none"),
            CompressionAlgorithm::Gzip => serializer.serialize_str("gzip"),
            CompressionAlgorithm::Lz4 => serializer.serialize_str("lz4"),
            CompressionAlgorithm::Zstd => serializer.serialize_str("zstd"),
        }
    }
}

impl From<CompressionAlgorithm> for String {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::None => "none".to_string(),
            CompressionAlgorithm::Gzip => "gzip".to_string(),
            CompressionAlgorithm::Lz4 => "lz4".to_string(),
            CompressionAlgorithm::Zstd => "zstd".to_string(),
        }
    }
}
struct CompressionKindVisitor;

impl<'de> Visitor<'de> for CompressionKindVisitor {
    type Value = CompressionAlgorithm;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid compression type, check documentation for more information.")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CompressionAlgorithm::from_str(value).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CompressionAlgorithm {
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
        let none_alg = CompressionAlgorithm::from_str("none");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let none_alg = CompressionAlgorithm::from_str("None");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let gzip_alg = CompressionAlgorithm::from_str("gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let gzip_alg = CompressionAlgorithm::from_str("Gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let lz4_alg = CompressionAlgorithm::from_str("lz4");
        assert!(lz4_alg.is_ok());
        assert_eq!(lz4_alg.unwrap(), CompressionAlgorithm::Lz4);

        let zstd_alg = CompressionAlgorithm::from_str("zstd");
        assert!(zstd_alg.is_ok());
        assert_eq!(zstd_alg.unwrap(), CompressionAlgorithm::Zstd);
    }

    #[test]
    fn test_from_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_str("invalid");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("gzipp");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("lz44");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("ztd");
        assert!(invalid_compression_kind.is_err());
    }

    #[test]
    fn test_into() {
        let none: CompressionAlgorithm = CompressionAlgorithm::None;
        let none_string: String = none.into();

        assert_eq!(none_string, "none".to_string());

        let gzip: CompressionAlgorithm = CompressionAlgorithm::Gzip;
        let lz4: CompressionAlgorithm = CompressionAlgorithm::Lz4;
        let zstd: CompressionAlgorithm = CompressionAlgorithm::Zstd;
        let gzip_string: String = gzip.into();
        let lz4_string: String = lz4.into();
        let zstd_string: String = zstd.into();

        assert_eq!(gzip_string, "gzip".to_string());
        assert_eq!(lz4_string, "lz4".to_string());
        assert_eq!(zstd_string, "zstd".to_string());
    }
    #[test]
    fn test_as_code() {
        let none = CompressionAlgorithm::None;
        let none_code = none.as_code();
        assert_eq!(none_code, 1);

        let gzip = CompressionAlgorithm::Gzip;
        let gzip_code = gzip.as_code();
        assert_eq!(gzip_code, 2);

        let lz4 = CompressionAlgorithm::Lz4;
        let lz4_code = lz4.as_code();
        assert_eq!(lz4_code, 3);

        let zstd = CompressionAlgorithm::Zstd;
        let zstd_code = zstd.as_code();
        assert_eq!(zstd_code, 4);
    }
    #[test]
    fn test_from_code() {
        let none = CompressionAlgorithm::from_code(1);
        assert!(none.is_ok());
        assert_eq!(none.unwrap(), CompressionAlgorithm::None);

        let gzip = CompressionAlgorithm::from_code(2);
        assert!(gzip.is_ok());
        assert_eq!(gzip.unwrap(), CompressionAlgorithm::Gzip);

        let lz4 = CompressionAlgorithm::from_code(3);
        assert!(lz4.is_ok());
        assert_eq!(lz4.unwrap(), CompressionAlgorithm::Lz4);

        let zstd = CompressionAlgorithm::from_code(4);
        assert!(zstd.is_ok());
        assert_eq!(zstd.unwrap(), CompressionAlgorithm::Zstd);
    }
    #[test]
    fn test_from_code_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_code(0);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(69);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(255);
        assert!(invalid_compression_kind.is_err());
    }
}
