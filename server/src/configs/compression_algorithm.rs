use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use std::str::FromStr;

// for now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka) in addition to that
// we should consider brotli as well.
#[derive(Debug, PartialEq, Clone)]
pub enum CompressionAlgorithm {
    Gzip,
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Gzip" | "gzip" => Ok(CompressionAlgorithm::Gzip),
            _ => Err(format!("Unknown compression type: {}", s)),
        }
    }
}

impl Serialize for CompressionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionAlgorithm::Gzip => serializer.serialize_str("gzip"),
        }
    }
}
impl From<CompressionAlgorithm> for String {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::Gzip => "gzip".to_string(),
        }
    }
}
struct CompressionAlgorithmVisitor;

impl<'de> Visitor<'de> for CompressionAlgorithmVisitor {
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
        deserializer.deserialize_str(CompressionAlgorithmVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from() {
        let gzip_alg = CompressionAlgorithm::from_str("gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let gzip_alg = CompressionAlgorithm::from_str("Gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);
    }

    #[test]
    fn test_into() {
        let gzip: CompressionAlgorithm = CompressionAlgorithm::Gzip;
        let gzip_string: String = gzip.into();

        assert_eq!(gzip_string, "gzip".to_string());
    }
}
