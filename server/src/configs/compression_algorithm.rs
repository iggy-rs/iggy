use serde::{
    de::{self, Deserializer, Visitor},
    Deserialize, Serialize, Serializer,
};
use std::str::FromStr;

// for now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka) in addition to that
// we should consider brotli as well.
#[derive(Debug, PartialEq, Clone)]
pub enum CompressionAlgorithm {
    Producer,
    Gzip,
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Producer" | "producer" => Ok(CompressionAlgorithm::Producer),
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
            CompressionAlgorithm::Producer => serializer.serialize_str("producer"),
            CompressionAlgorithm::Gzip => serializer.serialize_str("gzip"),
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
    use serde_json::json;

    #[test]
    fn test_serialize() {
        let producer_alg = CompressionAlgorithm::Producer;
        let producer_serialized = serde_json::to_string(&producer_alg).unwrap();

        let gzip_alg = CompressionAlgorithm::Gzip;
        let gzip_serialized = serde_json::to_string(&gzip_alg).unwrap();
        assert_eq!(producer_serialized, json!("producer").to_string());
        assert_eq!(gzip_serialized, json!("gzip").to_string());
    }

    #[test]
    fn test_deserialize() {
        let json_data = "\"producer\"";
        let deserialized: Result<CompressionAlgorithm, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_ok());

        let json_data = "\"Producer\"";
        let deserialized: Result<CompressionAlgorithm, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_ok());
        assert_eq!(deserialized.unwrap(), CompressionAlgorithm::Producer);

        let json_data = "\"Gzip\"";
        let deserialized: Result<CompressionAlgorithm, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_ok());

        let json_data = "\"gzip\"";
        let deserialized: Result<CompressionAlgorithm, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_ok());
        assert_eq!(deserialized.unwrap(), CompressionAlgorithm::Gzip);
    }
}
