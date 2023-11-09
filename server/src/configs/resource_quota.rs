extern crate byte_unit;

use byte_unit::Byte;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use sysinfo::SystemExt;

#[derive(Debug, PartialEq, Clone)]
pub enum MemoryResourceQuota {
    Bytes(Byte),
    Percentage(u8),
}

impl MemoryResourceQuota {
    /// Converts the resource quota into bytes.
    /// NOTE: This is a blocking operation and it's slow. Don't use it in the hot path.
    pub fn into(self) -> u64 {
        match self {
            MemoryResourceQuota::Bytes(byte) => byte.get_bytes(),
            MemoryResourceQuota::Percentage(percentage) => {
                let mut sys = sysinfo::System::new_all();
                sys.refresh_system();
                sys.refresh_processes();

                let total_memory = sys.total_memory();
                (total_memory as f64 * (percentage as f64 / 100.0)) as u64
            }
        }
    }
}

impl FromStr for MemoryResourceQuota {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with('%') {
            match s.trim_end_matches('%').parse::<u8>() {
                Ok(val) => {
                    if val > 100 {
                        Err("Percentage cannot be greater than 100".to_string())
                    } else {
                        Ok(MemoryResourceQuota::Percentage(val))
                    }
                }
                Err(_) => Err("Invalid percentage value".to_string()),
            }
        } else {
            match Byte::from_str(s) {
                Ok(byte) => Ok(MemoryResourceQuota::Bytes(byte)),
                Err(_) => Err("Invalid byte unit".to_string()),
            }
        }
    }
}

struct ResourceQuotaVisitor;

impl<'de> Visitor<'de> for ResourceQuotaVisitor {
    type Value = MemoryResourceQuota;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte unit or a percentage")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        MemoryResourceQuota::from_str(value).map_err(de::Error::custom)
    }
}

impl Serialize for MemoryResourceQuota {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            MemoryResourceQuota::Bytes(byte) => serializer.serialize_str(&byte.to_string()),
            MemoryResourceQuota::Percentage(percentage) => {
                serializer.serialize_str(&format!("{}%", percentage))
            }
        }
    }
}

impl<'de> Deserialize<'de> for MemoryResourceQuota {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(ResourceQuotaVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_percentage() {
        let parsed: Result<MemoryResourceQuota, String> = "25%".parse();
        assert_eq!(parsed, Ok(MemoryResourceQuota::Percentage(25)));
    }

    #[test]
    fn test_invalid_percentage() {
        let parsed: Result<MemoryResourceQuota, String> = "125%".parse();
        assert_eq!(
            parsed,
            Err("Percentage cannot be greater than 100".to_string())
        );
    }

    #[test]
    fn test_parse_memory() {
        let parsed: Result<MemoryResourceQuota, String> = "4 GB".parse();
        assert_eq!(
            parsed,
            Ok(MemoryResourceQuota::Bytes(Byte::from_str("4GB").unwrap()))
        );
    }

    #[test]
    fn test_invalid_memory() {
        let parsed: Result<MemoryResourceQuota, String> = "invalid".parse();
        assert_eq!(parsed, Err("Invalid byte unit".to_string()));
    }

    #[test]
    fn test_serialize() {
        let quota = MemoryResourceQuota::Bytes(Byte::from_str("4GB").unwrap());
        let serialized = serde_json::to_string(&quota).unwrap();
        assert_eq!(serialized, json!("4000000000").to_string());

        let quota = MemoryResourceQuota::Percentage(25);
        let serialized = serde_json::to_string(&quota).unwrap();
        assert_eq!(serialized, json!("25%").to_string());
    }

    #[test]
    fn test_deserialize_bytes() {
        let json_data = "\"4000000000\""; // Corresponds to 4GB
        let deserialized: Result<MemoryResourceQuota, serde_json::Error> =
            serde_json::from_str(json_data);

        assert!(deserialized.is_ok());
        let unwrapped = deserialized.unwrap();
        assert_eq!(
            unwrapped,
            MemoryResourceQuota::Bytes(Byte::from_str("4GB").unwrap())
        );
    }

    #[test]
    fn test_deserialize_percentage() {
        let json_data = "\"25%\"";
        let deserialized: Result<MemoryResourceQuota, serde_json::Error> =
            serde_json::from_str(json_data);

        assert!(deserialized.is_ok());
        let unwrapped = deserialized.unwrap();
        assert_eq!(unwrapped, MemoryResourceQuota::Percentage(25));
    }

    #[test]
    fn test_deserialize_invalid_bytes() {
        let json_data = "\"invalid\"";
        let deserialized: Result<MemoryResourceQuota, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_err());
    }

    #[test]
    fn test_deserialize_invalid_percentage() {
        let json_data = "\"125%\"";
        let deserialized: Result<MemoryResourceQuota, serde_json::Error> =
            serde_json::from_str(json_data);
        assert!(deserialized.is_err());
    }
}
