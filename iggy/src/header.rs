use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

const EMPTY_BYTES: Vec<u8> = vec![];

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderKey(String);

impl HeaderKey {
    pub fn new(key: String) -> Result<Self, Error> {
        if key.is_empty() || key.len() > 255 {
            return Err(Error::InvalidHeaderKey);
        }

        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Hash for HeaderKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderValue {
    pub kind: HeaderKind,
    pub value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum HeaderKind {
    Raw,
    String,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Float32,
    Float64,
}

impl HeaderKind {
    pub fn as_code(&self) -> u8 {
        match self {
            HeaderKind::Raw => 1,
            HeaderKind::String => 2,
            HeaderKind::Bool => 3,
            HeaderKind::Int8 => 4,
            HeaderKind::Int16 => 5,
            HeaderKind::Int32 => 6,
            HeaderKind::Int64 => 7,
            HeaderKind::Int128 => 8,
            HeaderKind::Uint8 => 9,
            HeaderKind::Uint16 => 10,
            HeaderKind::Uint32 => 11,
            HeaderKind::Uint64 => 12,
            HeaderKind::Uint128 => 13,
            HeaderKind::Float32 => 14,
            HeaderKind::Float64 => 15,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(HeaderKind::Raw),
            2 => Ok(HeaderKind::String),
            3 => Ok(HeaderKind::Bool),
            4 => Ok(HeaderKind::Int8),
            5 => Ok(HeaderKind::Int16),
            6 => Ok(HeaderKind::Int32),
            7 => Ok(HeaderKind::Int64),
            8 => Ok(HeaderKind::Int128),
            9 => Ok(HeaderKind::Uint8),
            10 => Ok(HeaderKind::Uint16),
            11 => Ok(HeaderKind::Uint32),
            12 => Ok(HeaderKind::Uint64),
            13 => Ok(HeaderKind::Uint128),
            14 => Ok(HeaderKind::Float32),
            15 => Ok(HeaderKind::Float64),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for HeaderValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.kind)?;
        match self.kind {
            HeaderKind::Raw => write!(f, "{:?}", self.value),
            HeaderKind::String => write!(f, "{}", String::from_utf8_lossy(&self.value)),
            HeaderKind::Bool => write!(f, "{}", self.value[0] != 0),
            HeaderKind::Int8 => write!(
                f,
                "{}",
                i8::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Int16 => write!(
                f,
                "{}",
                i16::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Int32 => write!(
                f,
                "{}",
                i32::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Int64 => write!(
                f,
                "{}",
                i64::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Int128 => write!(
                f,
                "{}",
                i128::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Uint8 => write!(
                f,
                "{}",
                u8::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Uint16 => write!(
                f,
                "{}",
                u16::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Uint32 => write!(
                f,
                "{}",
                u32::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Uint64 => write!(
                f,
                "{}",
                u64::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Uint128 => write!(
                f,
                "{}",
                u128::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Float32 => write!(
                f,
                "{}",
                f32::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
            HeaderKind::Float64 => write!(
                f,
                "{}",
                f64::from_le_bytes(self.value.clone().try_into().unwrap())
            ),
        }
    }
}

impl Display for HeaderKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            HeaderKind::Raw => write!(f, "raw"),
            HeaderKind::String => write!(f, "string"),
            HeaderKind::Bool => write!(f, "bool"),
            HeaderKind::Int8 => write!(f, "int8"),
            HeaderKind::Int16 => write!(f, "int16"),
            HeaderKind::Int32 => write!(f, "int32"),
            HeaderKind::Int64 => write!(f, "int64"),
            HeaderKind::Int128 => write!(f, "int128"),
            HeaderKind::Uint8 => write!(f, "uint8"),
            HeaderKind::Uint16 => write!(f, "uint16"),
            HeaderKind::Uint32 => write!(f, "uint32"),
            HeaderKind::Uint64 => write!(f, "uint64"),
            HeaderKind::Uint128 => write!(f, "uint128"),
            HeaderKind::Float32 => write!(f, "float32"),
            HeaderKind::Float64 => write!(f, "float64"),
        }
    }
}

impl HeaderValue {
    pub fn raw(value: Vec<u8>) -> Result<Self, Error> {
        if value.is_empty() || value.len() > 255 {
            return Err(Error::InvalidHeaderValue);
        }

        Ok(Self {
            kind: HeaderKind::Raw,
            value,
        })
    }

    pub fn string(value: String) -> Result<Self, Error> {
        Self::raw(value.into_bytes())
    }

    pub fn bool(value: bool) -> Result<Self, Error> {
        Self::raw(vec![value as u8])
    }

    pub fn int8(value: i8) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn int16(value: i16) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn int32(value: i32) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn int64(value: i64) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn int128(value: i128) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn uint8(value: u8) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn uint16(value: u16) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn uint32(value: u32) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn uint64(value: u64) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn uint128(value: u128) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn float32(value: f32) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }

    pub fn float64(value: f64) -> Result<Self, Error> {
        Self::raw(value.to_le_bytes().to_vec())
    }
}

impl BytesSerializable for HashMap<HeaderKey, HeaderValue> {
    fn as_bytes(&self) -> Vec<u8> {
        if self.is_empty() {
            return EMPTY_BYTES;
        }

        let mut bytes = vec![];
        for (key, value) in self {
            bytes.extend((key.0.len() as u32).to_le_bytes());
            bytes.extend(key.0.as_bytes());
            bytes.extend(value.kind.as_code().to_le_bytes());
            bytes.extend((value.value.len() as u32).to_le_bytes());
            bytes.extend(&value.value);
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        if bytes.is_empty() {
            return Ok(Self::new());
        }

        let mut headers = Self::new();
        let mut position = 0;
        while position < bytes.len() {
            let key_length = u32::from_le_bytes(bytes[position..position + 4].try_into()?) as usize;
            position += 4;
            let key = String::from_utf8(bytes[position..position + key_length].to_vec());
            if key.is_err() {
                return Err(Error::InvalidHeaderKey);
            }

            let key = key.unwrap();
            position += key_length;
            let kind = HeaderKind::from_code(bytes[position])?;
            position += 1;
            let value_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into()?) as usize;
            position += 4;
            let value = bytes[position..position + value_length].to_vec();
            position += value_length;
            headers.insert(HeaderKey(key), HeaderValue { kind, value });
        }

        Ok(headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey("key-1".to_string()),
            HeaderValue::string("Value 1".to_string()).unwrap(),
        );
        headers.insert(
            HeaderKey("key-2".to_string()),
            HeaderValue::uint64(12345).unwrap(),
        );
        headers.insert(
            HeaderKey("key-3".to_string()),
            HeaderValue::bool(true).unwrap(),
        );

        let bytes = headers.as_bytes();

        let mut position = 0;
        let mut headers_count = 0;
        while position < bytes.len() {
            let key_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let key = String::from_utf8(bytes[position..position + key_length].to_vec()).unwrap();
            position += key_length;
            let kind = HeaderKind::from_code(bytes[position]).unwrap();
            position += 1;
            let value_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let value = bytes[position..position + value_length].to_vec();
            position += value_length;
            let header = headers.get(&HeaderKey(key));
            assert!(header.is_some());
            let header = header.unwrap();
            assert_eq!(header.kind, kind);
            assert_eq!(header.value, value);
            headers_count += 1;
        }

        assert_eq!(headers_count, headers.len());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey("key-1".to_string()),
            HeaderValue::string("Value 1".to_string()).unwrap(),
        );
        headers.insert(
            HeaderKey("key-2".to_string()),
            HeaderValue::uint64(12345).unwrap(),
        );
        headers.insert(
            HeaderKey("key-3".to_string()),
            HeaderValue::bool(true).unwrap(),
        );

        let mut bytes = vec![];
        for (key, value) in &headers {
            bytes.extend((key.0.len() as u32).to_le_bytes());
            bytes.extend(key.0.as_bytes());
            bytes.extend(value.kind.as_code().to_le_bytes());
            bytes.extend((value.value.len() as u32).to_le_bytes());
            bytes.extend(&value.value);
        }

        let deserialized_headers = HashMap::<HeaderKey, HeaderValue>::from_bytes(&bytes);

        assert!(deserialized_headers.is_ok());
        let deserialized_headers = deserialized_headers.unwrap();
        assert_eq!(deserialized_headers.len(), headers.len());

        for (key, value) in &headers {
            let deserialized_value = deserialized_headers.get(key);
            assert!(deserialized_value.is_some());
            let deserialized_value = deserialized_value.unwrap();
            assert_eq!(deserialized_value.kind, value.kind);
            assert_eq!(deserialized_value.value, value.value);
        }
    }
}
