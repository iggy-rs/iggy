use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

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
