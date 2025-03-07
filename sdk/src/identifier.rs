use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use arrayvec::ArrayString;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// `Identifier` represents the unique identifier of the resources such as stream, topic, partition, user etc.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum Identifier {
    Numeric(u32),
    String(ArrayString<255>),
}

impl Default for Identifier {
    fn default() -> Self {
        Self::Numeric(1)
    }
}

impl Validatable<IggyError> for Identifier {
    fn validate(&self) -> Result<(), IggyError> {
        if self.length() == 0 {
            return Err(IggyError::InvalidIdentifier);
        }

        if self.is_empty() {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(())
    }
}

impl Identifier {
    fn is_empty(&self) -> bool {
        match self {
            Identifier::Numeric(num) => *num == 0,
            Identifier::String(array_string) => array_string.is_empty(),
        }
    }

    /// Returns the numeric value of the identifier.
    pub fn get_u32_value(&self) -> Result<u32, IggyError> {
        match self {
            Identifier::Numeric(num) => Ok(*num),
            Identifier::String(_) => Err(IggyError::InvalidIdentifier),
        }
    }

    /// Returns the string value of the identifier.
    pub fn get_string_value(&self) -> Result<String, IggyError> {
        self.get_cow_str_value().map(|cow| cow.to_string())
    }

    /// Returns the Cow<str> value of the identifier.
    pub fn get_cow_str_value(&self) -> Result<Cow<str>, IggyError> {
        match self {
            Identifier::Numeric(_) => Err(IggyError::InvalidIdentifier),
            Identifier::String(array_string) => {
                Ok(String::from_utf8_lossy(array_string.as_bytes()))
            }
        }
    }

    /// Returns the string representation of the identifier.
    pub fn as_string(&self) -> String {
        self.as_cow_str().to_string()
    }

    // Returns the Cow<str> representation of the identifier.
    pub fn as_cow_str(&self) -> Cow<str> {
        match self {
            Identifier::Numeric(_) => Cow::Owned(self.get_u32_value().unwrap().to_string()),
            Identifier::String(_) => self.get_cow_str_value().unwrap(),
        }
    }

    /// Creates a new identifier from the given identifier.
    pub fn from_identifier(identifier: &Identifier) -> Self {
        match identifier {
            Identifier::Numeric(num) => Identifier::Numeric(*num),
            Identifier::String(array_string) => Identifier::String(*array_string),
        }
    }

    /// Creates a new identifier from the given string value, either numeric or string.
    pub fn from_str_value(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        match value.parse::<u32>() {
            Ok(id) => Identifier::numeric(id),
            Err(_) => Identifier::named(value),
        }
    }

    /// Creates a new identifier from the given numeric value.
    pub fn numeric(value: u32) -> Result<Self, IggyError> {
        if value == 0 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(Identifier::Numeric(value))
    }

    /// Creates a new identifier from the given string value.
    pub fn named(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(Identifier::String(ArrayString::from(value).unwrap()))
    }

    /// Length of the identifier, for a string it is the capacity of the underlying array
    pub fn length(&self) -> usize {
        match self {
            Identifier::Numeric(_) => 4,
            Identifier::String(vec) => vec.len(),
        }
    }

    /// Returns the code of the identifier kind.
    pub fn code(&self) -> u8 {
        match self {
            Identifier::Numeric(_) => 1,
            Identifier::String(_) => 2,
        }
    }

    pub fn extract_name_id(
        &self,
        fallback_name: String,
    ) -> Result<(String, Option<u32>), IggyError> {
        let (name, id) = match self {
            Identifier::Numeric(id) => (fallback_name, Some(*id)),
            Identifier::String(_id) => (self.get_string_value()?, None),
        };
        Ok((name, id))
    }
}

impl Sizeable for Identifier {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.length() as u64 + 2)
    }
}

impl BytesSerializable for Identifier {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.length());
        bytes.put_u8(self.code());
        bytes.put_u8(self.length() as u8);
        match self {
            Identifier::Numeric(value) => bytes.put_slice(&value.to_le_bytes()),
            Identifier::String(array_string) => bytes.put_slice(array_string.as_bytes()),
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidIdentifier);
        }

        let kind = bytes[0];
        let length = bytes[1] as usize;

        let id = match kind {
            1 => {
                if length != 4 {
                    Err(IggyError::InvalidIdentifier)
                } else {
                    let num = u32::from_le_bytes(bytes[2..2 + 4].try_into().unwrap());
                    Ok(Identifier::Numeric(num))
                }
            }
            2 => {
                let value = &bytes[2..2 + length];
                if value.len() != length && length <= 255 {
                    Err(IggyError::InvalidIdentifier)
                } else {
                    let s = std::str::from_utf8(value).map_err(|_| IggyError::InvalidIdentifier)?;
                    let ar = ArrayString::from(s).unwrap();
                    Ok(Identifier::String(ar))
                }
            }
            _ => Err(IggyError::InvalidIdentifier),
        }?;

        id.validate()?;
        Ok(id)
    }
}

impl FromStr for Identifier {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(value) = input.parse::<u32>() {
            return Identifier::numeric(value);
        }

        let identifier = Identifier::named(input)?;
        identifier.validate()?;
        Ok(identifier)
    }
}

impl TryFrom<u32> for Identifier {
    type Error = IggyError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Identifier::numeric(value)
    }
}

impl TryFrom<String> for Identifier {
    type Error = IggyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Identifier::from_str(&value)
    }
}

impl TryFrom<&str> for Identifier {
    type Error = IggyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Identifier::from_str(value)
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Identifier::Numeric(num) => write!(f, "{num}"),
            Identifier::String(array_string) => {
                write!(f, "{}", String::from_utf8_lossy(array_string.as_bytes()))
            }
        }
    }
}

impl Hash for Identifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Identifier::Numeric(_) => self.get_u32_value().unwrap().hash(state),
            Identifier::String(_) => self.get_cow_str_value().unwrap().hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_with_a_value_of_greater_than_zero_should_be_valid() {
        assert!(Identifier::numeric(1).is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_zero_should_be_invalid() {
        assert!(Identifier::numeric(0).is_err());
    }

    #[test]
    fn identifier_with_a_value_of_non_empty_string_should_be_valid() {
        assert!(Identifier::named("test").is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_empty_string_should_be_invalid() {
        assert!(Identifier::named("").is_err());
    }

    #[test]
    fn identifier_with_a_value_of_string_greater_than_255_chars_should_be_invalid() {
        assert!(Identifier::named(&"a".repeat(256)).is_err());
    }

    #[test]
    fn numeric_id_should_be_converted_into_identifier_using_trait() {
        let id = 1;
        if let Identifier::Numeric(num) = id.try_into().unwrap() {
            assert_eq!(id, num);
        } else {
            panic!("identifier not numeric");
        }
    }

    #[test]
    fn string_id_should_be_converted_into_identifier_using_trait() {
        let id = "test";
        if let Identifier::String(av) = id.try_into().unwrap() {
            assert_eq!(av.as_bytes(), id.as_bytes());
        } else {
            panic!("identifier not string");
        }
    }

    #[test]
    fn string_roundtrip() {
        let id = Identifier::named("hello there").unwrap();
        let bytes = id.to_bytes();
        let from_bytes = Identifier::from_bytes(bytes);
        assert!(from_bytes.is_ok());
        assert_eq!(id, from_bytes.unwrap());
    }
}
