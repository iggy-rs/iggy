use crate::bytes_serializable::BytesSerializable;
use crate::command::KILL;
use crate::error::Error;
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug)]
pub struct Kill {}

impl FromStr for Kill {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.is_empty() {
            return Err(Error::InvalidCommand);
        }

        Ok(Kill {})
    }
}

impl BytesSerializable for Kill {
    type Type = Kill;

    fn as_bytes(&self) -> Vec<u8> {
        Vec::with_capacity(0)
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self::Type, Error> {
        if !bytes.is_empty() {
            return Err(Error::InvalidCommand);
        }

        Ok(Kill {})
    }
}

impl Display for Kill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", KILL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_empty_bytes() {
        let is_empty = true;
        let command = Kill {};
        let bytes = command.as_bytes();
        assert_eq!(bytes.is_empty(), is_empty);
    }

    #[test]
    fn should_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![];
        let command = Kill::from_bytes(&bytes);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_deserialized_from_empty_bytes() {
        let bytes: Vec<u8> = vec![0];
        let command = Kill::from_bytes(&bytes);
        assert!(command.is_err());
    }

    #[test]
    fn should_be_read_from_empty_string() {
        let input = "";
        let command = Kill::from_str(input);
        assert!(command.is_ok());
    }

    #[test]
    fn should_not_be_read_from_non_empty_string() {
        let input = " ";
        let command = Kill::from_str(input);
        assert!(command.is_err());
    }
}
