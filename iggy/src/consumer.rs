use crate::bytes_serializable::BytesSerializable;
use crate::error::Error;
use crate::validatable::Validatable;
use bytes::BufMut;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::Display;
use std::str::FromStr;

#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct Consumer {
    #[serde(skip)]
    pub kind: ConsumerKind,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_id")]
    pub id: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    #[default]
    Consumer,
    ConsumerGroup,
}

fn default_id() -> u32 {
    0
}

impl Validatable<Error> for Consumer {
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
}

impl Consumer {
    pub fn from_consumer(consumer: &Consumer) -> Self {
        Self {
            kind: consumer.kind,
            id: consumer.id,
        }
    }

    pub fn new(id: u32) -> Self {
        Self {
            kind: ConsumerKind::Consumer,
            id,
        }
    }

    pub fn group(id: u32) -> Self {
        Self {
            kind: ConsumerKind::ConsumerGroup,
            id,
        }
    }
}

impl BytesSerializable for Consumer {
    fn as_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(5);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u32_le(self.id);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>
    where
        Self: Sized,
    {
        if bytes.len() != 5 {
            return Err(Error::InvalidCommand);
        }

        let kind = ConsumerKind::from_code(bytes[0])?;
        let id = u32::from_le_bytes(bytes[1..5].try_into()?);
        let consumer = Consumer { kind, id };
        consumer.validate()?;
        Ok(consumer)
    }
}

impl ConsumerKind {
    pub fn as_code(&self) -> u8 {
        match self {
            ConsumerKind::Consumer => 1,
            ConsumerKind::ConsumerGroup => 2,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(ConsumerKind::Consumer),
            2 => Ok(ConsumerKind::ConsumerGroup),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for Consumer {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let parts = input.split('|').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(Error::InvalidCommand);
        }

        let kind = parts[0].parse::<ConsumerKind>()?;
        let id = parts[1].parse::<u32>()?;
        let consumer = Consumer { kind, id };
        consumer.validate()?;
        Ok(consumer)
    }
}

impl FromStr for ConsumerKind {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "c" | "consumer" => Ok(ConsumerKind::Consumer),
            "g" | "consumer_group" => Ok(ConsumerKind::ConsumerGroup),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.id)
    }
}

impl Display for ConsumerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerKind::Consumer => write!(f, "consumer"),
            ConsumerKind::ConsumerGroup => write!(f, "consumer_group"),
        }
    }
}
