use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerType {
    #[default]
    Consumer,
    ConsumerGroup,
}

impl ConsumerType {
    pub fn as_code(&self) -> u8 {
        match self {
            ConsumerType::Consumer => 1,
            ConsumerType::ConsumerGroup => 2,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(ConsumerType::Consumer),
            2 => Ok(ConsumerType::ConsumerGroup),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for ConsumerType {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "c" | "consumer" => Ok(ConsumerType::Consumer),
            "g" | "consumer_group" => Ok(ConsumerType::ConsumerGroup),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for ConsumerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerType::Consumer => write!(f, "consumer"),
            ConsumerType::ConsumerGroup => write!(f, "consumer_group"),
        }
    }
}
