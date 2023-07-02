use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerType {
    #[default]
    Consumer,
    Group,
}

impl ConsumerType {
    pub fn as_code(&self) -> u8 {
        match self {
            ConsumerType::Consumer => 0,
            ConsumerType::Group => 1,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            0 => Ok(ConsumerType::Consumer),
            1 => Ok(ConsumerType::Group),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl FromStr for ConsumerType {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "c" | "consumer" => Ok(ConsumerType::Consumer),
            "g" | "group" => Ok(ConsumerType::Group),
            _ => Err(Error::InvalidCommand),
        }
    }
}

impl Display for ConsumerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsumerType::Consumer => write!(f, "consumer"),
            ConsumerType::Group => write!(f, "group"),
        }
    }
}
