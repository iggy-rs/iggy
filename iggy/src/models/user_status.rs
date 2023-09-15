use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone, Copy)]
pub enum UserStatus {
    #[default]
    Active,
    Inactive,
}

impl FromStr for UserStatus {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "active" => Ok(UserStatus::Active),
            "inactive" => Ok(UserStatus::Inactive),
            _ => Err(Error::InvalidUserStatus),
        }
    }
}

impl Display for UserStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserStatus::Active => write!(f, "active"),
            UserStatus::Inactive => write!(f, "inactive"),
        }
    }
}

impl UserStatus {
    pub fn as_code(&self) -> u8 {
        match self {
            UserStatus::Active => 1,
            UserStatus::Inactive => 2,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, Error> {
        match code {
            1 => Ok(UserStatus::Active),
            2 => Ok(UserStatus::Inactive),
            _ => Err(Error::InvalidCommand),
        }
    }
}
