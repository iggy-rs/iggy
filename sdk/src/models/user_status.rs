use crate::error::IggyError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `UserStatus` represents the status of the user.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum UserStatus {
    /// The user is active.
    #[default]
    Active,
    /// The user is inactive.
    Inactive,
}

impl FromStr for UserStatus {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "active" => Ok(UserStatus::Active),
            "inactive" => Ok(UserStatus::Inactive),
            _ => Err(IggyError::InvalidUserStatus),
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
    /// Returns the code of the user status.
    pub fn as_code(&self) -> u8 {
        match self {
            UserStatus::Active => 1,
            UserStatus::Inactive => 2,
        }
    }

    /// Returns the user status from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(UserStatus::Active),
            2 => Ok(UserStatus::Inactive),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
