use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Deserialize, Serialize, Debug)]
pub enum Confirmation {
    #[default]
    WaitWithFlush,
    Wait,
    Nowait,
}

impl FromStr for Confirmation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "wait_with_flush" => Ok(Confirmation::WaitWithFlush),
            "wait" => Ok(Confirmation::Wait),
            "nowait" => Ok(Confirmation::Nowait),
            _ => Err(format!("Invalid confirmation type: {}", s)),
        }
    }
}

impl fmt::Display for Confirmation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Confirmation::WaitWithFlush => "wait_with_flush",
            Confirmation::Wait => "wait",
            Confirmation::Nowait => "nowait",
        };
        write!(f, "{}", s)
    }
}
