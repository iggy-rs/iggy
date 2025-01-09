use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Clone, Copy, Debug, Default, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum Confirmation {
    #[default]
    Wait,
    NoWait,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_to_string() {
        assert_eq!(Confirmation::Wait.to_string(), "wait");
        assert_eq!(Confirmation::NoWait.to_string(), "no_wait");
    }

    #[test]
    fn test_from_str() {
        assert_eq!(Confirmation::from_str("wait").unwrap(), Confirmation::Wait);
        assert_eq!(
            Confirmation::from_str("no_wait").unwrap(),
            Confirmation::NoWait
        );
    }

    #[test]
    fn test_default() {
        assert_eq!(Confirmation::default(), Confirmation::Wait);
    }
}
