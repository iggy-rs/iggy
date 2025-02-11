use std::error::Error;
use std::fmt;

#[derive(Debug, Clone)]
pub struct EventConsumerError(pub String);

impl EventConsumerError {
    #[inline]
    pub const fn new(field0: String) -> Self {
        Self(field0)
    }
}

impl Error for EventConsumerError {}

impl fmt::Display for EventConsumerError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EventConsumerError: {}", self.0)
    }
}
