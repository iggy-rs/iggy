use crate::error::Error;

pub trait Validatable {
    fn validate(&self) -> Result<(), Error>;
}
