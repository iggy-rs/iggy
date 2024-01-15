/// A trait for validating a type.
pub trait Validatable<E> {
    fn validate(&self) -> Result<(), E>;
}
