pub trait Validatable<E> {
    fn validate(&self) -> Result<(), E>;
}
