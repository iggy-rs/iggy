#[derive(Debug, Default, PartialEq, Eq)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}
