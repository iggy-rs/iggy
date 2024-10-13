#[derive(Default)]
pub enum Status {
    #[default]
    Normal,
    ViewChange,
    Recovering,
}
