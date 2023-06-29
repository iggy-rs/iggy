use std::fmt::Display;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug)]
pub struct ClientContext {
    pub client_id: u32,
}

impl Display for ClientContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client ID: {}", self.client_id)
    }
}
