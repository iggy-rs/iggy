use std::fmt::Display;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug)]
pub struct UserContext {
    pub user_id: u32,
    pub client_id: u32,
}

impl UserContext {
    pub fn new(user_id: u32, client_id: u32) -> Self {
        Self { user_id, client_id }
    }
}

impl Display for UserContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "User ID: {}, Client ID: {}",
            self.user_id, self.client_id
        )
    }
}
