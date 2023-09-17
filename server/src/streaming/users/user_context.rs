use std::fmt::Display;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug)]
pub struct UserContext {
    pub user_id: u32,
    pub client_id: u32,
    authentication_enabled: bool,
}

impl UserContext {
    pub fn new(user_id: u32, client_id: u32) -> Self {
        Self {
            user_id,
            client_id,
            authentication_enabled: true,
        }
    }

    pub fn from_client_id(client_id: u32) -> Self {
        Self::new(0, client_id)
    }

    // In order to avoid the breaking changes for now, this is controlled by the server configuration.
    pub fn disable_authentication(&mut self) {
        self.authentication_enabled = false;
    }

    pub fn set_user_id(&mut self, user_id: u32) {
        self.user_id = user_id;
    }

    pub fn clear_user_id(&mut self) {
        self.user_id = 0;
    }

    pub fn is_authenticated(&self) -> bool {
        !self.authentication_enabled || self.user_id > 0
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
