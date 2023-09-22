use std::fmt::Display;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug)]
pub struct Session {
    pub user_id: u32,
    pub client_id: u32,
    authentication_enabled: bool,
}

impl Session {
    pub fn new(client_id: u32, user_id: u32) -> Self {
        Self {
            client_id,
            user_id,
            authentication_enabled: true,
        }
    }

    pub fn from_client_id(client_id: u32) -> Self {
        Self::new(client_id, 0)
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

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.user_id > 0 {
            return write!(
                f,
                "client ID: {}, user ID: {}",
                self.client_id, self.user_id
            );
        }

        write!(f, "client ID: {}", self.client_id)
    }
}
