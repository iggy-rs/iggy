use iggy::models::user_info::UserId;
use std::fmt::Display;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug)]
pub struct Session {
    pub user_id: UserId,
    pub client_id: u32,
    pub ip_address: String,
}

impl Session {
    pub fn new(client_id: u32, user_id: UserId, ip_address: String) -> Self {
        Self {
            client_id,
            user_id,
            ip_address,
        }
    }

    pub fn stateless(user_id: UserId, ip_address: String) -> Self {
        Self::new(0, user_id, ip_address)
    }

    pub fn from_client_id(client_id: u32, ip_address: String) -> Self {
        Self::new(client_id, 0, ip_address)
    }

    pub fn set_user_id(&mut self, user_id: UserId) {
        self.user_id = user_id;
    }

    pub fn clear_user_id(&mut self) {
        self.user_id = 0;
    }

    pub fn is_authenticated(&self) -> bool {
        self.user_id > 0
    }
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.user_id > 0 {
            return write!(
                f,
                "client ID: {}, user ID: {}, IP address: {}",
                self.client_id, self.user_id, self.ip_address
            );
        }

        write!(
            f,
            "client ID: {}, IP address: {}",
            self.client_id, self.ip_address
        )
    }
}
