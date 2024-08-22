use keyring::{Entry, Result};

const SESSION_TOKEN_NAME: &str = "iggy-cli-session";
const SESSION_KEYRING_SERVICE_NAME: &str = "iggy-cli-session";

pub struct ServerSession {
    server_address: String,
}

impl ServerSession {
    pub fn new(server_address: String) -> Self {
        Self { server_address }
    }

    pub fn get_server_address(&self) -> &str {
        &self.server_address
    }

    fn get_service_name(&self) -> String {
        format!("{SESSION_KEYRING_SERVICE_NAME}:{}", self.server_address)
    }

    pub fn get_token_name(&self) -> String {
        String::from(SESSION_TOKEN_NAME)
    }

    pub fn is_active(&self) -> bool {
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name()) {
            return entry.get_password().is_ok();
        }

        false
    }

    pub fn store(&self, token: &str) -> Result<()> {
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.set_password(token)?;
        Ok(())
    }

    pub fn get_token(&self) -> Option<String> {
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name()) {
            if let Ok(token) = entry.get_password() {
                return Some(token);
            }
        }

        None
    }

    pub fn delete(&self) -> Result<()> {
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.delete_credential()
    }
}
