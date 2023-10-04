use crate::streaming::utils::crypto::{hash_password, verify_password};
use iggy::utils::text::as_base64;
use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};

const SIZE: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonalAccessToken {
    pub token: String,
    pub expiry: Option<u64>,
}

#[allow(dead_code)]
impl PersonalAccessToken {
    // Raw token is generated and returned only once
    pub fn new(expiry: Option<u64>) -> Self {
        let mut buffer: [u8; SIZE] = [0; SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        Self { token, expiry }
    }

    // Token is hashed before being stored in the database
    pub fn hash_token(&mut self) {
        self.token = hash_password(&self.token);
    }

    pub fn verify_token(&self, token: &str) -> bool {
        verify_password(token, &self.token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personal_access_token_should_be_created_with_random_secure_value_and_hashed_successfully() {
        let mut pat = PersonalAccessToken::new(None);
        let raw_token = pat.token.clone();
        assert!(!pat.token.is_empty());

        pat.hash_token();
        assert!(!pat.token.is_empty());
        assert_ne!(pat.token, raw_token);
        assert!(pat.verify_token(&raw_token));
    }
}
