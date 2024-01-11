use crate::streaming::utils::hash;
use iggy::models::user_info::UserId;
use iggy::utils::text::as_base64;
use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};

const SIZE: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct PersonalAccessToken {
    pub user_id: UserId,
    pub name: String,
    pub token: String,
    pub expiry: Option<u64>,
}

impl PersonalAccessToken {
    // Raw token is generated and returned only once
    pub fn new(user_id: UserId, name: &str, now: u64, expiry: Option<u32>) -> (Self, String) {
        let mut buffer: [u8; SIZE] = [0; SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        let token_hash = Self::hash_token(&token);
        let expiry = expiry.map(|e| now + e as u64 * 1_000_000);
        (
            Self {
                user_id,
                name: name.to_string(),
                token: token_hash,
                expiry,
            },
            token,
        )
    }

    pub fn is_expired(&self, now: u64) -> bool {
        match self.expiry {
            Some(expiry) => now > expiry,
            None => false,
        }
    }

    pub fn hash_token(token: &str) -> String {
        hash::calculate_256(token.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::utils::timestamp::IggyTimestamp;
    #[test]
    fn personal_access_token_should_be_created_with_random_secure_value_and_hashed_successfully() {
        let user_id = 1;
        let now = IggyTimestamp::now().to_micros();
        let name = "test_token";
        let (personal_access_token, raw_token) = PersonalAccessToken::new(user_id, name, now, None);
        assert_eq!(personal_access_token.name, name);
        assert!(!personal_access_token.token.is_empty());
        assert!(!raw_token.is_empty());
        assert_ne!(personal_access_token.token, raw_token);
        assert_eq!(
            personal_access_token.token,
            PersonalAccessToken::hash_token(&raw_token)
        );
    }

    #[test]
    fn personal_access_token_should_be_expired_given_passed_expiry() {
        let user_id = 1;
        let now = IggyTimestamp::now().to_micros();
        let expiry = 1;
        let name = "test_token";
        let (personal_access_token, _) = PersonalAccessToken::new(user_id, name, now, Some(expiry));
        assert!(personal_access_token.is_expired(now + expiry as u64 * 1_000_000 + 1));
    }
}
