use crate::streaming::utils::hash;
use iggy::models::user_info::UserId;
use iggy::utils::text::as_base64;
use iggy::utils::timestamp::TimeStamp;
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

#[allow(dead_code)]
impl PersonalAccessToken {
    // Raw token is generated and returned only once
    pub fn new(user_id: UserId, name: &str, expiry: Option<u32>) -> (Self, String) {
        let mut buffer: [u8; SIZE] = [0; SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        let token_hash = Self::hash_token(&token);
        let expiry = expiry.map(|e| TimeStamp::now().to_micros() + e as u64 * 1_000_000);
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

    pub fn is_expired(&self) -> bool {
        match self.expiry {
            Some(expiry) => TimeStamp::now().to_micros() > expiry,
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

    #[test]
    fn personal_access_token_should_be_created_with_random_secure_value_and_hashed_successfully() {
        let user_id = 1;
        let name = "test_token";
        let (pat, raw_token) = PersonalAccessToken::new(user_id, name, None);
        assert_eq!(pat.user_id, user_id);
        assert_eq!(pat.name, name);
        assert!(!pat.token.is_empty());
        assert!(!raw_token.is_empty());
        assert_ne!(pat.token, raw_token);
        assert_eq!(pat.token, PersonalAccessToken::hash_token(&raw_token));
    }
}
