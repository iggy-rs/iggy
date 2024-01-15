use crate::streaming::utils::hash;
use iggy::models::user_info::UserId;
use iggy::utils::text::as_base64;
use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};

const REFRESH_TOKEN_SIZE: usize = 50;

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshToken {
    #[serde(skip)]
    pub token_hash: String,
    pub user_id: u32,
    pub expiry: u64,
}

impl RefreshToken {
    pub fn new(user_id: UserId, now: u64, expiry: u64) -> (Self, String) {
        let mut buffer: [u8; REFRESH_TOKEN_SIZE] = [0; REFRESH_TOKEN_SIZE];
        let system_random = ring::rand::SystemRandom::new();
        system_random.fill(&mut buffer).unwrap();
        let token = as_base64(&buffer);
        let hash = Self::hash_token(&token);
        let expiry = now + expiry;
        (
            Self {
                token_hash: hash,
                user_id,
                expiry,
            },
            token,
        )
    }

    pub fn is_expired(&self, now: u64) -> bool {
        now > self.expiry
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
    fn refresh_token_should_be_created_with_random_secure_value_and_hashed_successfully() {
        let user_id = 1;
        let now = IggyTimestamp::now().to_secs();
        let expiry = 10;
        let (refresh_token, raw_token) = RefreshToken::new(user_id, now, expiry);
        assert_eq!(refresh_token.user_id, user_id);
        assert_eq!(refresh_token.expiry, now + expiry);
        assert!(!raw_token.is_empty());
        assert_ne!(refresh_token.token_hash, raw_token);
        assert_eq!(
            refresh_token.token_hash,
            RefreshToken::hash_token(&raw_token)
        );
    }

    #[test]
    fn refresh_access_token_should_be_expired_given_passed_expiry() {
        let user_id = 1;
        let now = IggyTimestamp::now().to_secs();
        let expiry = 1;
        let (refresh_token, _) = RefreshToken::new(user_id, now, expiry);
        assert!(refresh_token.is_expired(now + expiry + 1));
    }
}
