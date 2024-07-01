use super::user_info::UserId;
use serde::{Deserialize, Serialize};

/// `IdentityInfo` represents the information about an identity.
/// It consists of the following fields:
/// - `user_id`: the unique identifier (numeric) of the user.
/// - `access_token`: the optional access token, used only by HTTP transport.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    /// The unique identifier (numeric) of the user.
    pub user_id: UserId,
    /// The optional tokens, used only by HTTP transport.
    pub access_token: Option<TokenInfo>,
}

/// `TokenInfo` represents the details of the access token.
/// It consists of the following fields:
/// - `token`: the value of token.
/// - `expiry`: the expiry of token.
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    /// The value of token.
    pub token: String,
    /// The expiry of token.
    pub expiry: u64,
}
