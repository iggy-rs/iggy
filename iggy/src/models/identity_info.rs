use super::user_info::UserId;
use serde::{Deserialize, Serialize};

/// `IdentityInfo` represents the information about an identity.
/// It consists of the following fields:
/// - `user_id`: the unique identifier (numeric) of the user.
/// - `token`: the optional token, used only by HTTP transport.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    /// The unique identifier (numeric) of the user.
    pub user_id: UserId,
    /// The optional token, used only by HTTP transport.
    pub token: Option<TokenInfo>,
}

/// `TokenInfo` represents the information about a token, currently used only by HTTP transport.
/// It consists of the following fields:
/// - `access_token`: the access token used for the authentication.
/// - `refresh_token`: the refresh token used to refresh the access token.
/// - `expiry`: the expiry of the access token.
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    /// The access token used for the authentication.
    pub access_token: String,
    /// The refresh token used to refresh the access token.
    pub refresh_token: String,
    /// The expiry of the access token.
    pub expiry: u64,
}
