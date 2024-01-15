use super::user_info::UserId;
use serde::{Deserialize, Serialize};

/// `IdentityInfo` represents the information about an identity.
/// It consists of the following fields:
/// - `user_id`: the unique identifier (numeric) of the user.
/// - `tokens`: the optional tokens, used only by HTTP transport.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityInfo {
    /// The unique identifier (numeric) of the user.
    pub user_id: UserId,
    /// The optional tokens, used only by HTTP transport.
    pub tokens: Option<IdentityTokens>,
}

/// `IdentityTokens` represents the information about the tokens, currently used only by HTTP transport.
/// It consists of the following fields:
/// - `access_token`: the access token used for the authentication.
/// - `refresh_token`: the refresh token used to refresh the access token.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityTokens {
    /// The access token used for the authentication.
    pub access_token: TokenInfo,
    /// The refresh token used to refresh the access token.
    pub refresh_token: TokenInfo,
}

/// `TokenInfo` represents the details of the particular token.
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
