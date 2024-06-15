use crate::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};

/// `RawPersonalAccessToken` represents the raw personal access token - the secured token which is returned only once during the creation.
/// It consists of the following fields:
/// - `token`: the unique token that should be securely stored by the user and can be used for authentication.
#[derive(Debug, Serialize, Deserialize)]
pub struct RawPersonalAccessToken {
    /// The unique token that should be securely stored by the user and can be used for authentication.
    pub token: String,
}

/// `PersonalAccessToken` represents the personal access token. It does not contain the token itself, but the information about the token.
/// It consists of the following fields:
/// - `name`: the unique name of the token.
/// - `expiry`: the optional expiry of the token.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersonalAccessTokenInfo {
    /// The unique name of the token.
    pub name: String,
    /// The optional expiry of the token.
    pub expiry_at: Option<IggyTimestamp>,
}
