use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::models::user_info::UserId;
use serde::{Deserialize, Serialize};
use sled::Db;

const KEY_PREFIX: &str = "personal_access_token";

pub async fn load_all(db: &Db) -> Result<Vec<PersonalAccessToken>, IggyError> {
    let mut personal_access_tokens = Vec::new();
    for data in db.scan_prefix(format!("{}:token:", KEY_PREFIX)) {
        let personal_access_token_data = match data.with_context(|| {
            format!(
                "Failed to load personal access token, when searching by key: {}",
                KEY_PREFIX
            )
        }) {
            Ok((_, value)) => match rmp_serde::from_slice::<PersonalAccessTokenData>(&value)
                .with_context(|| {
                    format!(
                        "Failed to deserialize personal access token, when searching by key: {}",
                        KEY_PREFIX
                    )
                }) {
                Ok(personal_access_token) => personal_access_token,
                Err(err) => {
                    return Err(IggyError::CannotDeserializeResource(err));
                }
            },
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };

        let personal_access_token = PersonalAccessToken::raw(
            personal_access_token_data.user_id,
            &personal_access_token_data.name,
            &personal_access_token_data.token,
            personal_access_token_data
                .expiry
                .map(|expiry| expiry.into()),
        );
        personal_access_tokens.push(personal_access_token);
    }

    Ok(personal_access_tokens)
}

#[derive(Debug, Serialize, Deserialize)]
struct PersonalAccessTokenData {
    pub user_id: UserId,
    pub name: String,
    pub token: String,
    pub expiry: Option<u64>,
}
