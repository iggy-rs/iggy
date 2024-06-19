use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use anyhow::Context;
use iggy::error::IggyError;
use sled::Db;

const KEY_PREFIX: &str = "personal_access_token";

pub async fn load_all(db: &Db) -> Result<Vec<PersonalAccessToken>, IggyError> {
    let mut personal_access_tokens = Vec::new();
    for data in db.scan_prefix(format!("{}:token:", KEY_PREFIX)) {
        let personal_access_token = match data.with_context(|| {
            format!(
                "Failed to load personal access token, when searching by key: {}",
                KEY_PREFIX
            )
        }) {
            Ok((_, value)) => match rmp_serde::from_slice::<PersonalAccessToken>(&value)
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
        personal_access_tokens.push(personal_access_token);
    }

    Ok(personal_access_tokens)
}
