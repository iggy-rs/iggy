use crate::streaming::users::user::User;
use anyhow::Context;
use iggy::error::IggyError;
use sled::Db;

const KEY_PREFIX: &str = "users";

pub async fn load_all(db: &Db) -> Result<Vec<User>, IggyError> {
    let mut users = Vec::new();
    for data in db.scan_prefix(format!("{}:", KEY_PREFIX)) {
        let user = match data.with_context(|| {
            format!(
                "Failed to load user, when searching for key: {}",
                KEY_PREFIX
            )
        }) {
            Ok((_, value)) => match rmp_serde::from_slice::<User>(&value).with_context(|| {
                format!(
                    "Failed to deserialize user, when searching for key: {}",
                    KEY_PREFIX
                )
            }) {
                Ok(user) => user,
                Err(err) => {
                    return Err(IggyError::CannotDeserializeResource(err));
                }
            },
            Err(err) => {
                return Err(IggyError::CannotLoadResource(err));
            }
        };
        users.push(user);
    }

    Ok(users)
}
