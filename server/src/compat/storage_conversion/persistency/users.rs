use crate::streaming::users::user::User;
use anyhow::Context;
use iggy::error::IggyError;
use iggy::models::permissions::Permissions;
use iggy::models::user_info::UserId;
use iggy::models::user_status::UserStatus;
use iggy::utils::timestamp::IggyTimestamp;
use serde::{Deserialize, Serialize};
use sled::Db;

const KEY_PREFIX: &str = "users";

pub async fn load_all(db: &Db) -> Result<Vec<User>, IggyError> {
    let mut users = Vec::new();
    for data in db.scan_prefix(format!("{}:", KEY_PREFIX)) {
        let user_data = match data.with_context(|| {
            format!(
                "Failed to load user, when searching for key: {}",
                KEY_PREFIX
            )
        }) {
            Ok((_, value)) => match rmp_serde::from_slice::<UserData>(&value).with_context(|| {
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
        let mut user = User::empty(user_data.id);
        user.status = user_data.status;
        user.username = user_data.username;
        user.password = user_data.password;
        user.created_at = user_data.created_at;
        user.permissions = user_data.permissions;
        users.push(user);
    }

    Ok(users)
}

#[derive(Debug, Serialize, Deserialize)]
struct UserData {
    pub id: UserId,
    pub status: UserStatus,
    pub username: String,
    pub password: String,
    pub created_at: IggyTimestamp,
    pub permissions: Option<Permissions>,
}
