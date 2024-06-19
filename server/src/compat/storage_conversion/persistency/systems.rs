use crate::streaming::systems::info::SystemInfo;
use anyhow::Context;
use iggy::error::IggyError;
use sled::Db;

const KEY: &str = "system";

pub async fn load(db: &Db) -> Result<SystemInfo, IggyError> {
    let data = match db.get(KEY).with_context(|| "Failed to load system info") {
        Ok(data) => {
            if let Some(data) = data {
                let data = rmp_serde::from_slice::<SystemInfo>(&data)
                    .with_context(|| "Failed to deserialize system info");
                if let Err(err) = data {
                    return Err(IggyError::CannotDeserializeResource(err));
                } else {
                    data.unwrap()
                }
            } else {
                return Err(IggyError::ResourceNotFound(KEY.to_string()));
            }
        }
        Err(err) => {
            return Err(IggyError::CannotLoadResource(err));
        }
    };

    Ok(data)
}
