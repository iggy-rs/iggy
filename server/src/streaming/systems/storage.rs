use crate::streaming::storage::{Storage, SystemInfoStorage};
use crate::streaming::systems::info::SystemInfo;
use anyhow::Context;
use async_trait::async_trait;
use iggy::error::Error;
use sled::Db;
use std::sync::Arc;
use tracing::{error, info};

const KEY: &str = "system";

#[derive(Debug)]
pub struct FileSystemInfoStorage {
    db: Arc<Db>,
}

impl FileSystemInfoStorage {
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }
}

unsafe impl Send for FileSystemInfoStorage {}
unsafe impl Sync for FileSystemInfoStorage {}

impl SystemInfoStorage for FileSystemInfoStorage {}

#[async_trait]
impl Storage<SystemInfo> for FileSystemInfoStorage {
    async fn load(&self, system_info: &mut SystemInfo) -> Result<(), Error> {
        let data = match self
            .db
            .get(KEY)
            .with_context(|| format!("Failed to load system info"))
        {
            Ok(data) => {
                if let Some(data) = data {
                    let data = rmp_serde::from_slice::<SystemInfo>(&data)
                        .with_context(|| format!("Failed to deserialize system info"));
                    if let Err(err) = data {
                        return Err(Error::CannotDeserializeResource(err));
                    } else {
                        data.unwrap()
                    }
                } else {
                    return Err(Error::ResourceNotFound(KEY.to_string()));
                }
            }
            Err(err) => {
                return Err(Error::CannotLoadResource(err));
            }
        };

        system_info.version = data.version;
        system_info.migrations = data.migrations;
        Ok(())
    }

    async fn save(&self, system_info: &SystemInfo) -> Result<(), Error> {
        match rmp_serde::to_vec(&system_info) {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(KEY, data)
                    .with_context(|| format!("Failed to save system info"))
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                error!("Cannot serialize system info. Error: {}", err);
                return Err(Error::CannotSerializeResource(KEY.to_string()));
            }
        }

        info!("Saved system info, {}", system_info);
        Ok(())
    }

    async fn delete(&self, _: &SystemInfo) -> Result<(), Error> {
        if self.db.remove(KEY).is_err() {
            return Err(Error::CannotDeleteResource(KEY.to_string()));
        }

        info!("Deleted system info");
        Ok(())
    }
}
