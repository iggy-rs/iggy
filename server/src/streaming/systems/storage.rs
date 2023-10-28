use crate::streaming::storage::{Storage, SystemInfoStorage};
use crate::streaming::systems::info::SystemInfo;
use anyhow::Context;
use async_trait::async_trait;
use iggy::error::Error;
use sled::Db;
use std::sync::Arc;
use tracing::info;

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
            .with_context(|| "Failed to load system info")
        {
            Ok(data) => {
                if let Some(data) = data {
                    let data = rmp_serde::from_slice::<SystemInfo>(&data)
                        .with_context(|| "Failed to deserialize system info");
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
        match rmp_serde::to_vec(&system_info).with_context(|| "Failed to serialize system info") {
            Ok(data) => {
                if let Err(err) = self
                    .db
                    .insert(KEY, data)
                    .with_context(|| "Failed to save system info")
                {
                    return Err(Error::CannotSaveResource(err));
                }
            }
            Err(err) => {
                return Err(Error::CannotSerializeResource(err));
            }
        }

        info!("Saved system info, {}", system_info);
        Ok(())
    }

    async fn delete(&self, _: &SystemInfo) -> Result<(), Error> {
        if let Err(err) = self
            .db
            .remove(KEY)
            .with_context(|| "Failed to delete system info")
        {
            return Err(Error::CannotDeleteResource(err));
        }

        info!("Deleted system info");
        Ok(())
    }
}
