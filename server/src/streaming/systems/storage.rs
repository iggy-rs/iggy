use crate::streaming::storage::{Storage, SystemInfoStorage};
use crate::streaming::systems::info::SystemInfo;
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
        let data = match self.db.get(KEY) {
            Ok(data) => {
                if let Some(data) = data {
                    let data = rmp_serde::from_slice::<SystemInfo>(&data);
                    if let Err(data) = data {
                        error!("Cannot deserialize system info. Error: {}", data);
                        return Err(Error::CannotDeserializeResource(KEY.to_string()));
                    } else {
                        data.unwrap()
                    }
                } else {
                    return Err(Error::ResourceNotFound(KEY.to_string()));
                }
            }
            Err(error) => {
                error!("Cannot load system info. Error: {}", error);
                return Err(Error::CannotLoadResource(KEY.to_string()));
            }
        };

        system_info.version = data.version;
        system_info.migrations = data.migrations;
        Ok(())
    }

    async fn save(&self, system_info: &SystemInfo) -> Result<(), Error> {
        match rmp_serde::to_vec(&system_info) {
            Ok(data) => {
                if let Err(err) = self.db.insert(KEY, data) {
                    error!("Cannot save system info. Error: {}", err);
                    return Err(Error::CannotSaveResource(KEY.to_string()));
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
