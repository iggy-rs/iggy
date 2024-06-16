use std::{
    hash::{DefaultHasher, Hash, Hasher},
    str::FromStr,
};

use super::shard::IggyShard;
use crate::streaming::storage::Storage;
use crate::streaming::systems::info::{SemanticVersion, SystemInfo};
use iggy::error::IggyError;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

impl IggyShard {
    pub(crate) async fn load_version(&mut self) -> Result<(), IggyError> {
        info!("Loading system info...");
        let mut system_info = SystemInfo::default();
        if let Err(err) = self.storage.info.load(&mut system_info).await {
            match err {
                IggyError::ResourceNotFound(_) => {
                    info!("System info not found, creating...");
                    self.update_system_info(&mut system_info).await?;
                }
                _ => return Err(err),
            }
        }

        info!("Loaded {system_info}");
        let current_version = SemanticVersion::from_str(VERSION)?;
        let loaded_version = SemanticVersion::from_str(&system_info.version.version)?;
        if current_version.is_equal_to(&loaded_version) {
            info!("System version {current_version} is up to date.");
        } else if current_version.is_greater_than(&loaded_version) {
            info!("System version {current_version} is greater than {loaded_version}, checking the available migrations...");
            self.update_system_info(&mut system_info).await?;
        } else {
            info!("System version {current_version} is lower than {loaded_version}, possible downgrade.");
            self.update_system_info(&mut system_info).await?;
        }

        Ok(())
    }

    async fn update_system_info(&self, system_info: &mut SystemInfo) -> Result<(), IggyError> {
        system_info.update_version(VERSION);
        self.storage.info.save(system_info).await?;
        Ok(())
    }
}

impl SystemInfo {
    pub fn update_version(&mut self, version: &str) {
        self.version.version = version.to_string();
        let mut hasher = DefaultHasher::new();
        self.version.hash.hash(&mut hasher);
        self.version.hash = hasher.finish().to_string();
    }
}
