use crate::streaming::persistence::persister::Persister;
use crate::streaming::storage::{Storage, SystemInfoStorage};
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::info;

#[derive(Debug)]
pub struct FileSystemInfoStorage {
    persister: Arc<dyn Persister>,
}

impl FileSystemInfoStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self { persister }
    }
}
unsafe impl Send for FileSystemInfoStorage {}
unsafe impl Sync for FileSystemInfoStorage {}

impl SystemInfoStorage for FileSystemInfoStorage {}

#[async_trait]
impl Storage<SystemInfo> for FileSystemInfoStorage {
    async fn load(&self, system_info: &mut SystemInfo) -> Result<(), IggyError> {
        let file = file::open(&system_info.path).await;
        if file.is_err() {
            return Err(IggyError::ResourceNotFound(system_info.path.clone()));
        }

        let mut file = file.unwrap();
        let file_size = file.metadata().await?.len() as usize;
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.put_bytes(0, file_size);
        file.read_exact(&mut buffer).await?;
        let data = rmp_serde::from_slice::<SystemInfo>(&buffer)
            .with_context(|| "Failed to deserialize system info")
            .map_err(IggyError::CannotDeserializeResource)?;
        system_info.version = data.version;
        system_info.migrations = data.migrations;
        Ok(())
    }

    async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError> {
        let data = rmp_serde::to_vec(&system_info)
            .with_context(|| "Failed to serialize system info")
            .map_err(IggyError::CannotSerializeResource)?;
        self.persister
            .overwrite(system_info.path.as_str(), &data)
            .await?;
        info!("Saved system info, {}", system_info);
        Ok(())
    }

    async fn delete(&self, system_info: &SystemInfo) -> Result<(), IggyError> {
        self.persister.delete(system_info.path.as_str()).await?;
        info!("Deleted system info");
        Ok(())
    }
}
