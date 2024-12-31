use crate::streaming::persistence::persister::Persister;
use crate::streaming::storage::SystemInfoStorage;
use crate::streaming::systems::info::SystemInfo;
use crate::streaming::systems::COMPONENT;
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use error_set::ErrContext;
use iggy::error::IggyError;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tracing::info;

#[derive(Debug)]
pub struct FileSystemInfoStorage {
    persister: Arc<dyn Persister>,
    path: String,
}

impl FileSystemInfoStorage {
    pub fn new(path: String, persister: Arc<dyn Persister>) -> Self {
        Self { path, persister }
    }
}
unsafe impl Send for FileSystemInfoStorage {}
unsafe impl Sync for FileSystemInfoStorage {}

#[async_trait]
impl SystemInfoStorage for FileSystemInfoStorage {
    async fn load(&self) -> Result<SystemInfo, IggyError> {
        let file = file::open(&self.path).await;
        if file.is_err() {
            return Err(IggyError::ResourceNotFound(self.path.to_owned()));
        }

        let mut file = file.unwrap();
        let file_size = file
            .metadata()
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to retrieve metadata for file at path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as usize;
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.put_bytes(0, file_size);
        file.read_exact(&mut buffer)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to read file content from path: {}",
                    self.path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        bincode::deserialize(&buffer)
            .with_context(|| "Failed to deserialize system info")
            .map_err(|_| IggyError::CannotDeserializeResource)
    }

    async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError> {
        let data = bincode::serialize(&system_info)
            .with_context(|| "Failed to serialize system info")
            .map_err(|_| IggyError::CannotSerializeResource)?;
        self.persister
            .overwrite(&self.path, &data)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to overwrite file at path: {}",
                    self.path
                )
            })?;
        info!("Saved system info, {}", system_info);
        Ok(())
    }
}
