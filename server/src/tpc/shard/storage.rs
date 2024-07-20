use crate::streaming::persistence::persister::PersistenceStorage;
use crate::streaming::storage::SystemInfoStorage;
use crate::streaming::utils::file;
use crate::streaming::{persistence::persister::Persister, utils::file::metadata};
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use std::os::unix::fs::MetadataExt;
use std::rc::Rc;
use tracing::info;

use super::info::SystemInfo;

const KEY: &str = "system";

#[derive(Debug)]
pub struct FileSystemInfoStorage {
    persister: Rc<PersistenceStorage>,
    path: String,
}

impl FileSystemInfoStorage {
    pub fn new(path: String, persister: Rc<PersistenceStorage>) -> Self {
        Self { path, persister }
    }
}

impl SystemInfoStorage for FileSystemInfoStorage {
    async fn load(&self) -> Result<SystemInfo, IggyError> {
        let file = file::open(&self.path).await;
        if file.is_err() {
            return Err(IggyError::ResourceNotFound(self.path.to_owned()));
        }

        let file = file.unwrap();
        let metadata = metadata(&self.path).await?;
        let file_size = metadata.size() as usize;
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.put_bytes(0, file_size);
        let (result, buffer) = file.read_exact_at(buffer, 0).await;
        result?;
        bincode::deserialize(&buffer)
            .with_context(|| "Failed to deserialize system info")
            .map_err(IggyError::CannotDeserializeResource)
    }

    async fn save(&self, system_info: &SystemInfo) -> Result<(), IggyError> {
        let data = bincode::serialize(&system_info)
            .with_context(|| "Failed to serialize system info")
            .map_err(IggyError::CannotSerializeResource)?;
        self.persister.overwrite(&self.path, data).await?;
        info!("Saved system info, {}", system_info);
        Ok(())
    }
}
