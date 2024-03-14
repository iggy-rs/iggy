use crate::streaming::utils::file;
use async_trait::async_trait;
use iggy::error::IggyError;
use std::fmt::Debug;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[async_trait]
pub trait Persister: Sync + Send {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError>;
    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
}

impl Debug for dyn Persister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persister")
            .field("type", &"Persister")
            .finish()
    }
}

#[derive(Debug)]
pub struct FilePersister;

#[derive(Debug)]
pub struct FileWithSyncPersister;

unsafe impl Send for FilePersister {}
unsafe impl Sync for FilePersister {}

unsafe impl Send for FileWithSyncPersister {}
unsafe impl Sync for FileWithSyncPersister {}

#[async_trait]
impl Persister for FilePersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path).await?;
        file.write_all(bytes).await?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path).await?;
        file.write_all(bytes).await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path).await?;
        Ok(())
    }
}

#[async_trait]
impl Persister for FileWithSyncPersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path).await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path).await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path).await?;
        Ok(())
    }
}
