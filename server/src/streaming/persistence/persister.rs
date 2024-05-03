use crate::streaming::utils::file;
use bytes::Bytes;
use iggy::error::IggyError;
use std::fmt::Debug;

pub trait Persister {
    async fn append(&self, path: &str, bytes: Bytes) -> Result<(), IggyError>;
    async fn overwrite(&self, path: &str, bytes: Bytes) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct FilePersister;

#[derive(Debug)]
pub struct FileWithSyncPersister;

//TODO(numminex) - Maybe change Persister api to take position(u64) as argument, instead of statting the file
impl Persister for FilePersister {
    async fn append(&self, path: &str, bytes: Bytes) -> Result<(), IggyError> {
        let file = file::append(path).await?;
        let stat = file::metadata(path).await?;

        file.write_all_at(bytes, stat.len()).await.0?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: Bytes) -> Result<(), IggyError> {
        let file = file::overwrite(path).await?;
        let stat = file::metadata(path).await?;

        file.write_all_at(bytes, stat.len()).await.0?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        std::fs::remove_file(path)?;
        Ok(())
    }
}

impl Persister for FileWithSyncPersister {
    async fn append(&self, path: &str, bytes: Bytes) -> Result<(), IggyError> {
        let file = file::append(path).await?;
        let stat = file::metadata(path).await?;

        file.write_all_at(bytes, stat.len()).await.0?;
        file.sync_all().await?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: Bytes) -> Result<(), IggyError> {
        let file = file::overwrite(path).await?;
        let stat = file::metadata(path).await?;

        file.write_all_at(bytes, stat.len()).await.0?;
        file.sync_all().await?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum StoragePersister {
    File(FilePersister),
    FileWithSync(FileWithSyncPersister),
}

impl Persister for StoragePersister {
    async fn append(&self, path: &str, bytes: bytes::Bytes) -> Result<(), IggyError> {
        match self {
            StoragePersister::File(persister) => persister.append(path, bytes).await,
            StoragePersister::FileWithSync(persister) => persister.append(path, bytes).await,
        }
    }

    async fn overwrite(&self, path: &str, bytes: bytes::Bytes) -> Result<(), IggyError> {
        match self {
            StoragePersister::File(persister) => persister.overwrite(path, bytes).await,
            StoragePersister::FileWithSync(persister) => persister.overwrite(path, bytes).await,
        }
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        match self {
            StoragePersister::File(persister) => persister.delete(path).await,
            StoragePersister::FileWithSync(persister) => persister.delete(path).await,
        }
    }
}
