use crate::streaming::persistence::COMPONENT;
use crate::streaming::utils::file;
use async_trait::async_trait;
use error_set::ErrContext;
use iggy::error::IggyError;
use std::fmt::Debug;
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[cfg(test)]
use mockall::automock;

#[derive(Debug)]
pub enum PersisterKind {
    File(FilePersister),
    FileWithSync(FileWithSyncPersister),
    #[cfg(test)]
    Mock(MockPersister),
}

impl PersisterKind {
    pub async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.append(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.append(path, bytes).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.append(path, bytes).await,
        }
    }

    pub async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.overwrite(path, bytes).await,
            PersisterKind::FileWithSync(p) => p.overwrite(path, bytes).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.overwrite(path, bytes).await,
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), IggyError> {
        match self {
            PersisterKind::File(p) => p.delete(path).await,
            PersisterKind::FileWithSync(p) => p.delete(path).await,
            #[cfg(test)]
            PersisterKind::Mock(p) => p.delete(path).await,
        }
    }
}

#[async_trait]
#[cfg_attr(test, automock)]
pub trait Persister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError>;
    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct FilePersister;

#[derive(Debug)]
pub struct FileWithSyncPersister;

#[async_trait]
impl Persister for FilePersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to append to file: {path}"))
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to write data to file: {path}"))
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to overwrite file: {path}"))
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to write data to file: {path}"))
            .map_err(|_| IggyError::CannotWriteToFile)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to delete file: {path}"))
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}

#[async_trait]
impl Persister for FileWithSyncPersister {
    async fn append(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::append(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to append to file: {path}"))
            .map_err(|_| IggyError::CannotAppendToFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to write data to file: {path}"))
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error_context(|_| {
                format!("{COMPONENT} - failed to sync file after appending: {path}")
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        let mut file = file::overwrite(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to overwrite file: {path}"))
            .map_err(|_| IggyError::CannotOverwriteFile)?;
        file.write_all(bytes)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to write data to file: {path}"))
            .map_err(|_| IggyError::CannotWriteToFile)?;
        file.sync_all()
            .await
            .with_error_context(|_| {
                format!("{COMPONENT} - failed to sync file after overwriting: {path}")
            })
            .map_err(|_| IggyError::CannotSyncFile)?;
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path)
            .await
            .with_error_context(|_| format!("{COMPONENT} - failed to delete file: {path}"))
            .map_err(|_| IggyError::CannotDeleteFile)?;
        Ok(())
    }
}
