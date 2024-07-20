use crate::streaming::utils::file;
use iggy::error::IggyError;
use monoio::buf::IoBuf;
use std::fmt::Debug;
use tracing::error;

pub trait Persister {
    async fn append<T: IoBuf + Sized>(&self, path: &str, bytes: T) -> Result<(), IggyError>;
    async fn overwrite<T: IoBuf + Sized>(&self, path: &str, bytes: T) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub enum PersistenceStorage {
    File,
    FileWithSync,
    #[cfg(test)]
    Test,
}

impl Persister for PersistenceStorage {
    async fn append<T: IoBuf + Sized>(&self, path: &str, bytes: T) -> Result<(), IggyError> {
        match self {
            PersistenceStorage::File => {
                let file = file::append(path).await?;
                let stat = file::metadata(path).await?;

                file.write_all_at(bytes, stat.len()).await.0?;
                Ok(())
            }
            PersistenceStorage::FileWithSync => {
                let file = file::append(path).await?;
                let stat = file::metadata(path).await?;

                file.write_all_at(bytes, stat.len()).await.0?;
                file.sync_all().await?;
                Ok(())
            }
            #[cfg(test)]
            PersistenceStorage::Test => Ok(()),
        }
    }

    async fn overwrite<T: IoBuf + Sized>(&self, path: &str, bytes: T) -> Result<(), IggyError> {
        match self {
            PersistenceStorage::File => {
                let file = file::overwrite(path).await?;
                let stat = file::metadata(path).await?;

                file.write_all_at(bytes, stat.len()).await.0?;
                Ok(())
            }
            PersistenceStorage::FileWithSync => {
                let file = file::overwrite(path).await?;
                let stat = file::metadata(path).await?;

                file.write_all_at(bytes, stat.len()).await.0?;
                file.sync_all().await?;
                Ok(())
            }
            #[cfg(test)]
            PersistenceStorage::Test => Ok(()),
        }
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        match self {
            PersistenceStorage::File => {
                std::fs::remove_file(path)?;
                Ok(())
            }
            PersistenceStorage::FileWithSync => {
                std::fs::remove_file(path)?;
                Ok(())
            }
            #[cfg(test)]
            PersistenceStorage::Test => Ok(()),
        }
    }
}
