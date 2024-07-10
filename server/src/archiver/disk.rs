use crate::archiver::Archiver;
use crate::configs::server::DiskArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tracing::info;

#[derive(Debug)]
pub struct DiskArchiver {
    config: DiskArchiverConfig,
}

impl DiskArchiver {
    pub fn new(config: DiskArchiverConfig) -> Self {
        DiskArchiver { config }
    }
}

#[async_trait]
impl Archiver for DiskArchiver {
    async fn init(&self) -> Result<(), ServerError> {
        if !Path::new(&self.config.path).exists() {
            info!("Creating disk archiver directory: {}", self.config.path);
            fs::create_dir_all(&self.config.path).await?;
        }

        Ok(())
    }

    async fn is_archived(&self, file: &str) -> Result<bool, ServerError> {
        // TODO: Implement checking if file is archived on disk
        Ok(false)
    }

    async fn archive(&self, files: &[&str]) -> Result<(), ServerError> {
        // TODO: Implement archiving file on disk
        info!("Archiving files on disk");
        Ok(())
    }
}
