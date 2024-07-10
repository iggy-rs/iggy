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
        let path = Path::new(&self.config.path).join(file);
        Ok(path.exists())
    }

    async fn archive(&self, files: &[&str]) -> Result<(), ServerError> {
        info!("Archiving files on disk: {:?}", files);
        for file in files {
            info!("Archiving file: {file}");
            let source = Path::new(file);
            let destination = Path::new(&self.config.path).join(source);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            fs::create_dir_all(destination.parent().unwrap()).await?;
            fs::copy(source, destination).await?;
            info!("Archived file: {file} at: {destination_path}");
        }

        Ok(())
    }
}
