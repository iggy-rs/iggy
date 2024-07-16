use crate::archiver::Archiver;
use crate::configs::server::DiskArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use std::path::Path;
use tokio::fs;
use tracing::{debug, info};

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

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ServerError> {
        debug!("Checking if file: {file} is archived on disk.");
        let base_directory = base_directory.as_deref().unwrap_or_default();
        let path = Path::new(&self.config.path).join(base_directory).join(file);
        let is_archived = path.exists();
        debug!("File: {file} is archived: {is_archived}");
        Ok(is_archived)
    }

    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ServerError> {
        debug!("Archiving files on disk: {:?}", files);
        for file in files {
            debug!("Archiving file: {file}");
            let source = Path::new(file);
            if !source.exists() {
                return Err(ServerError::FileToArchiveNotFound(file.to_string()));
            }

            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&self.config.path).join(base_directory).join(file);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            fs::create_dir_all(destination.parent().unwrap()).await?;
            fs::copy(source, destination).await?;
            debug!("Archived file: {file} at: {destination_path}");
        }

        Ok(())
    }
}
