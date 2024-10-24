use crate::archiver::Archiver;
use crate::configs::server::DiskArchiverConfig;
use crate::server_error::ServerArchiverError;
use async_trait::async_trait;
use error_set::ResultContext;
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
    async fn init(&self) -> Result<(), ServerArchiverError> {
        if !Path::new(&self.config.path).exists() {
            info!("Creating disk archiver directory: {}", self.config.path);
            fs::create_dir_all(&self.config.path)
                .await
                .with_error(|_| format!("Failed to create directory: {}", self.config.path))?;
        }
        Ok(())
    }

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ServerArchiverError> {
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
    ) -> Result<(), ServerArchiverError> {
        debug!("Archiving files on disk: {:?}", files);
        for file in files {
            debug!("Archiving file: {file}");
            let source = Path::new(file);
            if !source.exists() {
                return Err(ServerArchiverError::FileToArchiveNotFound {
                    file_path: file.to_string(),
                });
            }

            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&self.config.path).join(base_directory).join(file);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            fs::create_dir_all(destination.parent().unwrap())
                .await
                .with_error(|_| {
                    format!("Failed to create directory for file: {file} at: {destination_path}",)
                })?;
            fs::copy(source, destination).await.with_error(|_| {
                format!("Failed to copy file: {file} to destination: {destination_path}")
            })?;
            debug!("Archived file: {file} at: {destination_path}");
        }

        Ok(())
    }
}
