use crate::archiver::{Archiver, COMPONENT};
use crate::configs::server::DiskArchiverConfig;
use crate::server_error::ArchiverError;
use error_set::ErrContext;
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

impl Archiver for DiskArchiver {
    async fn init(&self) -> Result<(), ArchiverError> {
        if !Path::new(&self.config.path).exists() {
            info!("Creating disk archiver directory: {}", self.config.path);
            fs::create_dir_all(&self.config.path)
                .await
                .with_error_context(|err| {
                    format!(
                        "ARCHIVER - failed to create directory: {} with error: {err}",
                        self.config.path
                    )
                })?;
        }
        Ok(())
    }

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError> {
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
    ) -> Result<(), ArchiverError> {
        debug!("Archiving files on disk: {:?}", files);
        for file in files {
            debug!("Archiving file: {file}");
            let source = Path::new(file);
            if !source.exists() {
                return Err(ArchiverError::FileToArchiveNotFound {
                    file_path: file.to_string(),
                });
            }

            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&self.config.path).join(base_directory).join(file);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            fs::create_dir_all(destination.parent().unwrap())
                .await
                .with_error_context(|err| {
                    format!("{COMPONENT} - failed to create file: {file} at path: {destination_path} with error: {err}",)
                })?;
            fs::copy(source, destination).await.with_error_context(|err| {
                format!("{COMPONENT} - failed to copy file: {file} to destination: {destination_path} with error: {err}")
            })?;
            debug!("Archived file: {file} at: {destination_path}");
        }

        Ok(())
    }
}
