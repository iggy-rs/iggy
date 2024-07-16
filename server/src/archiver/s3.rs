use crate::archiver::Archiver;
use crate::configs::server::S3ArchiverConfig;
use crate::server_error::ServerError;
use crate::streaming::utils::file;
use async_trait::async_trait;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use std::path::Path;
use tokio::fs;
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct S3Archiver {
    bucket: Bucket,
    tmp_upload_dir: String,
}

impl S3Archiver {
    pub fn new(config: S3ArchiverConfig) -> Result<Self, ServerError> {
        let credentials = Credentials::new(
            Some(&config.key_id),
            Some(&config.key_secret),
            None,
            None,
            None,
        )
        .map_err(|_| ServerError::InvalidS3Credentials)?;

        let bucket = Bucket::new(
            &config.bucket,
            Region::Custom {
                endpoint: config
                    .endpoint
                    .map(|e| e.to_owned())
                    .unwrap_or("".to_owned())
                    .to_owned(),
                region: config
                    .region
                    .map(|r| r.to_owned())
                    .unwrap_or("".to_owned())
                    .to_owned(),
            },
            credentials,
        )
        .map_err(|_| ServerError::CannotInitializeS3Archiver)?;
        Ok(Self {
            bucket,
            tmp_upload_dir: config.tmp_upload_dir,
        })
    }

    async fn copy_file_to_tmp(&self, path: &str) -> Result<String, ServerError> {
        debug!(
            "Copying file: {path} to temporary S3 upload directory: {}",
            self.tmp_upload_dir
        );
        let source = Path::new(path);
        let destination = Path::new(&self.tmp_upload_dir).join(path);
        let destination_path = destination.to_str().unwrap_or_default().to_owned();
        debug!("Creating temporary S3 upload directory: {destination_path}");
        fs::create_dir_all(destination.parent().unwrap()).await?;
        debug!("Copying file: {path} to temporary S3 upload path: {destination_path}");
        fs::copy(source, destination).await?;
        debug!("File: {path} copied to temporary S3 upload path: {destination_path}");
        Ok(destination_path)
    }
}

#[async_trait]
impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ServerError> {
        let response = self.bucket.list("/".to_string(), None).await;
        if let Err(error) = response {
            error!("Cannot initialize S3 archiver: {error}");
            return Err(ServerError::CannotInitializeS3Archiver);
        }

        if Path::new(&self.tmp_upload_dir).exists() {
            info!(
                "Removing existing S3 archiver temporary upload directory: {}",
                self.tmp_upload_dir
            );
            fs::remove_dir_all(&self.tmp_upload_dir).await?;
        }
        info!(
            "Creating S3 archiver temporary upload directory: {}",
            self.tmp_upload_dir
        );
        fs::create_dir_all(&self.tmp_upload_dir).await?;
        Ok(())
    }

    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ServerError> {
        debug!("Checking if file: {file} is archived on S3.");
        let base_directory = base_directory.as_deref().unwrap_or_default();
        let destination = Path::new(&base_directory).join(file);
        let destination_path = destination.to_str().unwrap_or_default().to_owned();
        let response = self.bucket.get_object_tagging(destination_path).await;
        if response.is_err() {
            debug!("File: {file} is not archived on S3.");
            return Ok(false);
        }

        let (_, status) = response.unwrap();
        if status == 200 {
            debug!("File: {file} is archived on S3.");
            return Ok(true);
        }

        debug!("File: {file} is not archived on S3.");
        Ok(false)
    }

    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ServerError> {
        for path in files {
            if !Path::new(path).exists() {
                return Err(ServerError::FileToArchiveNotFound(path.to_string()));
            }

            let source = self.copy_file_to_tmp(path).await?;
            debug!("Archiving file: {source} on S3.");
            let mut file = file::open(&source).await?;
            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&base_directory).join(path);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            let response = self
                .bucket
                .put_object_stream(&mut file, destination_path)
                .await;
            if let Err(error) = response {
                error!("Cannot archive file: {path} on S3: {}", error);
                fs::remove_file(&source).await?;
                return Err(ServerError::CannotArchiveFile(path.to_string()));
            }

            let response = response.unwrap();
            let status = response.status_code();
            if status == 200 {
                debug!("Archived file: {path} on S3.");
                fs::remove_file(&source).await?;
                continue;
            }

            error!("Cannot archive file: {path} on S3, received an invalid status code: {status}.");
            fs::remove_file(&source).await?;
            return Err(ServerError::CannotArchiveFile(path.to_string()));
        }
        Ok(())
    }
}
