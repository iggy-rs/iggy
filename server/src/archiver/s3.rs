use crate::archiver::{Archiver, COMPONENT};
use crate::configs::server::S3ArchiverConfig;
use crate::server_error::ArchiverError;
use crate::streaming::utils::file;
use async_trait::async_trait;
use error_set::ResultContext;
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
    pub fn new(config: S3ArchiverConfig) -> Result<Self, ArchiverError> {
        let credentials = Credentials::new(
            Some(&config.key_id),
            Some(&config.key_secret),
            None,
            None,
            None,
        )
        .map_err(|_| ArchiverError::InvalidS3Credentials)?;

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
        .map_err(|_| ArchiverError::CannotInitializeS3Archiver)?;
        Ok(Self {
            bucket,
            tmp_upload_dir: config.tmp_upload_dir,
        })
    }

    async fn copy_file_to_tmp(&self, path: &str) -> Result<String, ArchiverError> {
        debug!(
            "Copying file: {path} to temporary S3 upload directory: {}",
            self.tmp_upload_dir
        );
        let source = Path::new(path);
        let destination = Path::new(&self.tmp_upload_dir).join(path);
        let destination_path = destination.to_str().unwrap_or_default().to_owned();
        debug!("Creating temporary S3 upload directory: {destination_path}");
        fs::create_dir_all(destination.parent().unwrap())
            .await
            .with_error(|err| {
                format!(
                    "{COMPONENT} - failed to create temporary S3 upload directory for path: {destination_path} with error: {err}"
                )
            })?;
        debug!("Copying file: {path} to temporary S3 upload path: {destination_path}");
        fs::copy(source, &destination).await.with_error(|err| {
            format!("{COMPONENT} - failed to copy file: {path} to temporary S3 upload path: {destination_path} with error: {err}")
        })?;
        debug!("File: {path} copied to temporary S3 upload path: {destination_path}");
        Ok(destination_path)
    }
}

#[async_trait]
impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ArchiverError> {
        let response = self.bucket.list("/".to_string(), None).await;
        if let Err(error) = response {
            error!("Cannot initialize S3 archiver: {error}");
            return Err(ArchiverError::CannotInitializeS3Archiver);
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
    ) -> Result<bool, ArchiverError> {
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
    ) -> Result<(), ArchiverError> {
        for path in files {
            if !Path::new(path).exists() {
                return Err(ArchiverError::FileToArchiveNotFound {
                    file_path: path.to_string(),
                });
            }

            let source = self.copy_file_to_tmp(path).await?;
            debug!("Archiving file: {source} on S3.");
            let mut file = file::open(&source)
                .await
                .with_error(|err| format!("{COMPONENT} - failed to open source file: {source} for archiving with error: {err}"))?;
            let base_directory = base_directory.as_deref().unwrap_or_default();
            let destination = Path::new(&base_directory).join(path);
            let destination_path = destination.to_str().unwrap_or_default().to_owned();
            let response = self
                .bucket
                .put_object_stream(&mut file, destination_path)
                .await;
            if let Err(error) = response {
                error!("Cannot archive file: {path} on S3: {}", error);
                fs::remove_file(&source).await.with_error(|err| {
                    format!("{COMPONENT} - failed to remove temporary file: {source} after S3 failure with error: {err}")
                })?;
                return Err(ArchiverError::CannotArchiveFile {
                    file_path: path.to_string(),
                });
            }

            let response = response.unwrap();
            let status = response.status_code();
            if status == 200 {
                debug!("Archived file: {path} on S3.");
                fs::remove_file(&source).await.with_error(|err| {
                    format!("{COMPONENT} - failed to remove temporary file: {source} after successful archive with error: {err}")
                })?;
                continue;
            }

            error!("Cannot archive file: {path} on S3, received an invalid status code: {status}.");
            fs::remove_file(&source).await.with_error(|err| {
                format!("{COMPONENT} - failed to remove temporary file: {source} after invalid status code with error: {err}")
            })?;
            return Err(ArchiverError::CannotArchiveFile {
                file_path: path.to_string(),
            });
        }
        Ok(())
    }
}
