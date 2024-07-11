use crate::archiver::Archiver;
use crate::configs::server::S3ArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use tracing::info;

#[derive(Debug)]
pub struct S3Archiver {
    _config: S3ArchiverConfig,
}

impl S3Archiver {
    pub fn new(config: S3ArchiverConfig) -> Self {
        S3Archiver { _config: config }
    }
}

#[async_trait]
impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ServerError> {
        Ok(())
    }

    async fn is_archived(&self, _file: &str) -> Result<bool, ServerError> {
        // TODO: Implement checking if file is archived on S3
        Ok(false)
    }

    async fn archive(
        &self,
        _files: &[&str],
        _base_directory: Option<String>,
    ) -> Result<(), ServerError> {
        // TODO: Implement archiving file on S3
        info!("Archiving files on S3...");
        Ok(())
    }
}
