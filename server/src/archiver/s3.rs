use crate::archiver::Archiver;
use crate::configs::server::S3ArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;

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
        todo!("Checking if file is archived on S3")
    }

    async fn archive(
        &self,
        _files: &[&str],
        _base_directory: Option<String>,
    ) -> Result<(), ServerError> {
        todo!("Archiving files on S3")
    }
}
