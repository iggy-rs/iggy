use crate::archiver::{ArchivableSegment, Archiver};
use crate::configs::server::S3ArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
use tracing::info;

#[derive(Debug)]
pub struct S3Archiver {
    config: S3ArchiverConfig,
}

impl S3Archiver {
    pub fn new(config: S3ArchiverConfig) -> Self {
        S3Archiver { config }
    }
}

#[async_trait]
impl Archiver for S3Archiver {
    async fn init(&self) -> Result<(), ServerError> {
        Ok(())
    }

    async fn is_archived(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        segment_start_offset: u64,
    ) -> Result<bool, ServerError> {
        // TODO: Implement checking if segment is archived on S3
        Ok(false)
    }

    async fn archive(&self, segment: ArchivableSegment) -> Result<(), ServerError> {
        // TODO: Implement archiving segment on S3
        info!("Archiving segment on S3: {segment}");
        Ok(())
    }
}
