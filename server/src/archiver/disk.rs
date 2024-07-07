use crate::archiver::{ArchivableSegment, Archiver};
use crate::configs::system::DiskArchiverConfig;
use crate::server_error::ServerError;
use async_trait::async_trait;
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
    async fn is_archived(
        &self,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        segment_start_offset: u64,
    ) -> Result<bool, ServerError> {
        // TODO: Implement checking if segment is archived on disk
        Ok(false)
    }

    async fn archive(&self, segment: ArchivableSegment) -> Result<(), ServerError> {
        // TODO: Implement archiving segment on disk
        info!("Archiving segment on disk: {segment}");
        Ok(())
    }
}
