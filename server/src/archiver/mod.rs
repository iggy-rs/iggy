pub mod disk;
pub mod s3;

use crate::server_error::ServerError;
use async_trait::async_trait;
use derive_more::Display;
use iggy::utils::byte_size::IggyByteSize;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ArchiverKind {
    #[default]
    #[display(fmt = "disk")]
    Disk,
    #[display(fmt = "s3")]
    S3,
}

impl FromStr for ArchiverKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disk" => Ok(ArchiverKind::Disk),
            "s3" => Ok(ArchiverKind::S3),
            _ => Err(format!("Unknown archiver kind: {}", s)),
        }
    }
}

#[async_trait]
pub trait Archiver: Sync + Send {
    async fn archive(&self, segment: ArchivableSegment) -> Result<(), ServerError>;
}

impl Debug for dyn Archiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Archiver")
    }
}

#[derive(Debug)]
pub struct ArchivableSegment {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub current_offset: u64,
    pub index_path: String,
    pub log_path: String,
    pub time_index_path: String,
    pub size: IggyByteSize,
}

impl Display for ArchivableSegment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Stream ID: {}, Topic ID: {}, Partition ID: {}, Start Offset: {}, End Offset: {}, Current Offset: {}, Index Path: {}, Log Path: {}, Time Index Path: {}, Size: {}",
            self.stream_id, self.topic_id, self.partition_id, self.start_offset, self.end_offset, self.current_offset, self.index_path, self.log_path, self.time_index_path, self.size
        )
    }
}
