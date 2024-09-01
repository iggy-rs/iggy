pub mod disk;
pub mod s3;

use crate::server_error::ServerError;
use async_trait::async_trait;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ArchiverKind {
    #[default]
    #[display("disk")]
    Disk,
    #[display("s3")]
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
    async fn init(&self) -> Result<(), ServerError>;
    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ServerError>;
    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ServerError>;
}

impl Debug for dyn Archiver {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Archiver")
    }
}
