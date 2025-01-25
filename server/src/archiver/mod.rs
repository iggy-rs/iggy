pub mod disk;
pub mod s3;

use crate::configs::server::{DiskArchiverConfig, S3ArchiverConfig};
use crate::server_error::ArchiverError;
use async_trait::async_trait;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;

use crate::archiver::disk::DiskArchiver;
use crate::archiver::s3::S3Archiver;

pub const COMPONENT: &str = "ARCHIVER";

#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ArchiverKindType {
    #[default]
    #[display("disk")]
    Disk,
    #[display("s3")]
    S3,
}

impl FromStr for ArchiverKindType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disk" => Ok(ArchiverKindType::Disk),
            "s3" => Ok(ArchiverKindType::S3),
            _ => Err(format!("Unknown archiver kind: {}", s)),
        }
    }
}

#[async_trait]
pub trait Archiver {
    async fn init(&self) -> Result<(), ArchiverError>;
    async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError>;
    async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ArchiverError>;
}

#[derive(Debug)]
pub enum ArchiverKind {
    Disk(DiskArchiver),
    S3(S3Archiver),
}

impl ArchiverKind {
    pub fn get_disk_arhiver(config: DiskArchiverConfig) -> Self {
        Self::Disk(DiskArchiver::new(config))
    }

    pub fn get_s3_archiver(config: S3ArchiverConfig) -> Result<Self, ArchiverError> {
        let archiver = S3Archiver::new(config)?;
        Ok(Self::S3(archiver))
    }

    pub async fn init(&self) -> Result<(), ArchiverError> {
        match self {
            Self::Disk(a) => a.init().await,
            Self::S3(a) => a.init().await,
        }
    }

    pub async fn is_archived(
        &self,
        file: &str,
        base_directory: Option<String>,
    ) -> Result<bool, ArchiverError> {
        match self {
            Self::Disk(d) => d.is_archived(file, base_directory).await,
            Self::S3(d) => d.is_archived(file, base_directory).await,
        }
    }

    pub async fn archive(
        &self,
        files: &[&str],
        base_directory: Option<String>,
    ) -> Result<(), ArchiverError> {
        match self {
            Self::Disk(d) => d.archive(files, base_directory).await,
            Self::S3(d) => d.archive(files, base_directory).await,
        }
    }
}
