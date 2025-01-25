pub mod disk;
pub mod s3;

use crate::configs::server::{DiskArchiverConfig, S3ArchiverConfig};
use crate::server_error::ArchiverError;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::str::FromStr;

use crate::archiver::disk::DiskArchiver;
use crate::archiver::s3::S3Archiver;

pub const COMPONENT: &str = "ARCHIVER";

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

macro_rules! forward_async_methods {
    (
        $(
            fn $method_name:ident(
                &self $(, $arg:ident : $arg_ty:ty )*
            ) -> $ret:ty ;
        )*
    ) => {
        $(
            pub async fn $method_name(&self, $( $arg: $arg_ty ),* ) -> $ret {
                match self {
                    Self::Disk(d) => d.$method_name($( $arg ),*).await,
                    Self::S3(s) => s.$method_name($( $arg ),*).await,
                }
            }
        )*
    }
}

#[derive(Debug)]
pub enum Archiver {
    Disk(DiskArchiver),
    S3(S3Archiver),
}

impl Archiver {
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

    forward_async_methods! {
        fn is_archived(&self, file: &str, base_directory: Option<String>) -> Result<bool, ArchiverError>;
        fn archive(&self, files: &[&str], base_directory: Option<String>) -> Result<(), ArchiverError>;
    }
}
