use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::IggyError;

/// Enum representing the different types of system snapshots that can be taken.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SystemSnapshotType {
    /// Overview of the filesystem.
    FilesystemOverview,
    /// List of currently running processes.
    ProcessList,
    /// Resource usage statistics of the system.
    ResourceUsage,
    /// Test snapshot type for development purposes.
    Test,
    /// Server logs
    ServerLogs,
}

/// Enum representing the various compression methods available for snapshots.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum SnapshotCompression {
    /// Store the file as is
    Stored,
    /// Compress the file using Deflate
    #[default]
    Deflated,
    /// Compress the file using BZIP2
    Bzip2,
    /// Compress the file using ZStandard
    Zstd,
    /// Compress the file using LZMA
    Lzma,
    /// Compress the file using XZ
    Xz,
}

impl SystemSnapshotType {
    pub fn as_code(&self) -> u8 {
        match self {
            SystemSnapshotType::FilesystemOverview => 1,
            SystemSnapshotType::ProcessList => 2,
            SystemSnapshotType::ResourceUsage => 3,
            SystemSnapshotType::Test => 4,
            SystemSnapshotType::ServerLogs => 5,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(SystemSnapshotType::FilesystemOverview),
            2 => Ok(SystemSnapshotType::ProcessList),
            3 => Ok(SystemSnapshotType::ResourceUsage),
            4 => Ok(SystemSnapshotType::Test),
            5 => Ok(SystemSnapshotType::ServerLogs),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl fmt::Display for SystemSnapshotType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemSnapshotType::FilesystemOverview => write!(f, "filesystem_overview"),
            SystemSnapshotType::ProcessList => write!(f, "process_list"),
            SystemSnapshotType::ResourceUsage => write!(f, "resource_usage"),
            SystemSnapshotType::Test => write!(f, "test"),
            SystemSnapshotType::ServerLogs => write!(f, "server_logs"),
        }
    }
}

impl FromStr for SystemSnapshotType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "filesystem_overview" => Ok(SystemSnapshotType::FilesystemOverview),
            "process_list" => Ok(SystemSnapshotType::ProcessList),
            "resource_usage" => Ok(SystemSnapshotType::ResourceUsage),
            "test" => Ok(SystemSnapshotType::Test),
            "server_logs" => Ok(SystemSnapshotType::ServerLogs),
            _ => Err(format!("Invalid snapshot type: {}", s)),
        }
    }
}

impl SnapshotCompression {
    pub fn as_code(&self) -> u8 {
        match self {
            SnapshotCompression::Stored => 1,
            SnapshotCompression::Deflated => 2,
            SnapshotCompression::Bzip2 => 3,
            SnapshotCompression::Zstd => 4,
            SnapshotCompression::Lzma => 5,
            SnapshotCompression::Xz => 6,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(SnapshotCompression::Stored),
            2 => Ok(SnapshotCompression::Deflated),
            3 => Ok(SnapshotCompression::Bzip2),
            4 => Ok(SnapshotCompression::Zstd),
            5 => Ok(SnapshotCompression::Lzma),
            6 => Ok(SnapshotCompression::Xz),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl FromStr for SnapshotCompression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stored" => Ok(SnapshotCompression::Stored),
            "deflated" => Ok(SnapshotCompression::Deflated),
            "bzip2" => Ok(SnapshotCompression::Bzip2),
            "zstd" => Ok(SnapshotCompression::Zstd),
            "lzma" => Ok(SnapshotCompression::Lzma),
            "xz" => Ok(SnapshotCompression::Xz),
            _ => Err(format!("Invalid compression type: {}", s)),
        }
    }
}

impl fmt::Display for SnapshotCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotCompression::Stored => write!(f, "stored"),
            SnapshotCompression::Deflated => write!(f, "deflated"),
            SnapshotCompression::Bzip2 => write!(f, "bzip2"),
            SnapshotCompression::Zstd => write!(f, "zstd"),
            SnapshotCompression::Lzma => write!(f, "lzma"),
            SnapshotCompression::Xz => write!(f, "xz"),
        }
    }
}
