use crate::configs::system::SystemConfig;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy::error::IggyError;
use iggy::models::snapshot::Snapshot;
use iggy::snapshot::{SnapshotCompression, SystemSnapshotType};
use std::io::Cursor;
use std::io::Write;
use std::path::PathBuf;
use std::process::Output;
use std::sync::Arc;
use tokio::process::Command;
use tracing::error;
use zip::write::{SimpleFileOptions, ZipWriter};

impl System {
    pub async fn get_snapshot(
        &self,
        session: &Session,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.ensure_authenticated(session)?;

        let mut zip_buffer = Cursor::new(Vec::new());
        let mut zip_writer = ZipWriter::new(&mut zip_buffer);

        let compression = match compression {
            SnapshotCompression::Stored => zip::CompressionMethod::Stored,
            SnapshotCompression::Deflated => zip::CompressionMethod::Deflated,
            SnapshotCompression::Bzip2 => zip::CompressionMethod::Bzip2,
            SnapshotCompression::Lzma => zip::CompressionMethod::Lzma,
            SnapshotCompression::Xz => zip::CompressionMethod::Xz,
            SnapshotCompression::Zstd => zip::CompressionMethod::Zstd,
        };

        for snapshot_type in &snapshot_types {
            match get_command_result(snapshot_type, self.config.clone()).await {
                Ok(out) => {
                    let filename = format!("{}.txt", snapshot_type);
                    if let Err(e) = zip_writer.start_file(
                        &filename,
                        SimpleFileOptions::default().compression_method(compression),
                    ) {
                        error!("Failed to create snapshot file {}: {}", filename, e);
                        continue;
                    }

                    if let Err(e) = zip_writer.write_all(&out.stdout) {
                        error!("Failed to write to snapshot file {}: {}", filename, e);
                        continue;
                    }
                }
                Err(e) => {
                    error!("Failed to execute command: {}", e);
                    continue;
                }
            }
        }

        zip_writer
            .finish()
            .map_err(|_| IggyError::SnapshotFileCompletionFailed)?;
        let zip_data = zip_buffer.into_inner();

        Ok(Snapshot::new(zip_data))
    }
}

async fn get_command_result(
    snapshot_type: &SystemSnapshotType,
    config: Arc<SystemConfig>,
) -> Result<Output, std::io::Error> {
    match snapshot_type {
        SystemSnapshotType::FilesystemOverview => {
            Command::new("ls")
                .args(vec!["-la", "/tmp", "/proc"])
                .output()
                .await
        }
        SystemSnapshotType::ProcessList => Command::new("ps").arg("aux").output().await,
        SystemSnapshotType::ResourceUsage => {
            Command::new("top")
                .args(vec!["-H", "-b", "-n", "1"])
                .output()
                .await
        }
        SystemSnapshotType::Test => Command::new("echo").arg("test").output().await,
        SystemSnapshotType::ServerLogs => {
            let base_directory = PathBuf::from(config.get_system_path());
            let logs_subdirectory = PathBuf::from(&config.logging.path);
            let logs_path = base_directory.join(logs_subdirectory);
            Command::new("sh")
                .args([
                    "-c",
                    &format!(
                        "ls -tr {} | xargs -I {{}} cat {}/{{}}",
                        logs_path.display(),
                        logs_path.display()
                    ),
                ])
                .output()
                .await
        }
    }
}
