mod procdump;

use crate::configs::system::SystemConfig;
use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use iggy::error::IggyError;
use iggy::models::snapshot::Snapshot;
use iggy::snapshot::{SnapshotCompression, SystemSnapshotType};
use iggy::utils::duration::IggyDuration;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tempfile::NamedTempFile;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::{error, info};

impl System {
    pub async fn get_snapshot(
        &self,
        session: &Session,
        compression: SnapshotCompression,
        snapshot_types: &Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        self.ensure_authenticated(session)?;

        let snapshot_types = if snapshot_types.contains(&SystemSnapshotType::All) {
            if snapshot_types.len() > 1 {
                error!("When using 'All' snapshot type, no other types can be specified");
                return Err(IggyError::InvalidCommand);
            }
            &SystemSnapshotType::all_snapshot_types()
        } else {
            &snapshot_types
        };

        let cursor = Cursor::new(Vec::new());
        let mut zip_writer = ZipFileWriter::new(cursor.compat_write());

        let compression = match compression {
            SnapshotCompression::Stored => Compression::Stored,
            SnapshotCompression::Deflated => Compression::Deflate,
            SnapshotCompression::Bzip2 => Compression::Bz,
            SnapshotCompression::Lzma => Compression::Lzma,
            SnapshotCompression::Xz => Compression::Xz,
            SnapshotCompression::Zstd => Compression::Zstd,
        };

        info!("Executing snapshot commands: {:?}", snapshot_types);
        let now = Instant::now();

        for snapshot_type in snapshot_types {
            match get_command_result(snapshot_type, self.config.clone()).await {
                Ok(temp_file) => {
                    let filename = format!("{}.txt", snapshot_type);
                    let entry = ZipEntryBuilder::new(filename.clone().into(), compression);

                    let mut file = File::open(temp_file.path()).await.map_err(|e| {
                        error!("Failed to open temporary file: {}", e);
                        IggyError::SnapshotFileCompletionFailed
                    })?;

                    let mut content = Vec::new();
                    if let Err(e) = file.read_to_end(&mut content).await {
                        error!("Failed to read temporary file: {}", e);
                        continue;
                    }
                    info!(
                        "Read {} bytes from temp file for {}",
                        content.len(),
                        filename
                    );

                    if let Err(e) = zip_writer.write_entry_whole(entry, &content).await {
                        error!("Failed to write to snapshot file: {}", e);
                        continue;
                    }
                    info!("Wrote entry {} to zip file", filename);
                }
                Err(e) => {
                    error!(
                        "Failed to execute command for snapshot type {:?}: {}",
                        snapshot_type, e
                    );
                    continue;
                }
            }
        }

        info!(
            "Snapshot commands {:?} finished in {}",
            snapshot_types,
            IggyDuration::new(now.elapsed())
        );

        let writer = zip_writer
            .close()
            .await
            .map_err(|_| IggyError::SnapshotFileCompletionFailed)?;

        let cursor = writer.into_inner();
        let zip_data = cursor.into_inner();

        info!("Final zip size: {} bytes", zip_data.len());
        Ok(Snapshot::new(zip_data))
    }
}

async fn write_command_output_to_temp_file(
    command: &mut Command,
) -> Result<NamedTempFile, std::io::Error> {
    let output = command.output().await?;
    let temp_file = NamedTempFile::new()?;
    let mut file = File::from_std(temp_file.as_file().try_clone()?);
    file.write_all(&output.stdout).await?;
    file.flush().await?;
    Ok(temp_file)
}

async fn get_filesystem_overview() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("ls").args(["-la", "/tmp", "/proc"])).await
}

async fn get_process_info() -> Result<NamedTempFile, std::io::Error> {
    let temp_file = NamedTempFile::new()?;
    let mut file = File::from_std(temp_file.as_file().try_clone()?);

    let ps_output = Command::new("ps").arg("aux").output().await?;
    file.write_all(b"=== Process List (ps aux) ===\n").await?;
    file.write_all(&ps_output.stdout).await?;
    file.write_all(b"\n\n").await?;

    file.write_all(b"=== Detailed Process Information ===\n")
        .await?;
    let proc_info = procdump::get_proc_info().await?;
    file.write_all(proc_info.as_bytes()).await?;
    file.flush().await?;

    Ok(temp_file)
}

async fn get_resource_usage() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("top").args(["-H", "-b", "-n", "1"])).await
}

async fn get_test_snapshot() -> Result<NamedTempFile, std::io::Error> {
    write_command_output_to_temp_file(Command::new("echo").arg("test")).await
}

async fn get_server_logs(config: Arc<SystemConfig>) -> Result<NamedTempFile, std::io::Error> {
    let base_directory = PathBuf::from(config.get_system_path());
    let logs_subdirectory = PathBuf::from(&config.logging.path);
    let logs_path = base_directory.join(logs_subdirectory);

    let list_and_cat = format!(
        r#"ls -tr "{logs}" | xargs -I {{}} cat "{logs}/{{}}" "#,
        logs = logs_path.display()
    );

    write_command_output_to_temp_file(Command::new("sh").args(["-c", &list_and_cat])).await
}

async fn get_server_config(config: Arc<SystemConfig>) -> Result<NamedTempFile, std::io::Error> {
    let base_directory = PathBuf::from(config.get_system_path());
    let config_path = base_directory.join("runtime").join("current_config.toml");

    write_command_output_to_temp_file(Command::new("cat").arg(config_path)).await
}

async fn get_command_result(
    snapshot_type: &SystemSnapshotType,
    config: Arc<SystemConfig>,
) -> Result<NamedTempFile, std::io::Error> {
    match snapshot_type {
        SystemSnapshotType::FilesystemOverview => get_filesystem_overview().await,
        SystemSnapshotType::ProcessList => get_process_info().await,
        SystemSnapshotType::ResourceUsage => get_resource_usage().await,
        SystemSnapshotType::Test => get_test_snapshot().await,
        SystemSnapshotType::ServerLogs => get_server_logs(config).await,
        SystemSnapshotType::ServerConfig => get_server_config(config).await,
        SystemSnapshotType::All => {
            // This should not be reached (we filter out `All`` at the call site)
            unreachable!(
                "SystemSnapshotType::All should be handled before calling get_command_result()."
            )
        }
    }
}
