use async_trait::async_trait;
use flume::{unbounded, Receiver, Sender};
use iggy::{error::IggyError, models::messages::Message};
use std::sync::Arc;
use tokio::{
    fs::{self, File},
    io::{AsyncWriteExt, BufWriter},
    sync::oneshot::Sender as OneshotSender,
    task,
};
use tracing::{error, warn};

use crate::streaming::utils::file;

use super::paths::*;

#[async_trait]
pub trait SegmentPersister {
    async fn append(
        &self,
        messages: Vec<Arc<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError>;
    async fn create(&self, path: &str) -> Result<(), IggyError>;
    async fn delete(&self) -> Result<(), IggyError>;
}

#[derive(Debug)]
pub struct WithoutSync {
    join_handle: task::JoinHandle<()>,
    sender: Sender<FilePersisterCommand>,
}

impl WithoutSync {
    pub fn new(path: String) -> Self {
        let (sender, receiver): (Sender<FilePersisterCommand>, Receiver<FilePersisterCommand>) =
            unbounded();

        // TODO: perhaps some synchronization is needed here

        let join_handle = task::spawn(async move {
            let log_path = get_log_path(&path);
            let index_path = get_index_path(&path);
            let time_index_path = get_time_index_path(&path);
            std::thread::sleep(std::time::Duration::from_millis(10));

            warn!("opening log: {}", log_path);

            let log_file = file::append(&log_path).await.unwrap();
            let index_path = file::append(&index_path).await.unwrap();
            let time_index_file = file::append(&time_index_path).await.unwrap();

            let mut log_writer = BufWriter::new(log_file);
            let mut index_writer = BufWriter::new(index_path);
            let mut time_index_writer = BufWriter::new(time_index_file);

            while let Ok(file_operation) = receiver.recv_async().await {
                match file_operation {
                    FilePersisterCommand::Append(messages, current_position) => {
                        let mut current_position = current_position as u32;

                        for message in messages {
                            message.write_to(&mut log_writer).await.unwrap();
                            time_index_writer
                                .write_u64_le(message.timestamp)
                                .await
                                .unwrap();
                            index_writer.write_u32_le(current_position).await.unwrap();
                            current_position += message.get_size_bytes();
                        }

                        log_writer.flush().await.unwrap();
                        time_index_writer.flush().await.unwrap();
                        index_writer.flush().await.unwrap();
                    }
                    FilePersisterCommand::DeleteAndTerminate(completion_sender) => {
                        log_writer.flush().await.unwrap();
                        time_index_writer.flush().await.unwrap();
                        index_writer.flush().await.unwrap();
                        let result = fs::remove_file(&path).await.map_err(|err| {
                            error!("Failed to delete file {}. Error: {}", path, err);
                            IggyError::from(err)
                        });

                        let _ = completion_sender.send(result);
                        break;
                    }
                    FilePersisterCommand::Terminate => {
                        error!("File persister terminated path: {}", path);
                        log_writer.flush().await.unwrap();
                        time_index_writer.flush().await.unwrap();
                        index_writer.flush().await.unwrap();
                        break;
                    }
                }
            }
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        WithoutSync {
            sender,
            join_handle,
        }
    }

    pub async fn delete_and_terminate(&self) -> Result<(), IggyError> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let command = FilePersisterCommand::DeleteAndTerminate(sender);
        self.sender.send_async(command).await.unwrap();
        receiver.await.unwrap()
    }

    pub async fn terminate(self) {
        self.sender
            .send_async(FilePersisterCommand::Terminate)
            .await
            .unwrap();
        self.join_handle.await.unwrap();
    }
}

#[derive(Debug)]
enum FilePersisterCommand {
    Append(Vec<Arc<Message>>, u64),
    DeleteAndTerminate(OneshotSender<Result<(), IggyError>>),
    Terminate,
}

#[async_trait]
impl SegmentPersister for WithoutSync {
    async fn append(
        &self,
        messages: Vec<Arc<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError> {
        let file_operation = FilePersisterCommand::Append(messages, current_offset);

        self.sender.send_async(file_operation).await.unwrap();
        Ok(())
    }

    async fn create(&self, path: &str) -> Result<(), IggyError> {
        let log_path = get_log_path(path);
        let index_path = get_index_path(path);
        let time_index_path = get_time_index_path(path);

        warn!("Creating log file: {}", log_path);

        let log_file = File::create(log_path).await.unwrap();
        let index_path = File::create(index_path).await.unwrap();
        let time_index_file = File::create(time_index_path).await.unwrap();

        Ok(())
    }

    async fn delete(&self) -> Result<(), IggyError> {
        println!("Deleting without sync");
        Ok(())
    }
}

#[derive(Debug)]
pub struct WithSync {
    pub path: String,
}

#[async_trait]
impl SegmentPersister for WithSync {
    async fn append(
        &self,
        messages: Vec<Arc<Message>>,
        current_position: u64,
    ) -> Result<(), IggyError> {
        let mut log_file: fs::File = file::write(&get_log_path(&self.path)).await.unwrap();
        let mut indices_file = file::write(&get_index_path(&self.path)).await.unwrap();
        let mut time_indices_file = file::write(&get_time_index_path(&self.path)).await.unwrap();

        for message in messages {
            message.write_to(&mut log_file).await.unwrap();
            time_indices_file
                .write_u64_le(message.timestamp)
                .await
                .unwrap();
            indices_file
                .write_u32_le(current_position as u32)
                .await
                .unwrap();
        }

        log_file.flush().await.unwrap();
        time_indices_file.flush().await.unwrap();
        indices_file.flush().await.unwrap();

        log_file.sync_all().await.unwrap();
        time_indices_file.sync_all().await.unwrap();
        indices_file.sync_all().await.unwrap();

        Ok(())
    }

    async fn create(&self, path: &str) -> Result<(), IggyError> {
        let log_file = file::write(&get_log_path(&self.path)).await.unwrap();
        let indices_file = file::write(&get_index_path(&self.path)).await.unwrap();
        let time_indices_file = file::write(&get_time_index_path(&self.path)).await.unwrap();

        log_file.sync_all().await.unwrap();
        indices_file.sync_all().await.unwrap();
        time_indices_file.sync_all().await.unwrap();
        Ok(())
    }

    async fn delete(&self) -> Result<(), IggyError> {
        fs::remove_file(&get_log_path(&self.path))
            .await
            .map_err(|err| {
                error!("Failed to delete file {}. Error: {}", self.path, err);
                IggyError::from(err)
            })?;

        fs::remove_file(&get_index_path(&self.path))
            .await
            .map_err(|err| {
                error!("Failed to delete file {}. Error: {}", self.path, err);
                IggyError::from(err)
            })?;

        fs::remove_file(&get_time_index_path(&self.path))
            .await
            .map_err(|err| {
                error!("Failed to delete file {}. Error: {}", self.path, err);
                IggyError::from(err)
            })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct StoragePersister<P: SegmentPersister> {
    persister: P,
}

impl<P: SegmentPersister> StoragePersister<P> {
    pub fn new(persister: P) -> Self {
        StoragePersister { persister }
    }

    pub async fn append(
        &self,
        messages: Vec<Arc<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError> {
        self.persister.append(messages, current_offset).await
    }

    pub async fn create(&self, path: &str) -> Result<(), IggyError> {
        self.persister.create(path).await
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        self.persister.delete().await
    }
}
