use crate::streaming::utils::file;
use async_trait::async_trait;
use dashmap::DashMap;
use flume::{unbounded, Receiver, Sender};
use iggy::error::IggyError;
use iggy::models::messages::Message;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender as OneshotSender;
use tokio::{fs, task};
use tracing::{error, info};

// TODO:
// - implement shutdown
// - implement persister per segment

type CurrentPosition = u32;
type MessagesToSave = Vec<Arc<Message>>;

#[async_trait]
pub trait Persister: Sync + Send {
    async fn append(
        &self,
        path: &str,
        messages: Vec<Arc<Message>>,
        segment_size: u32,
    ) -> Result<(), IggyError>;
    async fn create(&self, path: &str) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
    async fn terminate(&self, path: &str);
}

impl Debug for dyn Persister {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Persister")
            .field("type", &"Persister")
            .finish()
    }
}

#[derive(Debug)]
enum FilePersisterCommand {
    Append(MessagesToSave, CurrentPosition),
    DeleteAndTerminate(String, OneshotSender<Result<(), IggyError>>),
    Terminate,
}

#[derive(Debug)]
struct FilePersisterTask {
    join_handle: task::JoinHandle<()>,
    sender: Sender<FilePersisterCommand>,
}

impl FilePersisterTask {
    async fn new(path: String) -> Self {
        let (sender, receiver): (Sender<FilePersisterCommand>, Receiver<FilePersisterCommand>) =
            unbounded();
        let (completion_sender, completion_receiver): (
            oneshot::Sender<i32>,
            oneshot::Receiver<i32>,
        ) = oneshot::channel();

        let join_handle = task::spawn(async move {
            let log_file = file::create(&Self::get_log_path(&path)).await.unwrap();
            let time_indices_file = file::create(&Self::get_time_indices_path(&path))
                .await
                .unwrap();
            let indices_file = file::create(&Self::get_indices_path(&path)).await.unwrap();

            let mut log_writer = BufWriter::new(log_file);
            let mut time_indices_writer = BufWriter::new(time_indices_file);
            let mut indices_writer = BufWriter::new(indices_file);

            completion_sender.send(1).unwrap();

            while let Ok(file_operation) = receiver.recv_async().await {
                match file_operation {
                    FilePersisterCommand::Append(messages, current_position) => {
                        let mut current_position = current_position;

                        for message in messages {
                            message.write_to(&mut log_writer).await.unwrap();
                            time_indices_writer
                                .write_u64_le(message.timestamp)
                                .await
                                .unwrap();
                            indices_writer.write_u32_le(current_position).await.unwrap();
                            current_position += message.get_size_bytes();
                        }

                        log_writer.flush().await.unwrap();
                        time_indices_writer.flush().await.unwrap();
                        indices_writer.flush().await.unwrap();
                    }
                    FilePersisterCommand::DeleteAndTerminate(_, completion_sender) => {
                        log_writer.flush().await.unwrap();
                        time_indices_writer.flush().await.unwrap();
                        indices_writer.flush().await.unwrap();
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
                        time_indices_writer.flush().await.unwrap();
                        indices_writer.flush().await.unwrap();
                        break;
                    }
                }
            }
        });
        completion_receiver.await.unwrap();

        FilePersisterTask {
            sender,
            join_handle,
        }
    }

    pub async fn terminate(self) {
        self.sender
            .send_async(FilePersisterCommand::Terminate)
            .await
            .unwrap();
        self.join_handle.await.unwrap();
    }

    fn get_log_path(path: &str) -> String {
        format!("{}.log", path)
    }

    fn get_time_indices_path(path: &str) -> String {
        format!("{}.timeindex", path)
    }

    fn get_indices_path(path: &str) -> String {
        format!("{}.index", path)
    }
}

#[derive(Debug)]
pub struct FilePersister {
    path_to_task_map: DashMap<String, FilePersisterTask>,
}

impl FilePersister {
    pub fn new() -> Self {
        FilePersister {
            path_to_task_map: DashMap::new(),
        }
    }

    pub async fn terminate_all(self) {
        for (_, task) in self.path_to_task_map.into_iter() {
            task.terminate().await;
        }
    }

    pub async fn terminate_one(&self, path: &str) {
        if let Some((_, task)) = self.path_to_task_map.remove(path) {
            task.terminate().await;
        }
    }
}

impl Default for FilePersister {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for FilePersister {}
unsafe impl Sync for FilePersister {}

#[async_trait]
impl Persister for FilePersister {
    async fn append(
        &self,
        path: &str,
        messages: Vec<Arc<Message>>,
        current_position: u32,
    ) -> Result<(), IggyError> {
        let file_operation = FilePersisterCommand::Append(messages, current_position);
        let path = path.to_owned();
        let task = self.path_to_task_map.get(&path).unwrap();

        task.sender.send_async(file_operation).await.unwrap();
        Ok(())
    }

    async fn create(&self, path: &str) -> Result<(), IggyError> {
        let path = path.to_owned();
        let task = FilePersisterTask::new(path.clone()).await;

        self.path_to_task_map.insert(path.clone(), task);

        info!("Created file persister for path: {}", path);

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        let (completion_sender, completion_receiver) = oneshot::channel();
        let file_operation =
            FilePersisterCommand::DeleteAndTerminate(path.to_owned(), completion_sender);
        let path = path.to_owned();

        let task = self.path_to_task_map.get(&path).unwrap();

        task.sender.send_async(file_operation).await.unwrap();

        completion_receiver.await.unwrap()
    }

    async fn terminate(&self, path: &str) {
        self.terminate_one(path).await;
    }
}

#[derive(Debug)]
pub struct FileWithSyncPersister;

unsafe impl Send for FileWithSyncPersister {}
unsafe impl Sync for FileWithSyncPersister {}

#[async_trait]
impl Persister for FileWithSyncPersister {
    async fn append(
        &self,
        path: &str,
        messages: Vec<Arc<Message>>,
        current_position: u32,
    ) -> Result<(), IggyError> {
        todo!()
    }

    async fn create(&self, path: &str) -> Result<(), IggyError> {
        todo!()
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        todo!()
    }

    async fn terminate(&self, path: &str) {
        todo!()
    }
}
