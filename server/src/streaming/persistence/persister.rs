use crate::configs::system::SystemConfig;
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use flume::{unbounded, Receiver, Sender};
use iggy::confirmation::Confirmation;
use iggy::error::IggyError;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::time::{Duration, Instant};
use tokio::{fs, sync::oneshot};
use tokio::{task, time};
use tracing::error;

#[async_trait]
pub trait Persister: Sync + Send {
    async fn append(
        &self,
        path: &str,
        bytes: Bytes,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError>;
    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError>;
    async fn delete(&self, path: &str) -> Result<(), IggyError>;
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
    Append(Bytes),
}

#[derive(Debug)]
struct FilePersisterTask {
    sender: Sender<FilePersisterCommand>,
}

impl FilePersisterTask {
    async fn new(
        path: String,
        idle_timeout: Duration,
        idle_notifier: Sender<String>,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Self {
        let (sender, receiver): (Sender<FilePersisterCommand>, Receiver<FilePersisterCommand>) =
            unbounded();
        let (completion_sender, completion_receiver): (
            oneshot::Sender<i32>,
            oneshot::Receiver<i32>,
        ) = oneshot::channel();

        task::spawn(async move {
            let mut idle_interval = time::interval(idle_timeout);
            let mut last_used = Instant::now();

            completion_sender.send(1).unwrap();

            loop {
                tokio::select! {
                    command = receiver.recv_async() => {
                        last_used = Instant::now();
                        let file_operation = match command {
                            Ok(file_operation) => file_operation,
                            Err(e) => {
                                error!("Error receiving command: {}", e);
                                continue
                            }
                        };

                        let mut retries = 0;
                        while retries < max_retries {
                            match FilePersisterTask::handle_file_operation(&file_operation, &path).await {
                                Ok(_) => break,
                                Err(e) => {
                                    error!("File operation failed: {:?}", e);
                                    retries += 1;
                                    tokio::time::sleep(retry_sleep).await;
                                }
                            };
                        }
                    },
                    _ = idle_interval.tick() => {
                        if last_used.elapsed() > idle_timeout {
                            if let Err(e) = idle_notifier.send_async(path.to_string()).await {
                                error!("Error sending idle notification: {:?}", e);
                            };
                            break
                        }
                    }
                }
            }
        });
        completion_receiver.await.unwrap();

        FilePersisterTask { sender }
    }

    async fn handle_file_operation(
        file_operation: &FilePersisterCommand,
        path: &str,
    ) -> Result<(), std::io::Error> {
        match file_operation {
            FilePersisterCommand::Append(bytes) => {
                let mut file = file::append(path).await?;
                file.write_all(bytes).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FilePersister {
    path_to_task_map: Arc<DashMap<String, Arc<FilePersisterTask>>>,
    idle_notifier: Sender<String>,
    config: Arc<SystemConfig>,
}

unsafe impl Send for FilePersister {}
unsafe impl Sync for FilePersister {}

impl FilePersister {
    pub async fn new(config: Arc<SystemConfig>) -> Self {
        let (sender, receiver): (Sender<String>, Receiver<String>) = unbounded();
        let path_to_task_map = Arc::new(DashMap::new());

        let persister = FilePersister {
            path_to_task_map: path_to_task_map.clone(),
            idle_notifier: sender,
            config,
        };

        let map_clone = Arc::clone(&path_to_task_map);

        task::spawn(async move {
            while let Ok(idle_path) = receiver.recv_async().await {
                map_clone.remove(&idle_path);
            }
        });
        persister
    }
}

impl FilePersister {
    async fn ensure_task(&self, path: &str) -> Arc<FilePersisterTask> {
        if let Some(task) = self.path_to_task_map.get(path) {
            task.clone()
        } else {
            let new_task = Arc::new(
                FilePersisterTask::new(
                    path.to_string(),
                    self.config.state.idle_timeout.get_duration(),
                    self.idle_notifier.clone(),
                    self.config.state.max_file_operation_retries,
                    self.config.state.retry_delay.get_duration(),
                )
                .await,
            );
            let task_entry = self
                .path_to_task_map
                .entry(path.to_string())
                .or_insert(new_task);
            task_entry.clone()
        }
    }

    pub fn is_task_active(&self, path: &str) -> bool {
        self.path_to_task_map.contains_key(path)
    }
}

#[async_trait]
impl Persister for FilePersister {
    async fn append(
        &self,
        path: &str,
        bytes: Bytes,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        let confirmation = confirmation.unwrap_or(self.config.state.server_confirmation.clone());

        match confirmation {
            Confirmation::WaitWithFlush => {
                let mut file = file::append(path).await?;
                file.write_all(&bytes).await?;
                file.sync_all().await?;
            }
            Confirmation::Wait => {
                let mut file = file::append(path).await?;
                file.write_all(&bytes).await?;
            }
            Confirmation::Nowait => {
                let task = self.ensure_task(path).await;
                task.sender
                    .send_async(FilePersisterCommand::Append(bytes))
                    .await
                    .with_context(|| format!("Failed to queue append command for file: {}", path))
                    .map_err(IggyError::CommandQueueError)?;
            }
        }
        Ok(())
    }

    async fn overwrite(&self, path: &str, bytes: &[u8]) -> Result<(), IggyError> {
        match self.config.state.server_confirmation {
            Confirmation::WaitWithFlush => {
                let mut file = file::overwrite(path).await?;
                file.write_all(bytes).await?;
                file.sync_all().await?;
            }
            _ => {
                let mut file = file::overwrite(path).await?;
                file.write_all(bytes).await?;
            }
        }
        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), IggyError> {
        fs::remove_file(path).await?;
        Ok(())
    }
}
