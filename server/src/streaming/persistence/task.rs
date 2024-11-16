use bytes::Bytes;
use flume::{unbounded, Receiver, Sender};
use iggy::error::IggyError;
use tokio::task;
use tracing::error;
use std::{sync::Arc, time::Duration};
use crate::streaming::persistence::persister::Persister;

#[derive(Debug)]
pub struct LogPersisterTask {
    _sender: Sender<Bytes>,
    _task_handle: task::JoinHandle<()>,
}

impl LogPersisterTask {
    pub fn new(
        path: String,
        persister: Arc<dyn Persister>,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Self {
        let (sender, receiver): (Sender<Bytes>, Receiver<Bytes>) = unbounded();

        let task_handle = task::spawn(async move {
            loop {
                match receiver.recv_async().await {
                    Ok(data) => {
                        if let Err(e) = Self::persist_with_retries(&path, &persister, data, max_retries, retry_sleep).await {
                            error!("Final failure to persist data: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("Error receiving data from channel: {}", e);
                    }
                }
            }
        });

        LogPersisterTask {
            _sender: sender,
            _task_handle: task_handle,
        }
    }

    async fn persist_with_retries(
        path: &str,
        persister: &Arc<dyn Persister>,
        data: Bytes,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Result<(), String> {
        let mut retries = 0;

        while retries < max_retries {
            match persister.append(path, &data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    error!("Could not append to persister (attempt {}): {}", retries + 1, e);
                    retries += 1;
                    tokio::time::sleep(retry_sleep).await;
                }
            }
        }

        Err(format!("Failed to persist data after {} retries", max_retries))
    }

    pub async fn send(&self, data: Bytes) -> Result<(), IggyError> {
        self._sender
            .send_async(data)
            .await
            .map_err(|e| IggyError::CannotSaveMessagesToSegment(e.into()))
    }
}
