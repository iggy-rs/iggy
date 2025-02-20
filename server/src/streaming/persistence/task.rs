use crate::streaming::{
    batching::batch_accumulator::{self, BatchAccumulator},
    persistence::COMPONENT,
    segments::storage::FileSegmentStorage,
};
use bytes::Bytes;
use error_set::ErrContext;
use flume::{unbounded, Receiver, Sender};
use iggy::error::IggyError;
use std::{sync::Arc, time::Duration};
use tokio::task;
use tracing::error;

use super::persister::PersisterKind;

#[derive(Debug)]
pub struct LogPersisterTask {
    _sender: Option<Sender<BatchAccumulator>>,
    _task_handle: Option<task::JoinHandle<()>>,
}

impl LogPersisterTask {
    pub fn new(
        path: String,
        persister: Arc<PersisterKind>,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Self {
        let (sender, receiver): (Sender<BatchAccumulator>, Receiver<BatchAccumulator>) =
            unbounded();

        let task_handle = task::spawn(async move {
            loop {
                match receiver.recv_async().await {
                    Ok(data) => {
                        if let Err(e) = Self::persist_with_retries(
                            &path,
                            &persister,
                            data,
                            max_retries,
                            retry_sleep,
                        )
                        .await
                        {
                            error!("{COMPONENT} - Final failure to persist data: {}", e);
                        }
                    }
                    Err(e) => {
                        error!("{COMPONENT} - Error receiving data from channel: {}", e);
                        return;
                    }
                }
            }
        });

        LogPersisterTask {
            _sender: Some(sender),
            _task_handle: Some(task_handle),
        }
    }

    async fn persist_with_retries(
        path: &str,
        persister: &Arc<PersisterKind>,
        batch_accumulator: BatchAccumulator,
        max_retries: u32,
        retry_sleep: Duration,
    ) -> Result<(), String> {
        match FileSegmentStorage::persist_batches(path, batch_accumulator).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                error!("Could not append to persister: {}", e);
                tokio::time::sleep(retry_sleep).await;
            }
        }
        Err(format!(
            "{COMPONENT} - failed to persist data after {} retries",
            max_retries
        ))
    }

    pub async fn send(&self, data: BatchAccumulator) -> Result<(), IggyError> {
        if let Some(sender) = &self._sender {
            sender
                .send_async(data)
                .await
                .with_error_context(|err| {
                    format!("{COMPONENT} - failed to send data to async channel, err: {err}")
                })
                .map_err(|_| IggyError::CannotSaveMessagesToSegment)
        } else {
            Err(IggyError::CannotSaveMessagesToSegment)
        }
    }
}

impl Drop for LogPersisterTask {
    fn drop(&mut self) {
        self._sender.take();

        if let Some(handle) = self._task_handle.take() {
            tokio::spawn(async move {
                if let Err(e) = handle.await {
                    error!(
                        "{COMPONENT} - error while shutting down task in Drop: {:?}",
                        e
                    );
                }
            });
        }
    }
}
