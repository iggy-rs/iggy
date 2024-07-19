use crate::client::Client;
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::poll_messages::PollingStrategy;
use crate::models::messages::{PolledMessage, PolledMessages};
use futures::Stream;
use futures_util::FutureExt;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

type PollMessagesTask = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>>>>;

pub struct IggyConsumer {
    client: IggySharedMut<Box<dyn Client>>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    polling_strategy: PollingStrategy,
    next_offset: Option<u64>,
    poll_task: Option<PollMessagesTask>,
    buffered_messages: VecDeque<PolledMessage>,
    batch_size: u32,
}

impl IggyConsumer {
    pub fn new(
        client: IggySharedMut<Box<dyn Client>>,
        stream_id: Identifier,
        topic_id: Identifier,
        polling_strategy: PollingStrategy,
        batch_size: u32,
    ) -> Result<Self, IggyError> {
        Ok(Self {
            client,
            stream_id: Arc::new(stream_id),
            topic_id: Arc::new(topic_id),
            polling_strategy,
            next_offset: None,
            poll_task: None,
            buffered_messages: VecDeque::new(),
            batch_size,
        })
    }
}

impl Stream for IggyConsumer {
    type Item = Result<PolledMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.buffered_messages.is_empty() {
            let message = self.buffered_messages.pop_back().unwrap();
            self.next_offset = Some(message.offset + 1);
            if self.buffered_messages.is_empty() {
                self.poll_task = None;
            }

            return Poll::Ready(Some(Ok(message)));
        }

        if self.poll_task.is_none() {
            let polling_strategy = if let Some(next_offset) = self.next_offset {
                PollingStrategy::offset(next_offset)
            } else {
                self.polling_strategy
            };

            let next_task = {
                let stream_id = self.stream_id.clone();
                let topic_id = self.topic_id.clone();
                let client = self.client.clone();
                let count = self.batch_size;
                async move {
                    client
                        .read()
                        .await
                        .poll_messages(
                            &stream_id,
                            &topic_id,
                            Some(1),
                            &Consumer::default(),
                            &polling_strategy,
                            count,
                            false,
                        )
                        .await
                }
            };

            self.poll_task = Some(Box::pin(next_task));
        }

        match self
            .poll_task
            .as_mut()
            .expect("Poll task is missing.")
            .poll_unpin(cx)
        {
            Poll::Ready(polled_messages_result) => match polled_messages_result {
                Ok(mut polled_messages) => {
                    if polled_messages.messages.is_empty() {
                        return Poll::Pending;
                    }

                    let message = polled_messages.messages.swap_remove(0);
                    self.next_offset = Some(message.offset + 1);
                    if polled_messages.messages.is_empty() {
                        self.poll_task = None;
                    } else {
                        self.buffered_messages.extend(polled_messages.messages);
                    }

                    Poll::Ready(Some(Ok(message)))
                }
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
