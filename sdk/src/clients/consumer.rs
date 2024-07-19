use crate::client::Client;
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::poll_messages::{PollingKind, PollingStrategy};
use crate::models::messages::{PolledMessage, PolledMessages};
use futures::Stream;
use futures_util::FutureExt;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

type PollMessagesFuture = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>>>>;

pub struct IggyConsumer {
    client: IggySharedMut<Box<dyn Client>>,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    next_offset: u64,
    poll_future: Option<PollMessagesFuture>,
    buffered_messages: VecDeque<PolledMessage>,
    batch_size: u32,
    auto_commit: bool,
}

impl IggyConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: IggySharedMut<Box<dyn Client>>,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        polling_strategy: PollingStrategy,
        batch_size: u32,
        auto_commit: bool,
    ) -> Self {
        Self {
            client,
            consumer: Arc::new(consumer),
            stream_id: Arc::new(stream_id),
            topic_id: Arc::new(topic_id),
            partition_id,
            polling_strategy,
            next_offset: 0,
            poll_future: None,
            buffered_messages: VecDeque::new(),
            batch_size,
            auto_commit,
        }
    }
}

impl Stream for IggyConsumer {
    type Item = Result<PolledMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(message) = self.buffered_messages.pop_front() {
            self.next_offset = message.offset + 1;
            if self.buffered_messages.is_empty()
                && self.polling_strategy.kind == PollingKind::Offset
            {
                self.polling_strategy = PollingStrategy::offset(self.next_offset);
            }

            return Poll::Ready(Some(Ok(message)));
        }

        if self.poll_future.is_none() {
            let future = {
                let stream_id = self.stream_id.clone();
                let topic_id = self.topic_id.clone();
                let partition_id = self.partition_id;
                let consumer = self.consumer.clone();
                let polling_strategy = self.polling_strategy;
                let client = self.client.clone();
                let count = self.batch_size;
                let auto_commit = self.auto_commit;
                async move {
                    client
                        .read()
                        .await
                        .poll_messages(
                            &stream_id,
                            &topic_id,
                            partition_id,
                            &consumer,
                            &polling_strategy,
                            count,
                            auto_commit,
                        )
                        .await
                }
            };

            self.poll_future = Some(Box::pin(future));
        }

        match self
            .poll_future
            .as_mut()
            .expect("Poll future is missing.")
            .poll_unpin(cx)
        {
            Poll::Ready(polled_messages_result) => match polled_messages_result {
                Ok(mut polled_messages) => {
                    if polled_messages.messages.is_empty() {
                        return Poll::Pending;
                    }

                    let message = polled_messages.messages.remove(0);
                    self.next_offset = message.offset + 1;
                    if polled_messages.messages.is_empty() {
                        if self.polling_strategy.kind == PollingKind::Offset {
                            self.polling_strategy = PollingStrategy::offset(self.next_offset);
                        }
                    } else {
                        self.buffered_messages.extend(polled_messages.messages);
                    }

                    self.poll_future = None;
                    Poll::Ready(Some(Ok(message)))
                }
                Err(err) => Poll::Ready(Some(Err(err))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
