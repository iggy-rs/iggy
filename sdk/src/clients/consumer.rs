use crate::client::Client;
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::poll_messages::{PollingKind, PollingStrategy};
use crate::models::messages::{PolledMessage, PolledMessages};
use crate::utils::crypto::Encryptor;
use bytes::Bytes;
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
    batch_size: u32,
    auto_commit: bool,
    next_offset: u64,
    poll_future: Option<PollMessagesFuture>,
    buffered_messages: VecDeque<PolledMessage>,
    encryptor: Option<Arc<dyn Encryptor>>,
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
        encryptor: Option<Arc<dyn Encryptor>>,
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
            batch_size,
            auto_commit,
            buffered_messages: VecDeque::new(),
            encryptor,
        }
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        // TODO: Join consumer group, create topic etc.
        let _client = self.client.read().await;
        Ok(())
    }

    fn create_poll_messages_future(
        &self,
    ) -> impl Future<Output = Result<PolledMessages, IggyError>> {
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
    }
}

// TODO: Handle joining consumer group during reconnect
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
            let future = self.create_poll_messages_future();
            self.poll_future = Some(Box::pin(future));
        }

        while let Some(future) = self.poll_future.as_mut() {
            match future.poll_unpin(cx) {
                Poll::Ready(Ok(mut polled_messages)) => {
                    if polled_messages.messages.is_empty() {
                        self.poll_future = Some(Box::pin(self.create_poll_messages_future()));
                    } else {
                        if let Some(ref encryptor) = self.encryptor {
                            for message in &mut polled_messages.messages {
                                let payload = encryptor.decrypt(&message.payload)?;
                                message.payload = Bytes::from(payload);
                            }
                        }

                        let message = polled_messages.messages.remove(0);
                        self.next_offset = message.offset + 1;
                        self.buffered_messages.extend(polled_messages.messages);
                        if self.polling_strategy.kind == PollingKind::Offset {
                            self.polling_strategy = PollingStrategy::offset(self.next_offset);
                        }
                        self.poll_future = None;
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => return Poll::Pending,
            }
        }

        Poll::Pending
    }
}
