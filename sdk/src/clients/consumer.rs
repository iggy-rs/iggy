use crate::client::Client;
use crate::consumer::{Consumer, ConsumerKind};
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::poll_messages::{PollingKind, PollingStrategy};
use crate::models::messages::{PolledMessage, PolledMessages};
use crate::utils::crypto::Encryptor;
use crate::utils::duration::IggyDuration;
use bytes::Bytes;
use futures::Stream;
use futures_util::FutureExt;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::sleep;
use tracing::{error, info};

type PollMessagesFuture = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>>>>;

pub struct IggyConsumer {
    client: IggySharedMut<Box<dyn Client>>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    interval: Option<IggyDuration>,
    batch_size: u32,
    auto_commit: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    next_offset: u64,
    poll_future: Option<PollMessagesFuture>,
    buffered_messages: VecDeque<PolledMessage>,
    encryptor: Option<Arc<dyn Encryptor>>,
}

impl IggyConsumer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        interval: Option<IggyDuration>,
        polling_strategy: PollingStrategy,
        batch_size: u32,
        auto_commit: bool,
        auto_join_consumer_group: bool,
        create_consumer_group_if_not_exists: bool,
        encryptor: Option<Arc<dyn Encryptor>>,
    ) -> Self {
        Self {
            client,
            consumer_name,
            consumer: Arc::new(consumer),
            stream_id: Arc::new(stream_id),
            topic_id: Arc::new(topic_id),
            partition_id,
            polling_strategy,
            interval,
            next_offset: 0,
            poll_future: None,
            batch_size,
            auto_commit,
            auto_join_consumer_group,
            create_consumer_group_if_not_exists,
            buffered_messages: VecDeque::new(),
            encryptor,
        }
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.consumer.kind != ConsumerKind::ConsumerGroup {
            return Ok(());
        }

        if !self.auto_join_consumer_group {
            info!("Auto join consumer group is disabled");
            return Ok(());
        }

        let client = self.client.read().await;
        if let Err(error) = client
            .get_consumer_group(&self.stream_id, &self.topic_id, &self.consumer.id)
            .await
        {
            if !self.create_consumer_group_if_not_exists {
                error!("Consumer group does not exist and auto-creation is disabled.");
                return Err(error);
            }

            let (name, id) = match self.consumer.id.kind {
                IdKind::Numeric => (
                    self.consumer_name.to_owned(),
                    Some(self.consumer.id.get_u32_value()?),
                ),
                IdKind::String => (self.consumer.id.get_string_value()?, None),
            };

            info!("Creating consumer group: {name}");
            client
                .create_consumer_group(&self.stream_id, &self.topic_id, &name, id)
                .await?;
        }

        info!("Joining consumer group: {}", self.consumer.id);
        client
            .join_consumer_group(&self.stream_id, &self.topic_id, &self.consumer.id)
            .await?;
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
        let interval = self.interval;

        async move {
            if let Some(interval) = interval {
                sleep(interval.get_duration()).await;
            }

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

#[derive(Debug)]
pub struct IggyConsumerBuilder {
    client: IggySharedMut<Box<dyn Client>>,
    consumer_name: String,
    consumer: Consumer,
    stream: Identifier,
    topic: Identifier,
    partition: Option<u32>,
    polling_strategy: PollingStrategy,
    interval: Option<IggyDuration>,
    batch_size: u32,
    auto_commit: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    encryptor: Option<Arc<dyn Encryptor>>,
}

impl IggyConsumerBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        consumer_name: String,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        encryptor: Option<Arc<dyn Encryptor>>,
        interval: Option<IggyDuration>,
    ) -> Self {
        Self {
            client,
            consumer_name,
            consumer,
            stream: stream_id,
            topic: topic_id,
            partition: partition_id,
            polling_strategy: PollingStrategy::next(),
            batch_size: 1000,
            auto_commit: true,
            auto_join_consumer_group: true,
            create_consumer_group_if_not_exists: true,
            encryptor,
            interval,
        }
    }

    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    pub fn partition(self, partition: Option<u32>) -> Self {
        Self { partition, ..self }
    }

    pub fn polling_strategy(self, polling_strategy: PollingStrategy) -> Self {
        Self {
            polling_strategy,
            ..self
        }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn auto_commit(self, auto_commit: bool) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn auto_join_consumer_group(self, join: bool) -> Self {
        Self {
            auto_join_consumer_group: join,
            ..self
        }
    }

    pub fn create_consumer_group_if_not_exists(self, create: bool) -> Self {
        Self {
            create_consumer_group_if_not_exists: create,
            ..self
        }
    }

    pub fn interval(self, interval: Option<IggyDuration>) -> Self {
        Self { interval, ..self }
    }

    pub fn encryptor(self, encryptor: Option<Arc<dyn Encryptor>>) -> Self {
        Self { encryptor, ..self }
    }

    pub fn build(self) -> IggyConsumer {
        IggyConsumer::new(
            self.client,
            self.consumer_name,
            self.consumer,
            self.stream,
            self.topic,
            self.partition,
            self.interval,
            self.polling_strategy,
            self.batch_size,
            self.auto_commit,
            self.auto_join_consumer_group,
            self.create_consumer_group_if_not_exists,
            self.encryptor,
        )
    }
}
