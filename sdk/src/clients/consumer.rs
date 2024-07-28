use crate::client::Client;
use crate::consumer::{Consumer, ConsumerKind};
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::poll_messages::{PollingKind, PollingStrategy};
use crate::models::messages::{PolledMessage, PolledMessages};
use crate::utils::crypto::Encryptor;
use crate::utils::duration::{IggyDuration, SEC_IN_MICRO};
use bytes::Bytes;
use futures::Stream;
use futures_util::FutureExt;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::time::sleep;
use tracing::{error, info, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;
type PollMessagesFuture = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>>>>;

/// The auto-commit configuration for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommit {
    /// The auto-commit is disabled and the offset must be stored manually by the consumer.
    Disabled,
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode.
    Mode(AutoCommitMode),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval.
    Interval(IggyDuration),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode.
    IntervalAndMode(IggyDuration, AutoCommitMode),
}

/// The auto-commit mode for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommitMode {
    /// The offset is stored on the server when the messages are received.
    AfterPollingMessages,
    /// The offset is stored on the server after all the messages are consumed.
    AfterConsumingAllMessages,
    /// The offset is stored on the server after consuming each message.
    AfterConsumingEachMessage,
}

unsafe impl Send for IggyConsumer {}
unsafe impl Sync for IggyConsumer {}

pub struct IggyConsumer {
    initialized: bool,
    can_poll: Arc<AtomicBool>,
    client: IggySharedMut<Box<dyn Client>>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    interval: Option<IggyDuration>,
    batch_size: u32,
    auto_commit: AutoCommit,
    auto_commit_after_polling: bool,
    auto_join_consumer_group: bool,
    create_consumer_group_if_not_exists: bool,
    next_offset: Arc<AtomicU64>,
    last_stored_offset: Arc<AtomicU64>,
    poll_future: Option<PollMessagesFuture>,
    buffered_messages: VecDeque<PolledMessage>,
    encryptor: Option<Arc<dyn Encryptor>>,
    store_offset_sender: Option<flume::Sender<u64>>,
    store_offset_receiver: Option<flume::Receiver<u64>>,
    store_offset_after_each_message: bool,
    store_offset_after_all_messages: bool,
    is_consumer_group: bool,
    consumer_group_joined: Arc<AtomicBool>,
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
        polling_interval: Option<IggyDuration>,
        polling_strategy: PollingStrategy,
        batch_size: u32,
        auto_commit: AutoCommit,
        auto_join_consumer_group: bool,
        create_consumer_group_if_not_exists: bool,
        encryptor: Option<Arc<dyn Encryptor>>,
    ) -> Self {
        let (store_offset_sender, store_offset_receiver) = if matches!(
            auto_commit,
            AutoCommit::Mode(AutoCommitMode::AfterConsumingEachMessage)
                | AutoCommit::IntervalAndMode(_, AutoCommitMode::AfterConsumingEachMessage)
        ) {
            let (sender, receiver) = flume::unbounded();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };

        Self {
            initialized: false,
            is_consumer_group: consumer.kind == ConsumerKind::ConsumerGroup,
            can_poll: Arc::new(AtomicBool::new(true)),
            client,
            consumer_name,
            consumer: Arc::new(consumer),
            stream_id: Arc::new(stream_id),
            topic_id: Arc::new(topic_id),
            partition_id,
            polling_strategy,
            interval: polling_interval,
            next_offset: Arc::new(AtomicU64::new(0)),
            last_stored_offset: Arc::new(AtomicU64::new(0)),
            poll_future: None,
            batch_size,
            auto_commit,
            auto_commit_after_polling: matches!(
                auto_commit,
                AutoCommit::Mode(AutoCommitMode::AfterPollingMessages)
                    | AutoCommit::IntervalAndMode(_, AutoCommitMode::AfterPollingMessages)
            ),
            auto_join_consumer_group,
            create_consumer_group_if_not_exists,
            buffered_messages: VecDeque::new(),
            encryptor,
            store_offset_sender,
            store_offset_receiver,
            store_offset_after_each_message: matches!(
                auto_commit,
                AutoCommit::Mode(AutoCommitMode::AfterConsumingEachMessage)
                    | AutoCommit::IntervalAndMode(_, AutoCommitMode::AfterConsumingEachMessage)
            ),
            store_offset_after_all_messages: matches!(
                auto_commit,
                AutoCommit::Mode(AutoCommitMode::AfterConsumingAllMessages)
                    | AutoCommit::IntervalAndMode(_, AutoCommitMode::AfterConsumingAllMessages)
            ),
            consumer_group_joined: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.initialized {
            return Ok(());
        }

        self.subscribe_events().await;
        self.init_consumer_group().await?;

        match self.auto_commit {
            AutoCommit::Interval(interval) => self.store_offsets_in_background(interval),
            AutoCommit::IntervalAndMode(interval, _) => self.store_offsets_in_background(interval),
            _ => {}
        }

        if let Some(store_offset_receiver) = self.store_offset_receiver.clone() {
            let client = self.client.clone();
            let consumer = self.consumer.clone();
            let stream_id = self.stream_id.clone();
            let topic_id = self.topic_id.clone();
            let partition_id = self.partition_id;
            let last_stored_offset = self.last_stored_offset.clone();
            tokio::spawn(async move {
                while let Ok(offset) = store_offset_receiver.recv_async().await {
                    let last_offset = last_stored_offset.load(ORDERING);
                    if offset <= last_offset {
                        continue;
                    }

                    let client = client.read().await;
                    if let Err(error) = client
                        .store_consumer_offset(
                            &consumer,
                            &stream_id,
                            &topic_id,
                            partition_id,
                            offset,
                        )
                        .await
                    {
                        error!("Failed to store offset: {offset}, error: {error}");
                        continue;
                    }
                    info!("Stored offset: {offset}");
                    last_stored_offset.store(offset, ORDERING);
                }
            });
        }

        self.initialized = true;
        Ok(())
    }

    fn store_offset(&self, offset: u64) {
        if let Some(sender) = self.store_offset_sender.as_ref() {
            if let Err(error) = sender.send(offset) {
                error!("Failed to send offset to store: {error}");
            }
        }
    }

    fn store_offsets_in_background(&self, interval: IggyDuration) {
        let client = self.client.clone();
        let consumer = self.consumer.clone();
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let partition_id = self.partition_id;
        let next_offset_to_poll = self.next_offset.clone();
        let last_stored_offset = self.last_stored_offset.clone();
        tokio::spawn(async move {
            loop {
                sleep(interval.get_duration()).await;
                let next_offset = next_offset_to_poll.load(ORDERING);
                let last_offset = last_stored_offset.load(ORDERING);
                if last_offset == 0 && next_offset == 0 {
                    continue;
                }

                let offset = next_offset - 1;
                if offset <= last_offset {
                    continue;
                }

                let client = client.read().await;
                if let Err(error) = client
                    .store_consumer_offset(&consumer, &stream_id, &topic_id, partition_id, offset)
                    .await
                {
                    error!("Failed to store offset: {offset} in the background, error: {error}");
                    continue;
                }
                info!("Stored offset: {offset} in the background");
                last_stored_offset.store(offset, ORDERING);
            }
        });
    }

    async fn init_consumer_group(&self) -> Result<(), IggyError> {
        if !self.is_consumer_group {
            return Ok(());
        }

        if self.consumer_group_joined.load(ORDERING) {
            info!("Consumer group is already joined");
            return Ok(());
        }

        if !self.auto_join_consumer_group {
            info!("Auto join consumer group is disabled");
            return Ok(());
        }

        Self::handle_consumer_group(
            self.client.clone(),
            self.create_consumer_group_if_not_exists,
            self.stream_id.clone(),
            self.topic_id.clone(),
            self.consumer.clone(),
            &self.consumer_name,
            self.consumer_group_joined.clone(),
        )
        .await
    }

    async fn subscribe_events(&self) {
        info!("Subscribing to diagnostic events");
        let receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let is_consumer_group = self.is_consumer_group;
        let can_join_consumer_group = is_consumer_group && self.auto_join_consumer_group;
        let client = self.client.clone();
        let create_consumer_group_if_not_exists = self.create_consumer_group_if_not_exists;
        let stream_id = self.stream_id.clone();
        let topic_id = self.topic_id.clone();
        let consumer = self.consumer.clone();
        let consumer_name = self.consumer_name.clone();
        let can_poll = self.can_poll.clone();
        let consumer_group_joined = self.consumer_group_joined.clone();
        let should_rejoin_consumer_group = Arc::new(AtomicBool::new(false));

        tokio::spawn(async move {
            while let Ok(event) = receiver.recv_async().await {
                info!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Connected => {
                        info!("Connected to the server");
                    }
                    DiagnosticEvent::Disconnected => {
                        can_poll.store(false, ORDERING);
                        if is_consumer_group {
                            consumer_group_joined.store(false, ORDERING);
                            should_rejoin_consumer_group.store(true, ORDERING);
                        }
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        if !is_consumer_group {
                            can_poll.store(true, ORDERING);
                            info!("Set can poll to true");
                            continue;
                        }

                        let join_consumer_group = should_rejoin_consumer_group.load(ORDERING);
                        should_rejoin_consumer_group.store(false, ORDERING);
                        if !join_consumer_group {
                            can_poll.store(true, ORDERING);
                            info!("Set can poll to true");
                            continue;
                        }

                        if !can_join_consumer_group {
                            can_poll.store(true, ORDERING);
                            info!("Set can poll to true");
                            warn!("Auto join consumer group is disabled");
                            continue;
                        }

                        info!("Rejoining consumer group");
                        if let Err(error) = Self::handle_consumer_group(
                            client.clone(),
                            create_consumer_group_if_not_exists,
                            stream_id.clone(),
                            topic_id.clone(),
                            consumer.clone(),
                            &consumer_name,
                            consumer_group_joined.clone(),
                        )
                        .await
                        {
                            error!("Failed to join consumer group: {error}");
                            continue;
                        }
                        info!("Rejoined consumer group");
                        can_poll.store(true, ORDERING);
                        info!("Set can poll to true");
                    }
                    DiagnosticEvent::SignedOut => {
                        can_poll.store(false, ORDERING);
                        if is_consumer_group {
                            consumer_group_joined.store(false, ORDERING);
                            should_rejoin_consumer_group.store(true, ORDERING);
                        }
                    }
                }
            }
        });
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
        let auto_commit = self.auto_commit_after_polling;
        let interval = self.interval;

        async move {
            if let Some(interval) = interval {
                sleep(interval.get_duration()).await;
            }

            info!("Sending poll messages request");
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

    async fn handle_consumer_group(
        client: IggySharedMut<Box<dyn Client>>,
        create_consumer_group_if_not_exists: bool,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        consumer: Arc<Consumer>,
        consumer_name: &str,
        joined: Arc<AtomicBool>,
    ) -> Result<(), IggyError> {
        let client = client.read().await;
        let (name, id) = match consumer.id.kind {
            IdKind::Numeric => (consumer_name.to_owned(), Some(consumer.id.get_u32_value()?)),
            IdKind::String => (consumer.id.get_string_value()?, None),
        };

        let consumer_group_id = name.to_owned().try_into()?;
        info!("Validating consumer group: {consumer_group_id}");
        if let Err(error) = client
            .get_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await
        {
            if !create_consumer_group_if_not_exists {
                error!("Consumer group does not exist and auto-creation is disabled.");
                return Err(error);
            }

            info!("Creating consumer group: {consumer_group_id}");
            client
                .create_consumer_group(&stream_id, &topic_id, &name, id)
                .await?;
        }

        info!("Joining consumer group: {consumer_group_id}",);
        client
            .join_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await?;
        joined.store(true, ORDERING);
        info!("Joined consumer group: {consumer_group_id}",);
        Ok(())
    }
}

impl Stream for IggyConsumer {
    type Item = Result<PolledMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(message) = self.buffered_messages.pop_front() {
            let next_offset = message.offset + 1;
            self.next_offset.store(next_offset, ORDERING);
            if self.buffered_messages.is_empty() {
                if self.polling_strategy.kind == PollingKind::Offset {
                    self.polling_strategy = PollingStrategy::offset(next_offset);
                }

                if self.store_offset_after_each_message || self.store_offset_after_all_messages {
                    self.store_offset(message.offset);
                }
            } else if self.store_offset_after_each_message {
                self.store_offset(message.offset);
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
                        let next_offset = message.offset + 1;
                        self.next_offset.store(next_offset, ORDERING);
                        self.buffered_messages.extend(polled_messages.messages);

                        if self.polling_strategy.kind == PollingKind::Offset {
                            self.polling_strategy = PollingStrategy::offset(next_offset);
                        }

                        if self.buffered_messages.is_empty() {
                            if self.store_offset_after_all_messages {
                                self.store_offset(message.offset);
                            }
                        } else if self.store_offset_after_each_message {
                            self.store_offset(message.offset);
                        }

                        self.poll_future = None;
                        return Poll::Ready(Some(Ok(message)));
                    }
                }
                Poll::Ready(Err(err)) => {
                    self.poll_future = None;
                    return Poll::Ready(Some(Err(err)));
                }
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
    polling_interval: Option<IggyDuration>,
    batch_size: u32,
    auto_commit: AutoCommit,
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
        polling_interval: Option<IggyDuration>,
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
            auto_commit: AutoCommit::IntervalAndMode(
                IggyDuration::from(SEC_IN_MICRO),
                AutoCommitMode::AfterPollingMessages,
            ),
            auto_join_consumer_group: true,
            create_consumer_group_if_not_exists: true,
            encryptor,
            polling_interval,
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

    pub fn auto_commit(self, auto_commit: AutoCommit) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    pub fn auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: true,
            ..self
        }
    }

    pub fn do_not_auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: false,
            ..self
        }
    }

    pub fn create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: true,
            ..self
        }
    }

    pub fn do_not_create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: false,
            ..self
        }
    }

    pub fn polling_interval(self, interval: IggyDuration) -> Self {
        Self {
            polling_interval: Some(interval),
            ..self
        }
    }

    pub fn without_polling_interval(self) -> Self {
        Self {
            polling_interval: None,
            ..self
        }
    }

    pub fn encryptor(self, encryptor: Arc<dyn Encryptor>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    pub fn without_encryptor(self) -> Self {
        Self {
            encryptor: None,
            ..self
        }
    }

    pub fn build(self) -> IggyConsumer {
        IggyConsumer::new(
            self.client,
            self.consumer_name,
            self.consumer,
            self.stream,
            self.topic,
            self.partition,
            self.polling_interval,
            self.polling_strategy,
            self.batch_size,
            self.auto_commit,
            self.auto_join_consumer_group,
            self.create_consumer_group_if_not_exists,
            self.encryptor,
        )
    }
}
