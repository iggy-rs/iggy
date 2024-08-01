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
use crate::utils::timestamp::IggyTimestamp;
use bytes::Bytes;
use futures::Stream;
use futures_util::{FutureExt, StreamExt};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;
type PollMessagesFuture = Pin<Box<dyn Future<Output = Result<PolledMessages, IggyError>>>>;

/// The auto-commit configuration for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommit {
    /// The auto-commit is disabled and the offset must be stored manually by the consumer.
    Disabled,
    /// The auto-commit is enabled and the offset is stored on the server depending on the mode.
    After(AutoCommitAfter),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval.
    Interval(IggyDuration),
    /// The auto-commit is enabled and the offset is stored on the server after a certain interval or depending on the mode.
    IntervalOrAfter(IggyDuration, AutoCommitAfter),
}

/// The auto-commit mode for storing the offset on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum AutoCommitAfter {
    /// The offset is stored on the server when the messages are received.
    PollingMessages,
    /// The offset is stored on the server after all the messages are consumed.
    ConsumingAllMessages,
    /// The offset is stored on the server after consuming each message.
    ConsumingEachMessage,
    /// The offset is stored on the server after consuming every Nth message.
    EveryNthMessage(u32),
}

unsafe impl Send for IggyConsumer {}
unsafe impl Sync for IggyConsumer {}

pub struct IggyConsumer {
    initialized: bool,
    can_poll: Arc<AtomicBool>,
    client: IggySharedMut<Box<dyn Client>>,
    consumer_name: String,
    consumer: Arc<Consumer>,
    is_consumer_group: bool,
    stream_id: Arc<Identifier>,
    topic_id: Arc<Identifier>,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    interval_micros: u64,
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
    store_after_every_nth_message: u64,
    last_polled_at: Arc<AtomicU64>,
    current_partition_id: Arc<AtomicU32>,
    current_offset: Arc<AtomicU64>,
    polled_messages_count: Arc<AtomicU64>,
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
            AutoCommit::After(
                AutoCommitAfter::ConsumingEachMessage | AutoCommitAfter::EveryNthMessage(_)
            ) | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEachMessage)
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
            interval_micros: polling_interval.map_or(0, |interval| interval.as_micros()),
            next_offset: Arc::new(AtomicU64::new(0)),
            last_stored_offset: Arc::new(AtomicU64::new(0)),
            poll_future: None,
            batch_size,
            auto_commit,
            auto_commit_after_polling: matches!(
                auto_commit,
                AutoCommit::After(AutoCommitAfter::PollingMessages)
                    | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::PollingMessages)
            ),
            auto_join_consumer_group,
            create_consumer_group_if_not_exists,
            buffered_messages: VecDeque::new(),
            encryptor,
            store_offset_sender,
            store_offset_receiver,
            store_offset_after_each_message: matches!(
                auto_commit,
                AutoCommit::After(AutoCommitAfter::ConsumingEachMessage)
                    | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingEachMessage)
            ),
            store_offset_after_all_messages: matches!(
                auto_commit,
                AutoCommit::After(AutoCommitAfter::ConsumingAllMessages)
                    | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::ConsumingAllMessages)
            ),
            store_after_every_nth_message: match auto_commit {
                AutoCommit::After(AutoCommitAfter::EveryNthMessage(n))
                | AutoCommit::IntervalOrAfter(_, AutoCommitAfter::EveryNthMessage(n)) => n as u64,
                _ => 0,
            },
            last_polled_at: Arc::new(AtomicU64::new(0)),
            current_partition_id: Arc::new(AtomicU32::new(0)),
            current_offset: Arc::new(AtomicU64::new(0)),
            polled_messages_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn stream(&self) -> &Identifier {
        &self.stream_id
    }

    pub fn topic(&self) -> &Identifier {
        &self.topic_id
    }

    /// Initializes the consumer by subscribing to diagnostic events, initializing the consumer group if needed, storing the offsets in the background etc.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.initialized {
            return Ok(());
        }

        self.subscribe_events().await;
        self.init_consumer_group().await?;

        match self.auto_commit {
            AutoCommit::Interval(interval) => self.store_offsets_in_background(interval),
            AutoCommit::IntervalOrAfter(interval, _) => self.store_offsets_in_background(interval),
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
                    trace!("Stored offset: {offset}");
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
                trace!("Stored offset: {offset} in the background");
                last_stored_offset.store(offset, ORDERING);
            }
        });
    }

    async fn init_consumer_group(&self) -> Result<(), IggyError> {
        if !self.is_consumer_group {
            return Ok(());
        }

        if !self.auto_join_consumer_group {
            warn!("Auto join consumer group is disabled");
            return Ok(());
        }

        Self::initialize_consumer_group(
            self.client.clone(),
            self.create_consumer_group_if_not_exists,
            self.stream_id.clone(),
            self.topic_id.clone(),
            self.consumer.clone(),
            &self.consumer_name,
        )
        .await
    }

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
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
        let mut reconnected = false;
        let mut disconnected = false;

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Connected => {
                        trace!("Connected to the server");
                        if disconnected {
                            reconnected = true;
                            disconnected = false;
                        }
                    }
                    DiagnosticEvent::Disconnected => {
                        disconnected = true;
                        reconnected = false;
                        can_poll.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        if !is_consumer_group {
                            can_poll.store(true, ORDERING);
                            continue;
                        }

                        if !can_join_consumer_group {
                            can_poll.store(true, ORDERING);
                            trace!("Auto join consumer group is disabled");
                            continue;
                        }

                        if !reconnected {
                            can_poll.store(true, ORDERING);
                            continue;
                        }

                        info!("Rejoining consumer group");
                        if let Err(error) = Self::initialize_consumer_group(
                            client.clone(),
                            create_consumer_group_if_not_exists,
                            stream_id.clone(),
                            topic_id.clone(),
                            consumer.clone(),
                            &consumer_name,
                        )
                        .await
                        {
                            error!("Failed to join consumer group: {error}");
                            continue;
                        }
                        info!("Rejoined consumer group");
                        can_poll.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        can_poll.store(false, ORDERING);
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
        let interval = self.interval_micros;
        let last_polled_at = self.last_polled_at.clone();

        async move {
            if interval > 0 {
                Self::wait_before_polling(interval, last_polled_at.load(ORDERING)).await;
            }

            trace!("Sending poll messages request");
            last_polled_at.store(IggyTimestamp::now().into(), ORDERING);
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

    async fn wait_before_polling(interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before polling messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!("Waiting for {remaining} microseconds before polling messages... {interval} - {elapsed} = {remaining}");
        sleep(Duration::from_micros(remaining)).await;
    }

    async fn initialize_consumer_group(
        client: IggySharedMut<Box<dyn Client>>,
        create_consumer_group_if_not_exists: bool,
        stream_id: Arc<Identifier>,
        topic_id: Arc<Identifier>,
        consumer: Arc<Consumer>,
        consumer_name: &str,
    ) -> Result<(), IggyError> {
        let client = client.read().await;
        let (name, id) = match consumer.id.kind {
            IdKind::Numeric => (consumer_name.to_owned(), Some(consumer.id.get_u32_value()?)),
            IdKind::String => (consumer.id.get_string_value()?, None),
        };

        let consumer_group_id = name.to_owned().try_into()?;
        trace!("Validating consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}");
        if let Err(error) = client
            .get_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await
        {
            if !create_consumer_group_if_not_exists {
                error!("Consumer group does not exist and auto-creation is disabled.");
                return Err(error);
            }

            info!("Creating consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}");
            client
                .create_consumer_group(&stream_id, &topic_id, &name, id)
                .await?;
        }

        info!("Joining consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}",);
        if let Err(error) = client
            .join_consumer_group(&stream_id, &topic_id, &consumer_group_id)
            .await
        {
            error!("Failed to join consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}: {error}");
            return Err(error);
        }

        info!(
            "Joined consumer group: {consumer_group_id} for topic: {topic_id}, stream: {stream_id}"
        );
        Ok(())
    }
}

pub struct ReceivedMessage {
    pub message: PolledMessage,
    pub current_offset: u64,
    pub partition_id: u32,
}

impl ReceivedMessage {
    pub fn new(message: PolledMessage, current_offset: u64, partition_id: u32) -> Self {
        Self {
            message,
            current_offset,
            partition_id,
        }
    }
}

impl Stream for IggyConsumer {
    type Item = Result<ReceivedMessage, IggyError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(message) = self.buffered_messages.pop_front() {
            let next_offset = message.offset + 1;
            self.next_offset.store(next_offset, ORDERING);
            let consumed_messages_count = self.polled_messages_count.fetch_add(1, ORDERING);
            if self.store_after_every_nth_message > 0
                && consumed_messages_count % self.store_after_every_nth_message == 0
            {
                self.store_offset(message.offset);
            }

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

            return Poll::Ready(Some(Ok(ReceivedMessage::new(
                message,
                self.current_offset.load(ORDERING),
                self.current_partition_id.load(ORDERING),
            ))));
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
                        self.current_offset
                            .store(polled_messages.current_offset, ORDERING);
                        self.current_partition_id
                            .store(polled_messages.partition_id, ORDERING);

                        let message = polled_messages.messages.remove(0);
                        let next_offset = message.offset + 1;
                        self.next_offset.store(next_offset, ORDERING);
                        self.buffered_messages.extend(polled_messages.messages);

                        if self.polling_strategy.kind == PollingKind::Offset {
                            self.polling_strategy = PollingStrategy::offset(next_offset);
                        }

                        let consumed_messages_count =
                            self.polled_messages_count.fetch_add(1, ORDERING);
                        if self.store_after_every_nth_message > 0
                            && consumed_messages_count % self.store_after_every_nth_message == 0
                        {
                            self.store_offset(message.offset);
                        } else if self.buffered_messages.is_empty() {
                            if self.store_offset_after_all_messages {
                                self.store_offset(message.offset);
                            }
                        } else if self.store_offset_after_each_message {
                            self.store_offset(message.offset);
                        }

                        self.poll_future = None;
                        return Poll::Ready(Some(Ok(ReceivedMessage::new(
                            message,
                            self.current_offset.load(ORDERING),
                            self.current_partition_id.load(ORDERING),
                        ))));
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
            auto_commit: AutoCommit::IntervalOrAfter(
                IggyDuration::from(SEC_IN_MICRO),
                AutoCommitAfter::PollingMessages,
            ),
            auto_join_consumer_group: true,
            create_consumer_group_if_not_exists: true,
            encryptor,
            polling_interval,
        }
    }

    /// Sets the stream identifier.
    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    /// Sets the topic identifier.
    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    /// Sets the partition identifier.
    pub fn partition(self, partition: Option<u32>) -> Self {
        Self { partition, ..self }
    }

    /// Sets the polling strategy.
    pub fn polling_strategy(self, polling_strategy: PollingStrategy) -> Self {
        Self {
            polling_strategy,
            ..self
        }
    }

    /// Sets the batch size for polling messages.
    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    /// Sets the auto-commit configuration for storing the offset on the server.
    pub fn auto_commit(self, auto_commit: AutoCommit) -> Self {
        Self {
            auto_commit,
            ..self
        }
    }

    /// Automatically joins the consumer group if the consumer is a part of a consumer group.
    pub fn auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: true,
            ..self
        }
    }

    /// Does not automatically join the consumer group if the consumer is a part of a consumer group.
    pub fn do_not_auto_join_consumer_group(self) -> Self {
        Self {
            auto_join_consumer_group: false,
            ..self
        }
    }

    /// Automatically creates the consumer group if it does not exist.
    pub fn create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: true,
            ..self
        }
    }

    /// Does not automatically create the consumer group if it does not exist.
    pub fn do_not_create_consumer_group_if_not_exists(self) -> Self {
        Self {
            create_consumer_group_if_not_exists: false,
            ..self
        }
    }

    /// Sets the polling interval for messages.
    pub fn poll_interval(self, interval: IggyDuration) -> Self {
        Self {
            polling_interval: Some(interval),
            ..self
        }
    }

    /// Clears the polling interval for messages.
    pub fn without_poll_interval(self) -> Self {
        Self {
            polling_interval: None,
            ..self
        }
    }

    /// Sets the encryptor for decrypting the messages' payloads.
    pub fn encryptor(self, encryptor: Arc<dyn Encryptor>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    /// Clears the encryptor for decrypting the messages' payloads.
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
