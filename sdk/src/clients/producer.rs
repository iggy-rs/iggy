use crate::client::Client;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::send_messages::{Message, Partitioning};
use crate::partitioner::Partitioner;
use crate::utils::crypto::Encryptor;
use crate::utils::duration::IggyDuration;
use crate::utils::expiry::IggyExpiry;
use crate::utils::timestamp::IggyTimestamp;
use crate::utils::topic_size::MaxTopicSize;
use bytes::Bytes;
use futures_util::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, trace, warn};

const ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;
const MAX_BATCH_SIZE: usize = 1000000;

unsafe impl Send for IggyProducer {}
unsafe impl Sync for IggyProducer {}

pub struct IggyProducer {
    initialized: bool,
    can_send: Arc<AtomicBool>,
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    stream_id: Arc<Identifier>,
    stream_name: String,
    topic_id: Arc<Identifier>,
    topic_name: String,
    batch_size: Option<usize>,
    partitioning: Option<Arc<Partitioning>>,
    encryptor: Option<Arc<dyn Encryptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    send_interval_micros: u64,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    topic_message_expiry: IggyExpiry,
    topic_max_size: MaxTopicSize,
    default_partitioning: Arc<Partitioning>,
    can_send_immediately: bool,
    last_sent_at: Arc<AtomicU64>,
    retry_interval: IggyDuration,
}

impl IggyProducer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        batch_size: Option<usize>,
        partitioning: Option<Partitioning>,
        encryptor: Option<Arc<dyn Encryptor>>,
        partitioner: Option<Arc<dyn Partitioner>>,
        interval: Option<IggyDuration>,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        topic_partitions_count: u32,
        topic_replication_factor: Option<u8>,
        topic_message_expiry: IggyExpiry,
        topic_max_size: MaxTopicSize,
        retry_interval: IggyDuration,
    ) -> Self {
        Self {
            initialized: false,
            client: Arc::new(client),
            can_send: Arc::new(AtomicBool::new(true)),
            stream_id: Arc::new(stream),
            stream_name,
            topic_id: Arc::new(topic),
            topic_name,
            batch_size,
            partitioning: partitioning.map(Arc::new),
            encryptor,
            partitioner,
            send_interval_micros: interval.map_or(0, |i| i.as_micros()),
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            topic_partitions_count,
            topic_replication_factor,
            topic_message_expiry,
            topic_max_size,
            default_partitioning: Arc::new(Partitioning::balanced()),
            can_send_immediately: interval.is_none(),
            last_sent_at: Arc::new(AtomicU64::new(0)),
            retry_interval,
        }
    }

    pub fn stream(&self) -> &Identifier {
        &self.stream_id
    }

    pub fn topic(&self) -> &Identifier {
        &self.topic_id
    }

    /// Initializes the producer by subscribing to diagnostic events, creating the stream and topic if they do not exist etc.
    pub async fn init(&mut self) -> Result<(), IggyError> {
        if self.initialized {
            return Ok(());
        }

        self.subscribe_events().await;
        let client = self.client.clone();
        let client = client.read().await;
        if client.get_stream(&self.stream_id).await?.is_none() {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(IggyError::StreamNameNotFound(self.stream_name.clone()));
            }

            let (name, id) = match self.stream_id.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream_id.get_u32_value()?),
                ),
                IdKind::String => (self.stream_id.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name, id).await?;
        }

        if client
            .get_topic(&self.stream_id, &self.topic_id)
            .await?
            .is_none()
        {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(IggyError::TopicNameNotFound(
                    self.topic_name.clone(),
                    self.stream_name.clone(),
                ));
            }

            let (name, id) = match self.topic_id.kind {
                IdKind::Numeric => (
                    self.topic_name.to_owned(),
                    Some(self.topic_id.get_u32_value()?),
                ),
                IdKind::String => (self.topic_id.get_string_value()?, None),
            };
            info!("Creating topic: {name} for stream: {}", self.stream_name);
            client
                .create_topic(
                    &self.stream_id,
                    &self.topic_name,
                    self.topic_partitions_count,
                    CompressionAlgorithm::None,
                    self.topic_replication_factor,
                    id,
                    self.topic_message_expiry,
                    self.topic_max_size,
                )
                .await?;
        }

        self.initialized = true;
        Ok(())
    }

    async fn subscribe_events(&self) {
        trace!("Subscribing to diagnostic events");
        let mut receiver;
        {
            let client = self.client.read().await;
            receiver = client.subscribe_events().await;
        }

        let can_send = self.can_send.clone();

        tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                trace!("Received diagnostic event: {event}");
                match event {
                    DiagnosticEvent::Shutdown => {
                        can_send.store(false, ORDERING);
                        warn!("Client has been shutdown");
                    }
                    DiagnosticEvent::Connected => {
                        can_send.store(false, ORDERING);
                        trace!("Connected to the server");
                    }
                    DiagnosticEvent::Disconnected => {
                        can_send.store(false, ORDERING);
                        warn!("Disconnected from the server");
                    }
                    DiagnosticEvent::SignedIn => {
                        can_send.store(true, ORDERING);
                    }
                    DiagnosticEvent::SignedOut => {
                        can_send.store(false, ORDERING);
                    }
                }
            }
        });
    }

    pub async fn send(&self, messages: Vec<Message>) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if !self.can_send.load(ORDERING) {
            trace!("Trying to send messages in {}...", self.retry_interval);
            sleep(self.retry_interval.get_duration()).await;
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, None)
                .await;
        }

        self.send_buffered(
            self.stream_id.clone(),
            self.topic_id.clone(),
            messages,
            None,
        )
        .await
    }

    pub async fn send_one(&self, message: Message) -> Result<(), IggyError> {
        self.send(vec![message]).await
    }

    pub async fn send_with_partitioning(
        &self,
        messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if !self.can_send.load(ORDERING) {
            trace!("Trying to send messages in {}...", self.retry_interval);
            sleep(self.retry_interval.get_duration()).await;
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, partitioning)
                .await;
        }

        self.send_buffered(
            self.stream_id.clone(),
            self.topic_id.clone(),
            messages,
            partitioning,
        )
        .await
    }

    pub async fn send_to(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            trace!("No messages to send.");
            return Ok(());
        }

        if !self.can_send.load(ORDERING) {
            trace!("Trying to send messages in {}...", self.retry_interval);
            sleep(self.retry_interval.get_duration()).await;
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream_id, &self.topic_id, messages, partitioning)
                .await;
        }

        self.send_buffered(stream, topic, messages, partitioning)
            .await
    }

    async fn send_buffered(
        &self,
        stream: Arc<Identifier>,
        topic: Arc<Identifier>,
        mut messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        self.encrypt_messages(&mut messages)?;
        let partitioning = self.get_partitioning(&stream, &topic, &messages, partitioning)?;
        let batch_size = self.batch_size.unwrap_or(MAX_BATCH_SIZE);
        let batches = messages.chunks_mut(batch_size);
        let mut current_batch = 1;
        let batches_count = batches.len();
        for batch in batches {
            if self.send_interval_micros > 0 {
                Self::wait_before_sending(
                    self.send_interval_micros,
                    self.last_sent_at.load(ORDERING),
                )
                .await;
            }

            let messages_count = batch.len();
            trace!(
                "Sending {messages_count} messages ({current_batch}/{batches_count} batch(es))..."
            );
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            let client = self.client.read().await;
            client
                .send_messages(&self.stream_id, &self.topic_id, &partitioning, batch)
                .await?;
            trace!("Sent {messages_count} messages ({current_batch}/{batches_count} batch(es)).");
            current_batch += 1;
        }
        Ok(())
    }

    async fn send_immediately(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        mut messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        trace!("No batch size specified, sending messages immediately.");
        self.encrypt_messages(&mut messages)?;
        let partitioning = self.get_partitioning(stream, topic, &messages, partitioning)?;
        let batch_size = self.batch_size.unwrap_or(MAX_BATCH_SIZE);
        let client = self.client.read().await;
        if messages.len() <= batch_size {
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            client
                .send_messages(stream, topic, &partitioning, &mut messages)
                .await?;
            return Ok(());
        }

        for batch in messages.chunks_mut(batch_size) {
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            client
                .send_messages(stream, topic, &partitioning, batch)
                .await?;
        }
        Ok(())
    }

    async fn wait_before_sending(interval: u64, last_sent_at: u64) {
        if interval == 0 {
            return;
        }

        let now: u64 = IggyTimestamp::now().into();
        let elapsed = now - last_sent_at;
        if elapsed >= interval {
            trace!("No need to wait before sending messages. {now} - {last_sent_at} = {elapsed}");
            return;
        }

        let remaining = interval - elapsed;
        trace!("Waiting for {remaining} microseconds before sending messages... {interval} - {elapsed} = {remaining}");
        sleep(Duration::from_micros(remaining)).await;
    }

    fn encrypt_messages(&self, messages: &mut [Message]) -> Result<(), IggyError> {
        if let Some(encryptor) = &self.encryptor {
            for message in messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.length = message.payload.len() as u32;
            }
        }
        Ok(())
    }

    fn get_partitioning(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        messages: &[Message],
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<Arc<Partitioning>, IggyError> {
        if let Some(partitioner) = &self.partitioner {
            trace!("Calculating partition id using custom partitioner.");
            let partition_id = partitioner.calculate_partition_id(stream, topic, messages)?;
            Ok(Arc::new(Partitioning::partition_id(partition_id)))
        } else {
            trace!("Using the provided partitioning.");
            Ok(partitioning.unwrap_or_else(|| {
                self.partitioning
                    .clone()
                    .unwrap_or_else(|| self.default_partitioning.clone())
            }))
        }
    }
}

#[derive(Debug)]
pub struct IggyProducerBuilder {
    client: IggySharedMut<Box<dyn Client>>,
    stream: Identifier,
    stream_name: String,
    topic: Identifier,
    topic_name: String,
    batch_size: Option<usize>,
    partitioning: Option<Partitioning>,
    encryptor: Option<Arc<dyn Encryptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    send_interval: Option<IggyDuration>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    retry_interval: IggyDuration,
    pub topic_message_expiry: IggyExpiry,
    pub topic_max_size: MaxTopicSize,
}

impl IggyProducerBuilder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: IggySharedMut<Box<dyn Client>>,
        stream: Identifier,
        stream_name: String,
        topic: Identifier,
        topic_name: String,
        encryptor: Option<Arc<dyn Encryptor>>,
        partitioner: Option<Arc<dyn Partitioner>>,
    ) -> Self {
        Self {
            client,
            stream,
            stream_name,
            topic,
            topic_name,
            batch_size: Some(1000),
            partitioning: None,
            encryptor,
            partitioner,
            send_interval: Some(IggyDuration::from(1000)),
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_partitions_count: 1,
            topic_replication_factor: None,
            retry_interval: IggyDuration::ONE_SECOND,
            topic_message_expiry: IggyExpiry::ServerDefault,
            topic_max_size: MaxTopicSize::ServerDefault,
        }
    }

    /// Sets the stream identifier.
    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    /// Sets the stream name.
    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    /// Sets the number of messages to batch before sending them, can be combined with `interval`.
    pub fn batch_size(self, batch_size: u32) -> Self {
        Self {
            batch_size: if batch_size == 0 {
                None
            } else {
                Some(batch_size.min(MAX_BATCH_SIZE as u32) as usize)
            },
            ..self
        }
    }

    /// Clears the batch size.
    pub fn without_batch_size(self) -> Self {
        Self {
            batch_size: None,
            ..self
        }
    }

    /// Sets the interval between sending the messages, can be combined with `batch_size`.
    pub fn send_interval(self, interval: IggyDuration) -> Self {
        Self {
            send_interval: Some(interval),
            ..self
        }
    }

    /// Clears the interval.
    pub fn without_send_interval(self) -> Self {
        Self {
            send_interval: None,
            ..self
        }
    }

    /// Sets the encryptor for encrypting the messages' payloads.
    pub fn encryptor(self, encryptor: Arc<dyn Encryptor>) -> Self {
        Self {
            encryptor: Some(encryptor),
            ..self
        }
    }

    /// Clears the encryptor for encrypting the messages' payloads.
    pub fn without_encryptor(self) -> Self {
        Self {
            encryptor: None,
            ..self
        }
    }

    /// Sets the partitioning strategy for messages.
    pub fn partitioning(self, partitioning: Partitioning) -> Self {
        Self {
            partitioning: Some(partitioning),
            ..self
        }
    }

    /// Clears the partitioning strategy.
    pub fn without_partitioning(self) -> Self {
        Self {
            partitioning: None,
            ..self
        }
    }

    /// Sets the partitioner for messages.
    pub fn partitioner(self, partitioner: Arc<dyn Partitioner>) -> Self {
        Self {
            partitioner: Some(partitioner),
            ..self
        }
    }

    /// Clears the partitioner.
    pub fn without_partitioner(self) -> Self {
        Self {
            partitioner: None,
            ..self
        }
    }

    /// Creates the stream if it does not exist - requires user to have the necessary permissions.
    pub fn create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: true,
            ..self
        }
    }

    /// Does not create the stream if it does not exist.
    pub fn do_not_create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: false,
            ..self
        }
    }

    /// Creates the topic if it does not exist - requires user to have the necessary permissions.
    pub fn create_topic_if_not_exists(
        self,
        partitions_count: u32,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_size: MaxTopicSize,
    ) -> Self {
        Self {
            create_topic_if_not_exists: true,
            topic_partitions_count: partitions_count,
            topic_replication_factor: replication_factor,
            topic_message_expiry: message_expiry,
            topic_max_size: max_size,
            ..self
        }
    }

    /// Does not create the topic if it does not exist.
    pub fn do_not_create_topic_if_not_exists(self) -> Self {
        Self {
            create_topic_if_not_exists: false,
            ..self
        }
    }

    /// Sets the retry interval in case of server disconnection.
    pub fn retry_interval(self, interval: IggyDuration) -> Self {
        Self {
            retry_interval: interval,
            ..self
        }
    }

    pub fn build(self) -> IggyProducer {
        IggyProducer::new(
            self.client,
            self.stream,
            self.stream_name,
            self.topic,
            self.topic_name,
            self.batch_size,
            self.partitioning,
            self.encryptor,
            self.partitioner,
            self.send_interval,
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
            self.topic_message_expiry,
            self.topic_max_size,
            self.retry_interval,
        )
    }
}
