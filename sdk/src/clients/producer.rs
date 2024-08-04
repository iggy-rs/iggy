use crate::client::Client;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::diagnostic::DiagnosticEvent;
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::send_messages::{Message, Partitioning};
use crate::partitioner::Partitioner;
use crate::utils::crypto::Encryptor;
use crate::utils::duration::{IggyDuration, SEC_IN_MICRO};
use crate::utils::expiry::IggyExpiry;
use crate::utils::timestamp::IggyTimestamp;
use crate::utils::topic_size::MaxTopicSize;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
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
    send_order: Arc<Mutex<VecDeque<Arc<Destination>>>>,
    buffered_messages: Arc<Mutex<HashMap<Arc<Destination>, MessagesBatch>>>,
    background_sender_initialized: bool,
    default_partitioning: Arc<Partitioning>,
    can_send_immediately: bool,
    last_sent_at: Arc<AtomicU64>,
    retry_interval: IggyDuration,
}

#[derive(Eq, PartialEq)]
struct Destination {
    stream: Arc<Identifier>,
    topic: Arc<Identifier>,
    partitioning: Arc<Partitioning>,
}

impl Hash for Destination {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stream.hash(state);
        self.topic.hash(state);
        self.partitioning.hash(state);
    }
}

struct MessagesBatch {
    destination: Arc<Destination>,
    messages: Vec<Message>,
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
            buffered_messages: Arc::new(Mutex::new(HashMap::new())),
            send_order: Arc::new(Mutex::new(VecDeque::new())),
            background_sender_initialized: false,
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
        if let Err(error) = client.get_stream(&self.stream_id).await {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(error);
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

        if let Err(error) = client.get_topic(&self.stream_id, &self.topic_id).await {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(error);
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
                    IggyExpiry::ServerDefault,
                    MaxTopicSize::ServerDefault,
                )
                .await?;
        }

        if self.send_interval_micros == 0 {
            return Ok(());
        };

        if self.background_sender_initialized {
            return Ok(());
        }

        self.background_sender_initialized = true;
        let interval_micros = self.send_interval_micros;
        let client = self.client.clone();
        let send_order = self.send_order.clone();
        let buffered_messages = self.buffered_messages.clone();
        let last_sent_at = self.last_sent_at.clone();
        let retry_interval = self.retry_interval;

        tokio::spawn(async move {
            loop {
                if interval_micros > 0 {
                    Self::wait_before_sending(interval_micros, last_sent_at.load(ORDERING)).await;
                }

                let client = client.read().await;
                let mut buffered_messages = buffered_messages.lock().await;
                let mut send_order = send_order.lock().await;
                while let Some(partitioning) = send_order.pop_front() {
                    let Some(mut batch) = buffered_messages.remove(&partitioning) else {
                        continue;
                    };

                    let messages_count = batch.messages.len();
                    if messages_count == 0 {
                        continue;
                    }

                    trace!("Sending {messages_count} buffered messages in the background...");
                    last_sent_at.store(IggyTimestamp::now().into(), ORDERING);
                    if let Err(error) = client
                        .send_messages(
                            &batch.destination.stream,
                            &batch.destination.topic,
                            &batch.destination.partitioning,
                            &mut batch.messages,
                        )
                        .await
                    {
                        error!(
                            "Failed to send {messages_count} messages in the background: {error}"
                        );
                        send_order.push_front(partitioning.clone());
                        buffered_messages.insert(partitioning, batch);
                        if matches!(error, IggyError::Disconnected | IggyError::Unauthenticated) {
                            trace!("Retrying to send {messages_count} buffered messages in the background in {retry_interval}...");
                            sleep(retry_interval.get_duration()).await;
                        }
                        continue;
                    }

                    trace!("Sent {messages_count} buffered messages in the background.");
                }
            }
        });
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
        if !self.can_send.load(ORDERING) {
            trace!("Trying to send message in {}...", self.retry_interval);
            sleep(self.retry_interval.get_duration()).await;
        }

        self.send_immediately(&self.stream_id, &self.topic_id, vec![message], None)
            .await
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
        let destination = Arc::new(Destination {
            stream,
            topic,
            partitioning,
        });
        let mut buffered_messages = self.buffered_messages.lock().await;
        let messages_batch = buffered_messages.get_mut(&destination);
        if let Some(messages_batch) = messages_batch {
            let messages_count = messages.len();
            trace!("Extending buffer with {messages_count} messages...");
            messages_batch.messages.extend(messages);
            if messages_batch.messages.len() >= batch_size {
                let mut messages = messages_batch
                    .messages
                    .drain(..batch_size)
                    .collect::<Vec<_>>();
                let messages_count = messages.len();

                if self.send_interval_micros > 0 {
                    Self::wait_before_sending(
                        self.send_interval_micros,
                        self.last_sent_at.load(ORDERING),
                    )
                    .await;
                }

                trace!("Sending {messages_count} buffered messages...");
                self.last_sent_at
                    .store(IggyTimestamp::now().into(), ORDERING);
                let client = self.client.read().await;
                client
                    .send_messages(
                        &self.stream_id,
                        &self.topic_id,
                        &destination.partitioning,
                        &mut messages,
                    )
                    .await?;
                trace!("Sent {messages_count} buffered messages.");
            }

            if messages_batch.messages.is_empty() {
                trace!("Removing empty messages batch.");
                buffered_messages.remove(&destination);
                self.send_order.lock().await.retain(|p| p != &destination);
            }
            return Ok(());
        }

        if messages.len() >= batch_size {
            trace!(
                "Sending messages {} exceeding the batch size of {batch_size}...",
                messages.len()
            );
            let mut messages_batch = messages.drain(..batch_size).collect::<Vec<_>>();
            if self.send_interval_micros > 0 {
                Self::wait_before_sending(
                    self.send_interval_micros,
                    self.last_sent_at.load(ORDERING),
                )
                .await;
            }

            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            let client = self.client.read().await;
            client
                .send_messages(
                    &destination.stream,
                    &destination.topic,
                    &destination.partitioning,
                    &mut messages_batch,
                )
                .await?;
            trace!("Sent messages exceeding the batch size of {batch_size}.");
        }

        if messages.is_empty() {
            trace!("No messages to buffer.");
            return Ok(());
        }

        let messages_count = messages.len();
        trace!("Buffering {messages_count} messages...");
        self.send_order.lock().await.push_back(destination.clone());
        buffered_messages.insert(
            destination.clone(),
            MessagesBatch {
                destination,
                messages,
            },
        );
        trace!("Buffered {messages_count} messages.");
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

#[async_trait]
impl AsyncDrop for IggyProducer {
    async fn async_drop(&mut self) {
        let mut buffered_messages = self.buffered_messages.lock().await;
        if buffered_messages.is_empty() {
            return;
        }

        let client = self.client.read().await;
        let mut send_order = self.send_order.lock().await;
        while let Some(partitioning) = send_order.pop_front() {
            let Some(mut batch) = buffered_messages.remove(&partitioning) else {
                continue;
            };

            let messages_count = batch.messages.len();
            self.last_sent_at
                .store(IggyTimestamp::now().into(), ORDERING);
            trace!("Sending {messages_count} remaining messages...");
            if let Err(error) = client
                .send_messages(
                    &batch.destination.stream,
                    &batch.destination.topic,
                    &batch.destination.partitioning,
                    &mut batch.messages,
                )
                .await
            {
                error!("Failed to send {messages_count} remaining messages: {error}");
            }

            trace!("Sent {messages_count} remaining messages.");
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
            retry_interval: IggyDuration::from(SEC_IN_MICRO),
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
    ) -> Self {
        Self {
            create_topic_if_not_exists: true,
            topic_partitions_count: partitions_count,
            topic_replication_factor: replication_factor,
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
            self.retry_interval,
        )
    }
}
