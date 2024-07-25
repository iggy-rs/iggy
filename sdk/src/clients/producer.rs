use crate::client::Client;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::error::IggyError;
use crate::identifier::{IdKind, Identifier};
use crate::locking::{IggySharedMut, IggySharedMutFn};
use crate::messages::send_messages::{Message, Partitioning};
use crate::partitioner::Partitioner;
use crate::utils::crypto::Encryptor;
use crate::utils::duration::IggyDuration;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info};

const MAX_BATCH_SIZE: usize = 1000000;

pub struct IggyProducer {
    client: Arc<IggySharedMut<Box<dyn Client>>>,
    stream: Arc<Identifier>,
    stream_name: String,
    topic: Arc<Identifier>,
    topic_name: String,
    batch_size: Option<usize>,
    partitioning: Option<Arc<Partitioning>>,
    encryptor: Option<Arc<dyn Encryptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    interval: Option<IggyDuration>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
    send_order: Arc<Mutex<VecDeque<Arc<Destination>>>>,
    buffered_messages: Arc<Mutex<HashMap<Arc<Destination>, MessagesBatch>>>,
    background_sender_initialized: bool,
    default_partitioning: Arc<Partitioning>,
    can_send_immediately: bool,
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
    ) -> Self {
        Self {
            client: Arc::new(client),
            stream: Arc::new(stream),
            stream_name,
            topic: Arc::new(topic),
            topic_name,
            batch_size,
            partitioning: partitioning.map(Arc::new),
            encryptor,
            partitioner,
            interval,
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            topic_partitions_count,
            topic_replication_factor,
            buffered_messages: Arc::new(Mutex::new(HashMap::new())),
            send_order: Arc::new(Mutex::new(VecDeque::new())),
            background_sender_initialized: false,
            default_partitioning: Arc::new(Partitioning::balanced()),
            can_send_immediately: interval.is_none(),
        }
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        let client = self.client.clone();
        let client = client.read().await;
        if let Err(error) = client.get_stream(&self.stream).await {
            if !self.create_stream_if_not_exists {
                error!("Stream does not exist and auto-creation is disabled.");
                return Err(error);
            }

            let (name, id) = match self.stream.kind {
                IdKind::Numeric => (
                    self.stream_name.to_owned(),
                    Some(self.stream.get_u32_value()?),
                ),
                IdKind::String => (self.stream.get_string_value()?, None),
            };
            info!("Creating stream: {name}");
            client.create_stream(&name, id).await?;
        }

        if let Err(error) = client.get_topic(&self.stream, &self.topic).await {
            if !self.create_topic_if_not_exists {
                error!("Topic does not exist and auto-creation is disabled.");
                return Err(error);
            }

            let (name, id) = match self.topic.kind {
                IdKind::Numeric => (
                    self.topic_name.to_owned(),
                    Some(self.topic.get_u32_value()?),
                ),
                IdKind::String => (self.topic.get_string_value()?, None),
            };
            info!("Creating topic: {name} for stream: {}", self.stream_name);
            client
                .create_topic(
                    &self.stream,
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
        let Some(interval) = self.interval else {
            return Ok(());
        };

        if self.background_sender_initialized {
            return Ok(());
        }

        self.background_sender_initialized = true;
        let client = self.client.clone();
        let send_order = self.send_order.clone();
        let buffered_messages = self.buffered_messages.clone();
        tokio::spawn(async move {
            loop {
                sleep(interval.get_duration()).await;
                let client = client.read().await;
                let mut buffered_messages = buffered_messages.lock().await;
                let mut send_order = send_order.lock().await;
                while let Some(partitioning) = send_order.pop_front() {
                    let Some(mut batch) = buffered_messages.remove(&partitioning) else {
                        continue;
                    };

                    let messages_count = batch.messages.len();
                    info!("Sending {messages_count} buffered messages in the background...");
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
                        continue;
                    }

                    info!("Sent {messages_count} buffered messages in the background.");
                }
            }
        });
        Ok(())
    }

    pub async fn send(&self, messages: Vec<Message>) -> Result<(), IggyError> {
        if messages.is_empty() {
            info!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream, &self.topic, messages, None)
                .await;
        }

        self.send_buffered(self.stream.clone(), self.topic.clone(), messages, None)
            .await
    }

    pub async fn send_with_partitioning(
        &self,
        messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            info!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream, &self.topic, messages, partitioning)
                .await;
        }

        self.send_buffered(
            self.stream.clone(),
            self.topic.clone(),
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
            info!("No messages to send.");
            return Ok(());
        }

        if self.can_send_immediately {
            return self
                .send_immediately(&self.stream, &self.topic, messages, partitioning)
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
            info!("Extending buffer with {messages_count} messages...");
            messages_batch.messages.extend(messages);
            if messages_batch.messages.len() >= batch_size {
                let mut messages = messages_batch
                    .messages
                    .drain(..batch_size)
                    .collect::<Vec<_>>();
                let messages_count = messages.len();
                info!("Sending {messages_count} buffered messages...");
                let client = self.client.read().await;
                client
                    .send_messages(
                        &self.stream,
                        &self.topic,
                        &destination.partitioning,
                        &mut messages,
                    )
                    .await?;
                info!("Sent {messages_count} buffered messages.");
            }

            if messages_batch.messages.is_empty() {
                info!("Removing empty messages batch.");
                buffered_messages.remove(&destination);
                self.send_order.lock().await.retain(|p| p != &destination);
            }
            return Ok(());
        }

        if messages.len() >= batch_size {
            info!("Sending messages exceeding the batch size of {batch_size}...");
            let mut messages_batch = messages.drain(..batch_size).collect::<Vec<_>>();
            let client = self.client.read().await;
            client
                .send_messages(
                    &destination.stream,
                    &destination.topic,
                    &destination.partitioning,
                    &mut messages_batch,
                )
                .await?;
            info!("Sent messages exceeding the batch size of {batch_size}.");
        }

        if messages.is_empty() {
            info!("No messages to buffer.");
            return Ok(());
        }

        let messages_count = messages.len();
        info!("Buffering {messages_count} messages...");
        self.send_order.lock().await.push_back(destination.clone());
        buffered_messages.insert(
            destination.clone(),
            MessagesBatch {
                destination,
                messages,
            },
        );
        info!("Buffered {messages_count} messages.");
        Ok(())
    }

    async fn send_immediately(
        &self,
        stream: &Identifier,
        topic: &Identifier,
        mut messages: Vec<Message>,
        partitioning: Option<Arc<Partitioning>>,
    ) -> Result<(), IggyError> {
        info!("No batch size specified, sending messages immediately.");
        self.encrypt_messages(&mut messages)?;
        let partitioning = self.get_partitioning(stream, topic, &messages, partitioning)?;
        let batch_size = self.batch_size.unwrap_or(MAX_BATCH_SIZE);
        let client = self.client.read().await;
        if messages.len() <= batch_size {
            return client
                .send_messages(stream, topic, &partitioning, &mut messages)
                .await;
        }

        for batch in messages.chunks_mut(batch_size) {
            client
                .send_messages(stream, topic, &partitioning, batch)
                .await?;
        }
        Ok(())
    }

    fn encrypt_messages(&self, messages: &mut [Message]) -> Result<(), IggyError> {
        if let Some(encryptor) = &self.encryptor {
            for message in messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
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
            info!("Calculating partition id using custom partitioner.");
            let partition_id = partitioner.calculate_partition_id(stream, topic, messages)?;
            Ok(Arc::new(Partitioning::partition_id(partition_id)))
        } else {
            info!("Using the provided partitioning.");
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
            info!("Sending {messages_count} remaining messages...");
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

            info!("Sent {messages_count} remaining messages.");
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
    interval: Option<IggyDuration>,
    create_stream_if_not_exists: bool,
    create_topic_if_not_exists: bool,
    topic_partitions_count: u32,
    topic_replication_factor: Option<u8>,
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
            interval: Some(IggyDuration::from(1000)),
            create_stream_if_not_exists: true,
            create_topic_if_not_exists: true,
            topic_partitions_count: 1,
            topic_replication_factor: None,
        }
    }

    pub fn stream(self, stream: Identifier) -> Self {
        Self { stream, ..self }
    }

    pub fn topic(self, topic: Identifier) -> Self {
        Self { topic, ..self }
    }

    pub fn batch_size(self, batch_size: Option<u32>) -> Self {
        Self {
            batch_size: if batch_size.unwrap_or(0) == 0 {
                None
            } else {
                batch_size.map(|x| x.min(MAX_BATCH_SIZE as u32) as usize)
            },
            ..self
        }
    }

    pub fn interval(self, interval: Option<IggyDuration>) -> Self {
        Self { interval, ..self }
    }

    pub fn encryptor(self, encryptor: Option<Arc<dyn Encryptor>>) -> Self {
        Self { encryptor, ..self }
    }

    pub fn partitioning(self, partitioning: Option<Partitioning>) -> Self {
        Self {
            partitioning,
            ..self
        }
    }

    pub fn partitioner(self, partitioner: Option<Arc<dyn Partitioner>>) -> Self {
        Self {
            partitioner,
            ..self
        }
    }

    pub fn create_stream_if_not_exists(self) -> Self {
        Self {
            create_stream_if_not_exists: true,
            ..self
        }
    }

    pub fn create_topic_if_not_exists(self) -> Self {
        Self {
            create_topic_if_not_exists: true,
            ..self
        }
    }

    pub fn topic_partitions_count(self, topic_partitions_count: u32) -> Self {
        Self {
            topic_partitions_count,
            ..self
        }
    }

    pub fn topic_replication_factor(self, topic_replication_factor: Option<u8>) -> Self {
        Self {
            topic_replication_factor,
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
            self.interval,
            self.create_stream_if_not_exists,
            self.create_topic_if_not_exists,
            self.topic_partitions_count,
            self.topic_replication_factor,
        )
    }
}
