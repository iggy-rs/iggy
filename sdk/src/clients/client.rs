use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    PersonalAccessTokenClient, StreamClient, SystemClient, TopicClient, UserClient,
};
use crate::consumer::Consumer;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::IggySharedMut;
use crate::locking::IggySharedMutFn;
use crate::message_handler::MessageHandler;
use crate::messages::send_messages::{Message, Partitioning, PartitioningKind, SendMessages};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::messages::{PolledMessage, PolledMessages};
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::partitioner::Partitioner;
use crate::tcp::client::TcpClient;
use crate::utils::crypto::Encryptor;
use async_dropper::AsyncDrop;
use async_trait::async_trait;
use bytes::Bytes;
use flume::{Receiver, Sender};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info, warn};

use crate::clients::builder::IggyClientBuilder;
use crate::clients::consumer::IggyConsumer;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::messages::poll_messages::{PollingKind, PollingStrategy};
use crate::models::permissions::Permissions;
use crate::models::user_status::UserStatus;
use crate::utils::expiry::IggyExpiry;
use crate::utils::personal_access_token_expiry::PersonalAccessTokenExpiry;
use crate::utils::topic_size::MaxTopicSize;

// The default interval between sending the messages as batches in the background.
pub const DEFAULT_SEND_MESSAGES_INTERVAL_MS: u64 = 100;

// The default interval between polling the messages in the background.
pub const DEFAULT_POLL_MESSAGES_INTERVAL_MS: u64 = 100;

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
///
/// It also provides additional functionality (outside the shared trait) like sending messages in background, partitioning, client-side encryption or message handling via channels.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IggyClient {
    client: IggySharedMut<Box<dyn Client>>,
    config: Option<IggyClientBackgroundConfig>,
    send_messages_batch: Option<Arc<Mutex<SendMessagesBatch>>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    encryptor: Option<Arc<dyn Encryptor>>,
    message_handler: Option<Arc<dyn MessageHandler>>,
    message_channel_sender: Option<Arc<Sender<PolledMessage>>>,
}

#[derive(Debug)]
pub struct IggyConsumerBuilder {
    client: IggySharedMut<Box<dyn Client>>,
    consumer: Consumer,
    stream_id: Identifier,
    topic_id: Identifier,
    partition_id: Option<u32>,
    polling_strategy: PollingStrategy,
    batch_size: u32,
    auto_commit: bool,
    encryptor: Option<Arc<dyn Encryptor>>,
}

impl IggyConsumerBuilder {
    fn new(
        client: IggySharedMut<Box<dyn Client>>,
        consumer: Consumer,
        stream_id: Identifier,
        topic_id: Identifier,
        partition_id: Option<u32>,
        encryptor: Option<Arc<dyn Encryptor>>,
    ) -> Self {
        IggyConsumerBuilder {
            client,
            consumer,
            stream_id,
            topic_id,
            partition_id,
            polling_strategy: PollingStrategy::next(),
            batch_size: 1000,
            auto_commit: true,
            encryptor,
        }
    }

    pub fn stream(self, stream_id: Identifier) -> Self {
        Self { stream_id, ..self }
    }

    pub fn topic(self, topic_id: Identifier) -> Self {
        Self { topic_id, ..self }
    }

    pub fn partition(self, partition_id: Option<u32>) -> Self {
        Self {
            partition_id,
            ..self
        }
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

    pub fn encryptor(self, encryptor: Option<Arc<dyn Encryptor>>) -> Self {
        Self { encryptor, ..self }
    }

    pub fn build(self) -> IggyConsumer {
        IggyConsumer::new(
            self.client,
            self.consumer,
            self.stream_id,
            self.topic_id,
            self.partition_id,
            self.polling_strategy,
            self.batch_size,
            self.auto_commit,
            self.encryptor,
        )
    }
}

#[derive(Debug)]
struct SendMessagesBatch {
    pub commands: VecDeque<SendMessages>,
}

/// The optional configuration for the `IggyClient` instance, consisting of the optional configuration for sending and polling the messages in the background.
#[derive(Debug, Default)]
pub struct IggyClientBackgroundConfig {
    /// The configuration for sending the messages in the background.
    pub send_messages: SendMessagesConfig,
    /// The configuration for polling the messages in the background.
    pub poll_messages: PollMessagesConfig,
}

/// The configuration for sending the messages in the background. It allows to configure the interval between sending the messages as batches in the background and the maximum number of messages in the batch.
#[derive(Debug)]
pub struct SendMessagesConfig {
    /// Whether the sending messages as batches in the background is enabled. Interval must be greater than 0.
    pub enabled: bool,
    /// The interval in milliseconds between sending the messages as batches in the background.
    pub interval: u64,
    /// The maximum number of messages in the batch.
    pub max_messages: u32,
}

/// The configuration for polling the messages in the background. It allows to configure the interval between polling the messages and the offset storing strategy.
#[derive(Debug, Copy, Clone)]
pub struct PollMessagesConfig {
    /// The interval in milliseconds between polling the messages.
    pub interval: u64,
    /// The offset storing strategy.
    pub store_offset_kind: StoreOffsetKind,
}

/// The consumer offset storing strategy on the server.
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum StoreOffsetKind {
    /// The offset is never stored on the server.
    Never,
    /// The offset is stored on the server when the messages are received.
    WhenMessagesAreReceived,
    /// The offset is stored on the server when the messages are processed.
    WhenMessagesAreProcessed,
    /// The offset is stored on the server after processing each message.
    AfterProcessingEachMessage,
}

impl Default for SendMessagesConfig {
    fn default() -> Self {
        SendMessagesConfig {
            enabled: false,
            interval: DEFAULT_SEND_MESSAGES_INTERVAL_MS,
            max_messages: 1000,
        }
    }
}

impl Default for PollMessagesConfig {
    fn default() -> Self {
        PollMessagesConfig {
            interval: DEFAULT_POLL_MESSAGES_INTERVAL_MS,
            store_offset_kind: StoreOffsetKind::WhenMessagesAreProcessed,
        }
    }
}

impl Default for IggyClient {
    fn default() -> Self {
        IggyClient::new(Box::<TcpClient>::default())
    }
}

impl IggyClient {
    /// Creates a new `IggyClientBuilder`.
    pub fn builder() -> IggyClientBuilder {
        IggyClientBuilder::new()
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport.
    pub fn new(client: Box<dyn Client>) -> Self {
        let client = IggySharedMut::new(client);
        IggyClient {
            client,
            config: None,
            send_messages_batch: None,
            partitioner: None,
            encryptor: None,
            message_handler: None,
            message_channel_sender: None,
        }
    }

    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
        ))
    }

    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, IggyError> {
        Ok(IggyConsumerBuilder::new(
            self.client.clone(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
        ))
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport and the optional configuration for sending and polling the messages in the background.
    /// Additionally, it allows to provide the custom implementations for the message handler, partitioner and encryptor.
    pub fn create(
        client: Box<dyn Client>,
        config: IggyClientBackgroundConfig,
        message_handler: Option<Arc<dyn MessageHandler>>,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<dyn Encryptor>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = IggySharedMut::new(client);
        let send_messages_batch = Arc::new(Mutex::new(SendMessagesBatch {
            commands: VecDeque::new(),
        }));
        if config.send_messages.enabled && config.send_messages.interval > 0 {
            info!("Messages will be sent in background.");
            Self::send_messages_in_background(
                config.send_messages.interval,
                config.send_messages.max_messages,
                client.clone(),
                send_messages_batch.clone(),
            );
        }

        IggyClient {
            client,
            config: Some(config),
            send_messages_batch: Some(send_messages_batch),
            message_handler,
            message_channel_sender: None,
            partitioner,
            encryptor,
        }
    }

    /// Returns the channel receiver for the messages which are polled in the background. This will only work if the `start_polling_messages` method is called.
    pub fn subscribe_to_polled_messages(&mut self) -> Receiver<PolledMessage> {
        let (sender, receiver) = flume::unbounded();
        self.message_channel_sender = Some(Arc::new(sender));
        receiver
    }

    /// Starts polling the messages in the background. It returns the `JoinHandle` which can be used to await for the completion of the task.
    #[allow(clippy::too_many_arguments)]
    pub fn start_polling_messages<F>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        on_message: Option<F>,
        config_override: Option<PollMessagesConfig>,
    ) -> JoinHandle<()>
    where
        F: Fn(PolledMessage) + Send + Sync + 'static,
    {
        let client = self.client.clone();
        let mut interval = Duration::from_millis(DEFAULT_POLL_MESSAGES_INTERVAL_MS);
        let message_handler = self.message_handler.clone();
        let message_channel_sender = self.message_channel_sender.clone();
        let mut store_offset_after_processing_each_message = false;
        let mut store_offset_when_messages_are_processed = false;
        let mut auto_commit = false;
        let mut strategy = *strategy;
        let consumer = consumer.clone();
        let stream_id = stream_id.clone();
        let topic_id = topic_id.clone();

        let config = match config_override {
            Some(config) => Some(config),
            None => self.config.as_ref().map(|config| config.poll_messages),
        };
        if let Some(config) = config {
            if config.interval > 0 {
                interval = Duration::from_millis(config.interval);
            }
            match config.store_offset_kind {
                StoreOffsetKind::Never => {
                    auto_commit = false;
                }
                StoreOffsetKind::WhenMessagesAreReceived => {
                    auto_commit = true;
                }
                StoreOffsetKind::WhenMessagesAreProcessed => {
                    auto_commit = false;
                    store_offset_when_messages_are_processed = true;
                }
                StoreOffsetKind::AfterProcessingEachMessage => {
                    auto_commit = false;
                    store_offset_after_processing_each_message = true;
                }
            }
        }

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let client = client.read().await;
                let polled_messages = client
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        partition_id,
                        &consumer,
                        &strategy,
                        count,
                        auto_commit,
                    )
                    .await;
                if let Err(error) = polled_messages {
                    error!("There was an error while polling messages: {:?}", error);
                    continue;
                }

                let messages = polled_messages.unwrap().messages;
                if messages.is_empty() {
                    continue;
                }

                let mut current_offset = 0;
                for message in messages {
                    current_offset = message.offset;
                    // Send a message to the subscribed channel (if created), otherwise to the provided closure or message handler.
                    if let Some(sender) = &message_channel_sender {
                        if sender.send_async(message).await.is_err() {
                            error!("Error when sending a message to the channel.");
                        }
                    } else if let Some(on_message) = &on_message {
                        on_message(message);
                    } else if let Some(message_handler) = &message_handler {
                        message_handler.handle(message);
                    } else {
                        warn!("Received a message with ID: {} at offset: {} which won't be processed. Consider providing the custom `MessageHandler` trait implementation or `on_message` closure.", message.id, message.offset);
                    }
                    if store_offset_after_processing_each_message {
                        if let Err(error) = client
                            .store_consumer_offset(
                                &consumer,
                                &stream_id,
                                &topic_id,
                                partition_id,
                                current_offset,
                            )
                            .await
                        {
                            error!("There was an error while storing offset: {:?}", error);
                        }
                    }
                }

                if store_offset_when_messages_are_processed {
                    if let Err(error) = client
                        .store_consumer_offset(
                            &consumer,
                            &stream_id,
                            &topic_id,
                            partition_id,
                            current_offset,
                        )
                        .await
                    {
                        error!("There was an error while storing offset: {:?}", error);
                    }
                }

                if strategy.kind == PollingKind::Offset {
                    strategy.value = current_offset + 1;
                }
            }
        })
    }

    /// Sends the provided messages in the background using the custom partitioner implementation.
    pub async fn send_messages_using_partitioner(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
        partitioner: &dyn Partitioner,
    ) -> Result<(), IggyError> {
        let partition_id =
            partitioner.calculate_partition_id(stream_id, topic_id, partitioning, messages)?;
        let partitioning = Partitioning::partition_id(partition_id);
        self.send_messages(stream_id, topic_id, &partitioning, messages)
            .await
    }

    fn send_messages_in_background(
        interval: u64,
        max_messages: u32,
        client: IggySharedMut<Box<dyn Client>>,
        send_messages_batch: Arc<Mutex<SendMessagesBatch>>,
    ) {
        tokio::spawn(async move {
            let max_messages = max_messages as usize;
            let interval = Duration::from_millis(interval);
            loop {
                sleep(interval).await;
                let mut send_messages_batch = send_messages_batch.lock().await;
                if send_messages_batch.commands.is_empty() {
                    continue;
                }

                let mut initialized = false;
                let mut stream_id = Identifier::default();
                let mut topic_id = Identifier::default();
                let mut key = Partitioning::partition_id(1);
                let mut batch_messages = true;

                for send_messages in &send_messages_batch.commands {
                    if !initialized {
                        if send_messages.partitioning.kind != PartitioningKind::PartitionId {
                            batch_messages = false;
                            break;
                        }

                        stream_id = Identifier::from_identifier(&send_messages.stream_id);
                        topic_id = Identifier::from_identifier(&send_messages.topic_id);
                        key.value.clone_from(&send_messages.partitioning.value);
                        initialized = true;
                    }

                    // Batching the messages is only possible for the same stream, topic and partition.
                    if send_messages.stream_id != stream_id
                        || send_messages.topic_id != topic_id
                        || send_messages.partitioning.kind != PartitioningKind::PartitionId
                        || send_messages.partitioning.value != key.value
                    {
                        batch_messages = false;
                        break;
                    }
                }

                if !batch_messages {
                    for send_messages in &mut send_messages_batch.commands {
                        if let Err(error) = client
                            .read()
                            .await
                            .send_messages(
                                &send_messages.stream_id,
                                &send_messages.topic_id,
                                &send_messages.partitioning,
                                &mut send_messages.messages,
                            )
                            .await
                        {
                            error!("There was an error when sending the messages: {:?}", error);
                        }
                    }
                    send_messages_batch.commands.clear();
                    continue;
                }

                let mut batches = VecDeque::new();
                let mut messages = Vec::new();
                while let Some(send_messages) = send_messages_batch.commands.pop_front() {
                    messages.extend(send_messages.messages);
                    if messages.len() >= max_messages {
                        batches.push_back(messages);
                        messages = Vec::new();
                    }
                }

                if !messages.is_empty() {
                    batches.push_back(messages);
                }

                while let Some(messages) = batches.pop_front() {
                    let mut send_messages = SendMessages {
                        stream_id: Identifier::from_identifier(&stream_id),
                        topic_id: Identifier::from_identifier(&topic_id),
                        partitioning: Partitioning {
                            kind: PartitioningKind::PartitionId,
                            length: 4,
                            value: key.value.clone(),
                        },
                        messages,
                    };

                    if let Err(error) = client
                        .read()
                        .await
                        .send_messages(
                            &send_messages.stream_id,
                            &send_messages.topic_id,
                            &send_messages.partitioning,
                            &mut send_messages.messages,
                        )
                        .await
                    {
                        error!(
                            "There was an error when sending the messages batch: {:?}",
                            error
                        );

                        if !send_messages.messages.is_empty() {
                            batches.push_back(send_messages.messages);
                        }
                    }
                }

                send_messages_batch.commands.clear();
            }
        });
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&self) -> Result<(), IggyError> {
        self.client.read().await.connect().await
    }

    async fn disconnect(&self) -> Result<(), IggyError> {
        self.client.read().await.disconnect().await
    }
}

#[async_trait]
impl UserClient for IggyClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<UserInfoDetails, IggyError> {
        self.client.read().await.get_user(user_id).await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        self.client.read().await.get_users().await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_user(username, password, status, permissions)
            .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.delete_user(user_id).await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_user(user_id, username, status)
            .await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_permissions(user_id, permissions)
            .await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .change_password(user_id, current_password, new_password)
            .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_user(username, password)
            .await
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.client.read().await.logout_user().await
    }
}

#[async_trait]
impl PersonalAccessTokenClient for IggyClient {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        self.client.read().await.get_personal_access_tokens().await
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        self.client
            .read()
            .await
            .create_personal_access_token(name, expiry)
            .await
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_personal_access_token(name)
            .await
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_with_personal_access_token(token)
            .await
    }
}

#[async_trait]
impl SystemClient for IggyClient {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        self.client.read().await.get_stats().await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_me().await
    }

    async fn get_client(&self, client_id: u32) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_client(client_id).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        self.client.read().await.get_clients().await
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.client.read().await.ping().await
    }
}

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<StreamDetails, IggyError> {
        self.client.read().await.get_stream(stream_id).await
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, IggyError> {
        self.client.read().await.get_streams().await
    }

    async fn create_stream(&self, name: &str, stream_id: Option<u32>) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_stream(name, stream_id)
            .await
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_stream(stream_id, name)
            .await
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.delete_stream(stream_id).await
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        self.client.read().await.purge_stream(stream_id).await
    }
}

#[async_trait]
impl TopicClient for IggyClient {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicDetails, IggyError> {
        self.client
            .read()
            .await
            .get_topic(stream_id, topic_id)
            .await
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        self.client.read().await.get_topics(stream_id).await
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_topic(
                stream_id,
                name,
                partitions_count,
                compression_algorithm,
                replication_factor,
                topic_id,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .update_topic(
                stream_id,
                topic_id,
                name,
                compression_algorithm,
                replication_factor,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_topic(stream_id, topic_id)
            .await
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .purge_topic(stream_id, topic_id)
            .await
    }
}

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_partitions(stream_id, topic_id, partitions_count)
            .await
    }

    async fn delete_partitions(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_partitions(stream_id, topic_id, partitions_count)
            .await
    }
}

#[async_trait]
impl MessageClient for IggyClient {
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Result<PolledMessages, IggyError> {
        if count == 0 {
            return Err(IggyError::InvalidMessagesCount);
        }

        let mut polled_messages = self
            .client
            .read()
            .await
            .poll_messages(
                stream_id,
                topic_id,
                partition_id,
                consumer,
                strategy,
                count,
                auto_commit,
            )
            .await?;

        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
            }
        }

        Ok(polled_messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [Message],
    ) -> Result<(), IggyError> {
        if messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        self.client
            .read()
            .await
            .send_messages(stream_id, topic_id, partitioning, messages)
            .await
    }
}

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .store_consumer_offset(consumer, stream_id, topic_id, partition_id, offset)
            .await
    }

    async fn get_consumer_offset(
        &self,
        consumer: &Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_offset(consumer, stream_id, topic_id, partition_id)
            .await
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.client
            .read()
            .await
            .get_consumer_groups(stream_id, topic_id)
            .await
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        self.client
            .read()
            .await
            .create_consumer_group(stream_id, topic_id, name, group_id)
            .await
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .join_consumer_group(stream_id, topic_id, group_id)
            .await
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .leave_consumer_group(stream_id, topic_id, group_id)
            .await
    }
}

#[async_trait]
impl AsyncDrop for IggyClient {
    async fn async_drop(&mut self) {
        let _ = self.client.read().await.logout_user().await;
    }
}
