use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    PersonalAccessTokenClient, StreamClient, SystemClient, TopicClient, UserClient,
};
use crate::consumer::Consumer;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::locking::IggySharedMut;
use crate::locking::IggySharedMutFn;
use crate::message_handler::MessageHandler;
use crate::messages::poll_messages::{PollMessages, PollingKind};
use crate::messages::send_messages::{Partitioning, PartitioningKind, SendMessages};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::identity_info::IdentityInfo;
use crate::models::messages::{Message, PolledMessages};
use crate::models::personal_access_token::{PersonalAccessTokenInfo, RawPersonalAccessToken};
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::models::user_info::{UserInfo, UserInfoDetails};
use crate::partitioner::Partitioner;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use crate::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use crate::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use crate::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::purge_stream::PurgeStream;
use crate::streams::update_stream::UpdateStream;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use crate::tcp::client::TcpClient;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;
use crate::users::change_password::ChangePassword;
use crate::users::create_user::CreateUser;
use crate::users::delete_user::DeleteUser;
use crate::users::get_user::GetUser;
use crate::users::get_users::GetUsers;
use crate::users::login_user::LoginUser;
use crate::users::logout_user::LogoutUser;
use crate::users::update_permissions::UpdatePermissions;
use crate::users::update_user::UpdateUser;
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

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
/// It also provides additional functionality (outside of the shared trait) like sending messages in background, partitioning, client-side encryption or message handling via channels.
#[derive(Debug)]
pub struct IggyClient {
    client: IggySharedMut<Box<dyn Client>>,
    config: Option<IggyClientConfig>,
    send_messages_batch: Option<Arc<Mutex<SendMessagesBatch>>>,
    partitioner: Option<Box<dyn Partitioner>>,
    encryptor: Option<Box<dyn Encryptor>>,
    message_handler: Option<Arc<Box<dyn MessageHandler>>>,
    message_channel_sender: Option<Arc<Sender<Message>>>,
}

/// The builder for the `IggyClient` instance, which allows to configure and provide custom implementations for the partitioner, encryptor or message handler.
#[derive(Debug)]
pub struct IggyClientBuilder {
    client: IggyClient,
}

impl IggyClientBuilder {
    /// Creates a new `IggyClientBuilder` with the provided client implementation for the specific transport.
    #[must_use]
    pub fn new(client: Box<dyn Client>) -> Self {
        IggyClientBuilder {
            client: IggyClient::new(client),
        }
    }

    /// Apply the provided configuration.
    pub fn with_config(mut self, config: IggyClientConfig) -> Self {
        self.client.config = Some(config);
        self
    }

    /// Use the the custom partitioner implementation.
    pub fn with_partitioner(mut self, partitioner: Box<dyn Partitioner>) -> Self {
        self.client.partitioner = Some(partitioner);
        self
    }

    /// Use the the custom encryptor implementation.
    pub fn with_encryptor(mut self, encryptor: Box<dyn Encryptor>) -> Self {
        self.client.encryptor = Some(encryptor);
        self
    }

    /// Use the the custom message handler implementation. This handler will be used only for `start_polling_messages` method, if neither `subscribe_to_polled_messages` (which returns the receiver for the messages channel) is called nor `on_message` closure is provided.
    pub fn with_message_handler(mut self, message_handler: Box<dyn MessageHandler>) -> Self {
        self.client.message_handler = Some(Arc::new(message_handler));
        self
    }

    /// Build the `IggyClient` instance.
    pub fn build(self) -> IggyClient {
        self.client
    }
}

#[derive(Debug)]
struct SendMessagesBatch {
    pub commands: VecDeque<SendMessages>,
}

/// The optional configuration for the `IggyClient` instance, consisting of the optional configuration for sending and polling the messages in the background.
#[derive(Debug, Default)]
pub struct IggyClientConfig {
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
            interval: 100,
            max_messages: 1000,
        }
    }
}

impl Default for PollMessagesConfig {
    fn default() -> Self {
        PollMessagesConfig {
            interval: 100,
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
    /// Creates a new `IggyClientBuilder` with the provided client implementation for the specific transport.
    pub fn builder(client: Box<dyn Client>) -> IggyClientBuilder {
        IggyClientBuilder::new(client)
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport.
    pub fn new(client: Box<dyn Client>) -> Self {
        IggyClient {
            client: IggySharedMut::new(client),
            config: None,
            send_messages_batch: None,
            partitioner: None,
            encryptor: None,
            message_handler: None,
            message_channel_sender: None,
        }
    }

    /// Creates a new `IggyClient` with the provided client implementation for the specific transport and the optional configuration for sending and polling the messages in the background.
    /// Additionally it allows to provide the custom implementations for the message handler, partitioner and encryptor.
    pub fn create(
        client: Box<dyn Client>,
        config: IggyClientConfig,
        message_handler: Option<Box<dyn MessageHandler>>,
        partitioner: Option<Box<dyn Partitioner>>,
        encryptor: Option<Box<dyn Encryptor>>,
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
            message_handler: message_handler.map(Arc::new),
            message_channel_sender: None,
            partitioner,
            encryptor,
        }
    }

    /// Returns the channel receiver for the messages which are polled in the background. This will only work if the `start_polling_messages` method is called.
    pub fn subscribe_to_polled_messages(&mut self) -> Receiver<Message> {
        let (sender, receiver) = flume::unbounded();
        self.message_channel_sender = Some(Arc::new(sender));
        receiver
    }

    /// Starts polling the messages in the background. It returns the `JoinHandle` which can be used to await for the completion of the task.
    pub fn start_polling_messages<F>(
        &self,
        mut poll_messages: PollMessages,
        on_message: Option<F>,
        config_override: Option<PollMessagesConfig>,
    ) -> JoinHandle<()>
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        let client = self.client.clone();
        let mut interval = Duration::from_millis(100);
        let message_handler = self.message_handler.clone();
        let message_channel_sender = self.message_channel_sender.clone();
        let mut store_offset_after_processing_each_message = false;
        let mut store_offset_when_messages_are_processed = false;

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
                    poll_messages.auto_commit = false;
                }
                StoreOffsetKind::WhenMessagesAreReceived => {
                    poll_messages.auto_commit = true;
                }
                StoreOffsetKind::WhenMessagesAreProcessed => {
                    poll_messages.auto_commit = false;
                    store_offset_when_messages_are_processed = true;
                }
                StoreOffsetKind::AfterProcessingEachMessage => {
                    poll_messages.auto_commit = false;
                    store_offset_after_processing_each_message = true;
                }
            }
        }

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let client = client.read().await;
                let polled_messages = client.poll_messages(&poll_messages).await;
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
                        Self::store_offset(client.as_ref(), &poll_messages, current_offset).await;
                    }
                }

                if store_offset_when_messages_are_processed {
                    Self::store_offset(client.as_ref(), &poll_messages, current_offset).await;
                }

                if poll_messages.strategy.kind == PollingKind::Offset {
                    poll_messages.strategy.value = current_offset + 1;
                }
            }
        })
    }

    /// Sends the provided messages in the background using the custom partitioner implementation.
    pub async fn send_messages_using_partitioner(
        &self,
        command: &mut SendMessages,
        partitioner: &dyn Partitioner,
    ) -> Result<(), IggyError> {
        let partition_id = partitioner.calculate_partition_id(
            &command.stream_id,
            &command.topic_id,
            &command.partitioning,
            &command.messages,
        )?;
        command.partitioning = Partitioning::partition_id(partition_id);
        self.send_messages(command).await
    }

    async fn store_offset(client: &dyn Client, poll_messages: &PollMessages, offset: u64) {
        let result = client
            .store_consumer_offset(&StoreConsumerOffset {
                consumer: Consumer::from_consumer(&poll_messages.consumer),
                stream_id: Identifier::from_identifier(&poll_messages.stream_id),
                topic_id: Identifier::from_identifier(&poll_messages.topic_id),
                partition_id: poll_messages.partition_id,
                offset,
            })
            .await;
        if let Err(error) = result {
            error!("There was an error while storing offset: {:?}", error);
        }
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
                let mut stream_id = Identifier::numeric(1).unwrap();
                let mut topic_id = Identifier::numeric(1).unwrap();
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
                        key.value = send_messages.partitioning.value.clone();
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
                        if let Err(error) = client.read().await.send_messages(send_messages).await {
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

                    if let Err(error) = client.read().await.send_messages(&mut send_messages).await
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
impl UserClient for IggyClient {
    async fn get_user(&self, command: &GetUser) -> Result<UserInfoDetails, IggyError> {
        self.client.read().await.get_user(command).await
    }

    async fn get_users(&self, command: &GetUsers) -> Result<Vec<UserInfo>, IggyError> {
        self.client.read().await.get_users(command).await
    }

    async fn create_user(&self, command: &CreateUser) -> Result<(), IggyError> {
        self.client.read().await.create_user(command).await
    }

    async fn delete_user(&self, command: &DeleteUser) -> Result<(), IggyError> {
        self.client.read().await.delete_user(command).await
    }

    async fn update_user(&self, command: &UpdateUser) -> Result<(), IggyError> {
        self.client.read().await.update_user(command).await
    }

    async fn update_permissions(&self, command: &UpdatePermissions) -> Result<(), IggyError> {
        self.client.read().await.update_permissions(command).await
    }

    async fn change_password(&self, command: &ChangePassword) -> Result<(), IggyError> {
        self.client.read().await.change_password(command).await
    }

    async fn login_user(&self, command: &LoginUser) -> Result<IdentityInfo, IggyError> {
        self.client.read().await.login_user(command).await
    }

    async fn logout_user(&self, command: &LogoutUser) -> Result<(), IggyError> {
        self.client.read().await.logout_user(command).await
    }
}

#[async_trait]
impl PersonalAccessTokenClient for IggyClient {
    async fn get_personal_access_tokens(
        &self,
        command: &GetPersonalAccessTokens,
    ) -> Result<Vec<PersonalAccessTokenInfo>, IggyError> {
        self.client
            .read()
            .await
            .get_personal_access_tokens(command)
            .await
    }

    async fn create_personal_access_token(
        &self,
        command: &CreatePersonalAccessToken,
    ) -> Result<RawPersonalAccessToken, IggyError> {
        self.client
            .read()
            .await
            .create_personal_access_token(command)
            .await
    }

    async fn delete_personal_access_token(
        &self,
        command: &DeletePersonalAccessToken,
    ) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_personal_access_token(command)
            .await
    }

    async fn login_with_personal_access_token(
        &self,
        command: &LoginWithPersonalAccessToken,
    ) -> Result<IdentityInfo, IggyError> {
        self.client
            .read()
            .await
            .login_with_personal_access_token(command)
            .await
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
impl SystemClient for IggyClient {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, IggyError> {
        self.client.read().await.get_stats(command).await
    }

    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_me(command).await
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, IggyError> {
        self.client.read().await.get_client(command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, IggyError> {
        self.client.read().await.get_clients(command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), IggyError> {
        self.client.read().await.ping(command).await
    }
}

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, IggyError> {
        self.client.read().await.get_stream(command).await
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, IggyError> {
        self.client.read().await.get_streams(command).await
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), IggyError> {
        self.client.read().await.create_stream(command).await
    }

    async fn update_stream(&self, command: &UpdateStream) -> Result<(), IggyError> {
        self.client.read().await.update_stream(command).await
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), IggyError> {
        self.client.read().await.delete_stream(command).await
    }

    async fn purge_stream(&self, command: &PurgeStream) -> Result<(), IggyError> {
        self.client.read().await.purge_stream(command).await
    }
}

#[async_trait]
impl TopicClient for IggyClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, IggyError> {
        self.client.read().await.get_topic(command).await
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, IggyError> {
        self.client.read().await.get_topics(command).await
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), IggyError> {
        self.client.read().await.create_topic(command).await
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), IggyError> {
        self.client.read().await.update_topic(command).await
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), IggyError> {
        self.client.read().await.delete_topic(command).await
    }

    async fn purge_topic(&self, command: &PurgeTopic) -> Result<(), IggyError> {
        self.client.read().await.purge_topic(command).await
    }
}

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), IggyError> {
        self.client.read().await.create_partitions(command).await
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), IggyError> {
        self.client.read().await.delete_partitions(command).await
    }
}

#[async_trait]
impl MessageClient for IggyClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, IggyError> {
        let mut polled_messages = self.client.read().await.poll_messages(command).await?;
        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                let payload = encryptor.decrypt(&message.payload)?;
                message.payload = Bytes::from(payload);
            }
        }
        Ok(polled_messages)
    }

    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), IggyError> {
        if command.messages.is_empty() {
            return Ok(());
        }

        if let Some(partitioner) = &self.partitioner {
            let partition_id = partitioner.calculate_partition_id(
                &command.stream_id,
                &command.topic_id,
                &command.partitioning,
                &command.messages,
            )?;
            command.partitioning = Partitioning::partition_id(partition_id);
        }

        if let Some(encryptor) = &self.encryptor {
            for message in &mut command.messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.length = message.payload.len() as u32;
            }
        }

        let send_messages_now = self.send_messages_batch.is_none()
            || match &self.config {
                Some(config) => !config.send_messages.enabled || config.send_messages.interval == 0,
                None => true,
            };

        if send_messages_now {
            return self.client.read().await.send_messages(command).await;
        }

        let mut messages = Vec::with_capacity(command.messages.len());
        for message in &command.messages {
            let message = crate::messages::send_messages::Message {
                id: message.id,
                length: message.length,
                payload: message.payload.clone(),
                headers: message.headers.clone(),
            };
            messages.push(message);
        }
        let send_messages = SendMessages {
            stream_id: Identifier::from_identifier(&command.stream_id),
            topic_id: Identifier::from_identifier(&command.topic_id),
            partitioning: Partitioning::from_partitioning(&command.partitioning),
            messages,
        };

        let mut batch = self.send_messages_batch.as_ref().unwrap().lock().await;
        batch.commands.push_back(send_messages);
        Ok(())
    }
}

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .store_consumer_offset(command)
            .await
    }

    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, IggyError> {
        self.client.read().await.get_consumer_offset(command).await
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        self.client.read().await.get_consumer_group(command).await
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        self.client.read().await.get_consumer_groups(command).await
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .create_consumer_group(command)
            .await
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), IggyError> {
        self.client
            .read()
            .await
            .delete_consumer_group(command)
            .await
    }

    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), IggyError> {
        self.client.read().await.join_consumer_group(command).await
    }

    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), IggyError> {
        self.client.read().await.leave_consumer_group(command).await
    }
}

#[async_trait]
impl AsyncDrop for IggyClient {
    async fn async_drop(&mut self) {
        let _ = self.client.read().await.logout_user(&LogoutUser {}).await;
    }
}
