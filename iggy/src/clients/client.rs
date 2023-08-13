use crate::client::{
    Client, ConsumerGroupClient, ConsumerOffsetClient, MessageClient, PartitionClient,
    StreamClient, SystemClient, TopicClient,
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
use crate::error::Error;
use crate::identifier::Identifier;
use crate::messages::poll_messages::{PollMessages, PollingKind};
use crate::messages::send_messages::{Partitioning, PartitioningKind, SendMessages};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::partitioner::Partitioner;
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info};

pub struct IggyClient {
    client: Arc<RwLock<Box<dyn Client>>>,
    config: IggyClientConfig,
    send_messages_batch: Arc<Mutex<SendMessagesBatch>>,
    partitioner: Option<Box<dyn Partitioner>>,
    cipher: Option<Aes256Gcm>,
}

#[derive(Debug)]
struct SendMessagesBatch {
    pub commands: VecDeque<SendMessages>,
}

#[derive(Debug, Default)]
pub struct IggyClientConfig {
    pub send_messages: SendMessagesConfig,
    pub poll_messages: PollMessagesConfig,
}

#[derive(Debug)]
pub struct SendMessagesConfig {
    pub enabled: bool,
    pub interval: u64,
    pub max_messages: u32,
}

#[derive(Debug, Copy, Clone)]
pub struct PollMessagesConfig {
    pub interval: u64,
    pub store_offset_kind: StoreOffsetKind,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum StoreOffsetKind {
    Never,
    WhenMessagesAreReceived,
    WhenMessagesAreProcessed,
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

impl IggyClient {
    pub fn new(
        client: Box<dyn Client>,
        config: IggyClientConfig,
        partitioner: Option<Box<dyn Partitioner>>,
        encryption_key: Option<Vec<u8>>,
    ) -> Self {
        let client = Arc::new(RwLock::new(client));
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
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryption_key.is_some() {
            if encryption_key.as_ref().unwrap().len() != 32 {
                panic!("Encryption key must be 32 bytes long.");
            }
            info!("Client-side encryption is enabled.");
        }

        IggyClient {
            client,
            config,
            send_messages_batch,
            partitioner,
            cipher: encryption_key.map(|key| Aes256Gcm::new(key.as_slice().into())),
        }
    }

    pub fn start_polling_messages<F>(
        &self,
        mut poll_messages: PollMessages,
        on_message: F,
        config_override: Option<PollMessagesConfig>,
    ) -> JoinHandle<()>
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        let client = self.client.clone();
        let config = config_override.unwrap_or(self.config.poll_messages);
        let interval = Duration::from_millis(config.interval);
        let mut store_offset_after_processing_each_message = false;
        let mut store_offset_when_messages_are_processed = false;
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

        tokio::spawn(async move {
            loop {
                sleep(interval).await;
                let client = client.read().await;
                let messages = client.poll_messages(&poll_messages).await;
                if let Err(error) = messages {
                    error!("There was an error while polling messages: {:?}", error);
                    continue;
                }

                let messages = messages.unwrap();
                if messages.is_empty() {
                    continue;
                }

                let mut current_offset = 0;
                for message in messages {
                    current_offset = message.offset;
                    on_message(message);
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

    pub async fn send_messages_using_partitioner(
        &self,
        command: &mut SendMessages,
        partitioner: &dyn Partitioner,
    ) -> Result<(), Error> {
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
        client: Arc<RwLock<Box<dyn Client>>>,
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
                let mut key = Partitioning::partition_id(0);
                let mut batch_messages = true;

                for send_messages in send_messages_batch.commands.iter() {
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
                    for send_messages in send_messages_batch.commands.iter() {
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

                while let Some(messages) = batches.pop_front() {
                    let send_messages = SendMessages {
                        stream_id: Identifier::from_identifier(&stream_id),
                        topic_id: Identifier::from_identifier(&topic_id),
                        partitioning: Partitioning {
                            kind: PartitioningKind::PartitionId,
                            length: 4,
                            value: key.value.clone(),
                        },
                        messages,
                    };

                    if let Err(error) = client.read().await.send_messages(&send_messages).await {
                        error!(
                            "There was an error when sending the messages batch: {:?}",
                            error
                        );
                    }
                }

                send_messages_batch.commands.clear();
            }
        });
    }

    fn map_send_messages(
        &self,
        command: &SendMessages,
        partitioning: Partitioning,
    ) -> SendMessages {
        SendMessages {
            stream_id: Identifier::from_identifier(&command.stream_id),
            topic_id: Identifier::from_identifier(&command.topic_id),
            partitioning,
            messages: command
                .messages
                .iter()
                .map(|message| {
                    if self.cipher.is_none() {
                        crate::messages::send_messages::Message {
                            id: message.id,
                            length: message.length,
                            payload: message.payload.clone(),
                        }
                    } else {
                        let cipher = self.cipher.as_ref().unwrap();
                        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
                        let encrypted_payload =
                            cipher.encrypt(&nonce, message.payload.as_ref()).unwrap();
                        let payload = [&nonce, encrypted_payload.as_slice()].concat();
                        crate::messages::send_messages::Message {
                            id: message.id,
                            length: payload.len() as u32,
                            payload: Bytes::from(payload),
                        }
                    }
                })
                .collect(),
        }
    }
}

impl Debug for IggyClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IggyClient").finish()
    }
}

#[async_trait]
impl Client for IggyClient {
    async fn connect(&mut self) -> Result<(), Error> {
        self.client.write().await.connect().await
    }

    async fn disconnect(&mut self) -> Result<(), Error> {
        self.client.write().await.disconnect().await
    }
}

#[async_trait]
impl SystemClient for IggyClient {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error> {
        self.client.read().await.get_stats(command).await
    }

    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error> {
        self.client.read().await.get_me(command).await
    }

    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error> {
        self.client.read().await.get_client(command).await
    }

    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error> {
        self.client.read().await.get_clients(command).await
    }

    async fn ping(&self, command: &Ping) -> Result<(), Error> {
        self.client.read().await.ping(command).await
    }

    async fn kill(&self, command: &Kill) -> Result<(), Error> {
        self.client.read().await.kill(command).await
    }
}

#[async_trait]
impl StreamClient for IggyClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error> {
        self.client.read().await.get_stream(command).await
    }

    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error> {
        self.client.read().await.get_streams(command).await
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.client.read().await.create_stream(command).await
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        self.client.read().await.delete_stream(command).await
    }
}

#[async_trait]
impl TopicClient for IggyClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        self.client.read().await.get_topic(command).await
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        self.client.read().await.get_topics(command).await
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        self.client.read().await.create_topic(command).await
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        self.client.read().await.delete_topic(command).await
    }
}

#[async_trait]
impl PartitionClient for IggyClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error> {
        self.client.read().await.create_partitions(command).await
    }

    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error> {
        self.client.read().await.delete_partitions(command).await
    }
}

#[async_trait]
impl MessageClient for IggyClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error> {
        let messages = self.client.read().await.poll_messages(command).await?;
        if let Some(ref cipher) = self.cipher {
            let mut decrypted_messages = Vec::with_capacity(messages.len());
            for message in messages {
                let nonce = GenericArray::from_slice(&message.payload[0..12]);
                let payload = cipher.decrypt(nonce, &message.payload[12..]);
                if payload.is_err() {
                    error!("Cannot decrypt the message.");
                    return Err(Error::CannotDecryptMessage);
                }

                let payload = payload.unwrap();
                decrypted_messages.push(Message {
                    id: message.id,
                    offset: message.offset,
                    timestamp: message.timestamp,
                    length: payload.len() as u32,
                    payload,
                });
            }
            Ok(decrypted_messages)
        } else {
            Ok(messages)
        }
    }

    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        let send_messages = match &self.partitioner {
            Some(partitioner) => {
                let partition_id = partitioner.calculate_partition_id(
                    &command.stream_id,
                    &command.topic_id,
                    &command.partitioning,
                    &command.messages,
                )?;
                let partitioning = Partitioning::partition_id(partition_id);
                Some(self.map_send_messages(command, partitioning))
            }
            None => {
                if self.cipher.is_some() {
                    let partitioning = Partitioning {
                        kind: command.partitioning.kind,
                        length: command.partitioning.length,
                        value: command.partitioning.value.clone(),
                    };
                    Some(self.map_send_messages(command, partitioning))
                } else {
                    None
                }
            }
        };

        if !self.config.send_messages.enabled || self.config.send_messages.interval == 0 {
            if let Some(send_messages) = send_messages {
                return self.client.read().await.send_messages(&send_messages).await;
            }

            return self.client.read().await.send_messages(command).await;
        }

        let send_messages = match send_messages {
            Some(send_messages) => send_messages,
            None => {
                let partitioning = Partitioning {
                    kind: command.partitioning.kind,
                    length: command.partitioning.length,
                    value: command.partitioning.value.clone(),
                };
                self.map_send_messages(command, partitioning)
            }
        };

        let mut batch = self.send_messages_batch.lock().await;
        batch.commands.push_back(send_messages);
        Ok(())
    }
}

#[async_trait]
impl ConsumerOffsetClient for IggyClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error> {
        self.client
            .read()
            .await
            .store_consumer_offset(command)
            .await
    }

    async fn get_consumer_offset(&self, command: &GetConsumerOffset) -> Result<Offset, Error> {
        self.client.read().await.get_consumer_offset(command).await
    }
}

#[async_trait]
impl ConsumerGroupClient for IggyClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error> {
        self.client.read().await.get_consumer_group(command).await
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error> {
        self.client.read().await.get_consumer_groups(command).await
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error> {
        self.client
            .read()
            .await
            .create_consumer_group(command)
            .await
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error> {
        self.client
            .read()
            .await
            .delete_consumer_group(command)
            .await
    }

    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error> {
        self.client.read().await.join_consumer_group(command).await
    }

    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error> {
        self.client.read().await.leave_consumer_group(command).await
    }
}
