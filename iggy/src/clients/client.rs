use crate::client::{
    Client, ConsumerGroupClient, MessageClient, PartitionClient, StreamClient, SystemClient,
    TopicClient,
};
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::error::Error;
use crate::identifier::{IdKind, Identifier};
use crate::messages::poll_messages::{Kind, PollMessages};
use crate::messages::send_messages::{Key, KeyKind, SendMessages};
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
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
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::error;

#[derive(Debug)]
pub struct IggyClient {
    client: Arc<RwLock<Box<dyn Client>>>,
    config: IggyClientConfig,
    send_messages_batch: Arc<Mutex<SendMessagesBatch>>,
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
    pub fn new(client: Box<dyn Client>, config: IggyClientConfig) -> Self {
        let client = Arc::new(RwLock::new(client));
        let send_messages_batch = Arc::new(Mutex::new(SendMessagesBatch {
            commands: VecDeque::new(),
        }));
        if config.send_messages.enabled && config.send_messages.interval > 0 {
            Self::send_messages_in_background(
                config.send_messages.interval,
                config.send_messages.max_messages,
                client.clone(),
                send_messages_batch.clone(),
            );
        }

        IggyClient {
            client,
            config,
            send_messages_batch,
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

                if poll_messages.kind == Kind::Offset {
                    poll_messages.value = current_offset + 1;
                }
            }
        })
    }

    async fn store_offset(client: &dyn Client, poll_messages: &PollMessages, offset: u64) {
        // TODO: Support string stream IDs.
        let stream_id = match poll_messages.stream_id.kind {
            IdKind::Numeric => poll_messages.stream_id.as_u32().unwrap(),
            IdKind::String => {
                error!("Storing offset for string stream ID is not supported yet.");
                return;
            }
        };

        let topic_id = match poll_messages.topic_id.kind {
            IdKind::Numeric => poll_messages.topic_id.as_u32().unwrap(),
            IdKind::String => {
                error!("Storing offset for string stream ID is not supported yet.");
                return;
            }
        };

        let result = client
            .store_offset(&StoreOffset {
                consumer_type: poll_messages.consumer_type,
                consumer_id: poll_messages.consumer_id,
                stream_id,
                topic_id,
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
                let mut key = Key::partition_id(0);
                let mut batch_messages = true;

                for send_messages in send_messages_batch.commands.iter() {
                    if !initialized {
                        if send_messages.key.kind != KeyKind::PartitionId {
                            batch_messages = false;
                            break;
                        }

                        stream_id = Identifier::from(&send_messages.stream_id);
                        topic_id = Identifier::from(&send_messages.topic_id);
                        key.value = send_messages.key.value.clone();
                        initialized = true;
                    }

                    // Batching the messages is only possible for the same stream, topic and partition.
                    if send_messages.stream_id != stream_id
                        || send_messages.topic_id != topic_id
                        || send_messages.key.kind != KeyKind::PartitionId
                        || send_messages.key.value != key.value
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
                        stream_id: Identifier::from(&stream_id),
                        topic_id: Identifier::from(&topic_id),
                        messages_count: messages.len() as u32,
                        key: Key {
                            kind: KeyKind::PartitionId,
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
        self.client.read().await.poll_messages(command).await
    }

    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error> {
        if !self.config.send_messages.enabled || self.config.send_messages.interval == 0 {
            return self.client.read().await.send_messages(command).await;
        }

        let mut batch = self.send_messages_batch.lock().await;
        let send_messages = SendMessages {
            stream_id: Identifier::from(&command.stream_id),
            topic_id: Identifier::from(&command.topic_id),
            messages_count: command.messages_count,
            key: Key {
                kind: command.key.kind,
                length: command.key.length,
                value: command.key.value.clone(),
            },
            messages: command
                .messages
                .iter()
                .map(|message| crate::messages::send_messages::Message {
                    id: message.id,
                    length: message.length,
                    payload: message.payload.clone(),
                })
                .collect(),
        };
        batch.commands.push_back(send_messages);
        Ok(())
    }

    async fn store_offset(&self, command: &StoreOffset) -> Result<(), Error> {
        self.client.read().await.store_offset(command).await
    }

    async fn get_offset(&self, command: &GetOffset) -> Result<Offset, Error> {
        self.client.read().await.get_offset(command).await
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
