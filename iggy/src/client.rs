use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use crate::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use crate::error::Error;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::client_info::{ClientInfo, ClientInfoDetails};
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::consumer_offset_info::ConsumerOffsetInfo;
use crate::models::messages::PolledMessages;
use crate::models::stats::Stats;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::partitions::create_partitions::CreatePartitions;
use crate::partitions::delete_partitions::DeletePartitions;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::update_stream::UpdateStream;
use crate::system::get_client::GetClient;
use crate::system::get_clients::GetClients;
use crate::system::get_me::GetMe;
use crate::system::get_stats::GetStats;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::update_topic::UpdateTopic;
use crate::users::login_user::LoginUser;
use async_trait::async_trait;
use std::fmt::Debug;

#[async_trait]
pub trait Client:
    SystemClient
    + UserClient
    + StreamClient
    + TopicClient
    + PartitionClient
    + MessageClient
    + ConsumerOffsetClient
    + ConsumerGroupClient
    + Sync
    + Send
    + Debug
{
    async fn connect(&mut self) -> Result<(), Error>;
    async fn disconnect(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait SystemClient {
    async fn get_stats(&self, command: &GetStats) -> Result<Stats, Error>;
    async fn get_me(&self, command: &GetMe) -> Result<ClientInfoDetails, Error>;
    async fn get_client(&self, command: &GetClient) -> Result<ClientInfoDetails, Error>;
    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error>;
    async fn ping(&self, command: &Ping) -> Result<(), Error>;
}

#[async_trait]
pub trait UserClient {
    async fn login_user(&self, command: &LoginUser) -> Result<(), Error>;
}

#[async_trait]
pub trait StreamClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error>;
    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error>;
    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error>;
    async fn update_stream(&self, command: &UpdateStream) -> Result<(), Error>;
    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error>;
}

#[async_trait]
pub trait TopicClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error>;
    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error>;
    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error>;
    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error>;
    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error>;
}

#[async_trait]
pub trait PartitionClient {
    async fn create_partitions(&self, command: &CreatePartitions) -> Result<(), Error>;
    async fn delete_partitions(&self, command: &DeletePartitions) -> Result<(), Error>;
}

#[async_trait]
pub trait MessageClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<PolledMessages, Error>;
    async fn send_messages(&self, command: &mut SendMessages) -> Result<(), Error>;
}

#[async_trait]
pub trait ConsumerOffsetClient {
    async fn store_consumer_offset(&self, command: &StoreConsumerOffset) -> Result<(), Error>;
    async fn get_consumer_offset(
        &self,
        command: &GetConsumerOffset,
    ) -> Result<ConsumerOffsetInfo, Error>;
}

#[async_trait]
pub trait ConsumerGroupClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error>;
    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error>;
    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error>;
    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error>;
    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), Error>;
    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), Error>;
}
