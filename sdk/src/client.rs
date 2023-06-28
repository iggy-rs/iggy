use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::groups::get_group::GetGroup;
use crate::groups::get_groups::GetGroups;
use crate::messages::poll_messages::PollMessages;
use crate::messages::send_messages::SendMessages;
use crate::models::client_info::ClientInfo;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::message::Message;
use crate::models::offset::Offset;
use crate::models::stream::{Stream, StreamDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::offsets::get_offset::GetOffset;
use crate::offsets::store_offset::StoreOffset;
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::system::get_clients::GetClients;
use crate::system::kill::Kill;
use crate::system::ping::Ping;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use async_trait::async_trait;

#[async_trait]
pub trait Client: SystemClient + StreamClient + TopicClient + MessageClient + Sync + Send {
    async fn connect(&mut self) -> Result<(), Error>;
    async fn disconnect(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait SystemClient {
    async fn get_clients(&self, command: &GetClients) -> Result<Vec<ClientInfo>, Error>;
    async fn ping(&self, command: &Ping) -> Result<(), Error>;
    async fn kill(&self, command: &Kill) -> Result<(), Error>;
}

#[async_trait]
pub trait StreamClient {
    async fn get_stream(&self, command: &GetStream) -> Result<StreamDetails, Error>;
    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error>;
    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error>;
    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error>;
}

#[async_trait]
pub trait TopicClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error>;
    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error>;
    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error>;
    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error>;
    async fn get_group(&self, command: &GetGroup) -> Result<ConsumerGroupDetails, Error>;
    async fn get_groups(&self, command: &GetGroups) -> Result<Vec<ConsumerGroup>, Error>;
    async fn create_group(&self, command: &CreateGroup) -> Result<(), Error>;
    async fn delete_group(&self, command: &DeleteGroup) -> Result<(), Error>;
}

#[async_trait]
pub trait MessageClient {
    async fn poll_messages(&self, command: &PollMessages) -> Result<Vec<Message>, Error>;
    async fn send_messages(&self, command: &SendMessages) -> Result<(), Error>;
    async fn store_offset(&self, command: &StoreOffset) -> Result<(), Error>;
    async fn get_offset(&self, command: &GetOffset) -> Result<Offset, Error>;
}
