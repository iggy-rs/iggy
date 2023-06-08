use crate::error::Error;
use crate::message::Message;
use crate::stream::Stream;
use crate::topic::Topic;
use async_trait::async_trait;
use shared::messages::poll_messages::PollMessages;
use shared::messages::send_messages::SendMessages;
use shared::offsets::store_offset::StoreOffset;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_streams::GetStreams;
use shared::system::kill::Kill;
use shared::system::ping::Ping;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;

#[async_trait]
pub trait Client: SystemClient + StreamClient + TopicClient + MessageClient + Sync + Send {
    async fn connect(&mut self) -> Result<(), Error>;
    async fn disconnect(&mut self) -> Result<(), Error>;
}

#[async_trait]
pub trait SystemClient {
    async fn ping(&self, command: Ping) -> Result<(), Error>;
    async fn kill(&self, command: Kill) -> Result<(), Error>;
}

#[async_trait]
pub trait StreamClient {
    async fn get_streams(&self, command: GetStreams) -> Result<Vec<Stream>, Error>;
    async fn create_stream(&self, command: CreateStream) -> Result<(), Error>;
    async fn delete_stream(&self, command: DeleteStream) -> Result<(), Error>;
}

#[async_trait]
pub trait TopicClient {
    async fn get_topics(&self, command: GetTopics) -> Result<Vec<Topic>, Error>;
    async fn create_topic(&self, command: CreateTopic) -> Result<(), Error>;
    async fn delete_topic(&self, command: DeleteTopic) -> Result<(), Error>;
}

#[async_trait]
pub trait MessageClient {
    async fn poll_messages(&self, command: PollMessages) -> Result<Vec<Message>, Error>;
    async fn send_messages(&self, command: SendMessages) -> Result<(), Error>;
    async fn store_offset(&self, command: StoreOffset) -> Result<(), Error>;
}
