use crate::binary;
use crate::client::TopicClient;
use crate::error::Error;
use crate::tcp::client::TcpClient;
use crate::topic::{Topic, TopicDetails};
use async_trait::async_trait;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topic::GetTopic;
use shared::topics::get_topics::GetTopics;

#[async_trait]
impl TopicClient for TcpClient {
    async fn get_topic(&self, command: GetTopic) -> Result<TopicDetails, Error> {
        binary::topics::get_topic(self, command).await
    }

    async fn get_topics(&self, command: GetTopics) -> Result<Vec<Topic>, Error> {
        binary::topics::get_topics(self, command).await
    }

    async fn create_topic(&self, command: CreateTopic) -> Result<(), Error> {
        binary::topics::create_topic(self, command).await
    }

    async fn delete_topic(&self, command: DeleteTopic) -> Result<(), Error> {
        binary::topics::delete_topic(self, command).await
    }
}
