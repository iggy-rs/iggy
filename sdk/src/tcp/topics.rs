use crate::binary;
use crate::client::TopicClient;
use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::groups::get_group::GetGroup;
use crate::groups::get_groups::GetGroups;
use crate::groups::join_group::JoinGroup;
use crate::groups::leave_group::LeaveGroup;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::tcp::client::TcpClient;
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use async_trait::async_trait;

#[async_trait]
impl TopicClient for TcpClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        binary::topics::get_topic(self, command).await
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        binary::topics::get_topics(self, command).await
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        binary::topics::create_topic(self, command).await
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        binary::topics::delete_topic(self, command).await
    }

    async fn get_group(&self, command: &GetGroup) -> Result<ConsumerGroupDetails, Error> {
        binary::groups::get_group(self, command).await
    }

    async fn get_groups(&self, command: &GetGroups) -> Result<Vec<ConsumerGroup>, Error> {
        binary::groups::get_groups(self, command).await
    }

    async fn create_group(&self, command: &CreateGroup) -> Result<(), Error> {
        binary::groups::create_group(self, command).await
    }

    async fn delete_group(&self, command: &DeleteGroup) -> Result<(), Error> {
        binary::groups::delete_group(self, command).await
    }

    async fn join_group(&self, command: &JoinGroup) -> Result<(), Error> {
        binary::groups::join_group(self, command).await
    }

    async fn leave_group(&self, command: &LeaveGroup) -> Result<(), Error> {
        binary::groups::leave_group(self, command).await
    }
}
