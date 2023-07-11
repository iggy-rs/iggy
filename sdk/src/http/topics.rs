use crate::client::TopicClient;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use async_trait::async_trait;

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_topics_path(command.stream_id),
                command.topic_id
            ))
            .await?;
        let topic = response.json().await?;
        Ok(topic)
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        let response = self.get(&get_topics_path(command.stream_id)).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        self.post(&get_topics_path(command.stream_id), &command)
            .await?;
        Ok(())
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        let path = format!(
            "{}/{}",
            get_topics_path(command.stream_id),
            command.topic_id
        );
        self.delete(&path).await?;
        Ok(())
    }

    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, Error> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_groups_path(command.stream_id, command.topic_id),
                command.stream_id
            ))
            .await?;
        let consumer_group = response.json().await?;
        Ok(consumer_group)
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, Error> {
        let response = self
            .get(&get_groups_path(command.stream_id, command.topic_id))
            .await?;
        let consumer_groups = response.json().await?;
        Ok(consumer_groups)
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), Error> {
        self.post(
            &get_groups_path(command.stream_id, command.topic_id),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), Error> {
        let path = format!(
            "{}/{}",
            get_groups_path(command.stream_id, command.topic_id),
            command.consumer_group_id
        );
        self.delete(&path).await?;
        Ok(())
    }

    async fn join_consumer_group(&self, _command: &JoinConsumerGroup) -> Result<(), Error> {
        Err(Error::FeatureUnavailable)
    }

    async fn leave_consumer_group(&self, _command: &LeaveConsumerGroup) -> Result<(), Error> {
        Err(Error::FeatureUnavailable)
    }
}

fn get_topics_path(stream_id: u32) -> String {
    format!("streams/{}/topics", stream_id)
}

fn get_groups_path(stream_id: u32, topic_id: u32) -> String {
    format!("streams/{}/topics/{}/consumer_groups", stream_id, topic_id)
}
