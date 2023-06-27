use crate::client::TopicClient;
use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::http::client::HttpClient;
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

    async fn create_group(&self, command: &CreateGroup) -> Result<(), Error> {
        self.post(
            &get_groups_path(command.stream_id, command.topic_id),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn delete_group(&self, command: &DeleteGroup) -> Result<(), Error> {
        let path = format!(
            "{}/{}",
            get_groups_path(command.stream_id, command.topic_id),
            command.group_id
        );
        self.delete(&path).await?;
        Ok(())
    }
}

fn get_topics_path(stream_id: u32) -> String {
    format!("streams/{}/topics", stream_id)
}

fn get_groups_path(stream_id: u32, topic_id: u32) -> String {
    format!("streams/{}/topics/{}/groups", stream_id, topic_id)
}
