use crate::client::TopicClient;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;
use async_trait::async_trait;

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, IggyError> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(&command.stream_id.as_cow_str()),
                command.topic_id
            ))
            .await?;
        let topic = response.json().await?;
        Ok(topic)
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, IggyError> {
        let response = self.get(&get_path(&command.stream_id.as_cow_str())).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), IggyError> {
        self.post(&get_path(&command.stream_id.as_cow_str()), &command)
            .await?;
        Ok(())
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), IggyError> {
        self.put(
            &get_details_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            command,
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), IggyError> {
        self.delete(&get_details_path(
            &command.stream_id.as_cow_str(),
            &command.topic_id.as_cow_str(),
        ))
        .await?;
        Ok(())
    }

    async fn purge_topic(&self, command: &PurgeTopic) -> Result<(), IggyError> {
        self.delete(&format!(
            "{}/purge",
            &get_details_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            )
        ))
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str) -> String {
    format!("streams/{stream_id}/topics")
}

fn get_details_path(stream_id: &str, topic_id: &str) -> String {
    format!("{}/{topic_id}", get_path(stream_id))
}
