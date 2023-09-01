use crate::client::TopicClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::update_topic::UpdateTopic;
use async_trait::async_trait;

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(&command.stream_id.as_string()),
                command.topic_id
            ))
            .await?;
        let topic = response.json().await?;
        Ok(topic)
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        let response = self.get(&get_path(&command.stream_id.as_string())).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        self.post(&get_path(&command.stream_id.as_string()), &command)
            .await?;
        Ok(())
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error> {
        self.put(
            &get_details_path(
                &command.stream_id.as_string(),
                &command.topic_id.as_string(),
            ),
            command,
        )
        .await?;
        Ok(())
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        self.delete(&get_details_path(
            &command.stream_id.as_string(),
            &command.topic_id.as_string(),
        ))
        .await?;
        Ok(())
    }
}

fn get_path(stream_id: &str) -> String {
    format!("streams/{}/topics", stream_id)
}

fn get_details_path(stream_id: &str, topic_id: &str) -> String {
    format!("{}/{}", get_path(stream_id), topic_id)
}
