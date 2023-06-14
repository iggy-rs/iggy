use crate::client::TopicClient;
use crate::error::Error;
use crate::http::client::HttpClient;
use crate::topic::{Topic, TopicDetails};
use async_trait::async_trait;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topic::GetTopic;
use shared::topics::get_topics::GetTopics;

#[async_trait]
impl TopicClient for HttpClient {
    async fn get_topic(&self, command: GetTopic) -> Result<TopicDetails, Error> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(command.stream_id),
                command.topic_id
            ))
            .await?;
        let topic = response.json().await?;
        Ok(topic)
    }

    async fn get_topics(&self, command: GetTopics) -> Result<Vec<Topic>, Error> {
        let response = self.get(&get_path(command.stream_id)).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    async fn create_topic(&self, command: CreateTopic) -> Result<(), Error> {
        self.post(&get_path(command.stream_id), &command).await?;
        Ok(())
    }

    async fn delete_topic(&self, command: DeleteTopic) -> Result<(), Error> {
        let path = format!("{}/{}", get_path(command.stream_id), command.topic_id);
        self.delete(&path).await?;
        Ok(())
    }
}

fn get_path(stream_id: u32) -> String {
    format!("streams/{}/topics", stream_id)
}
