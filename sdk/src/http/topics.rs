use crate::error::Error;
use crate::http::client::Client;
use crate::topic::Topic;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;

impl Client {
    pub async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        let response = self.get(&Self::get_topics_path(command.stream_id)).await?;
        let topics = response.json().await?;
        Ok(topics)
    }

    pub async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        self.post(&Self::get_topics_path(command.stream_id), command)
            .await?;
        Ok(())
    }

    pub async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        let path = format!(
            "{}/{}",
            Self::get_topics_path(command.stream_id),
            command.topic_id
        );
        self.delete(&path).await?;
        Ok(())
    }

    fn get_topics_path(stream_id: u32) -> String {
        format!("streams/{}/topics", stream_id)
    }
}
