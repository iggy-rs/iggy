use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::TopicClient;
use crate::command::{
    CREATE_TOPIC_CODE, DELETE_TOPIC_CODE, GET_TOPICS_CODE, GET_TOPIC_CODE, PURGE_TOPIC_CODE,
    UPDATE_TOPIC_CODE,
};
use crate::error::Error;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;

#[async_trait::async_trait]
impl<B: BinaryClient> TopicClient for B {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, Error> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_TOPIC_CODE, &command.as_bytes())
            .await?;
        mapper::map_topic(&response)
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, Error> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(GET_TOPICS_CODE, &command.as_bytes())
            .await?;
        mapper::map_topics(&response)
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), Error> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(CREATE_TOPIC_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), Error> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(DELETE_TOPIC_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), Error> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(UPDATE_TOPIC_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }

    async fn purge_topic(&self, command: &PurgeTopic) -> Result<(), Error> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(PURGE_TOPIC_CODE, &command.as_bytes())
            .await?;
        Ok(())
    }
}
