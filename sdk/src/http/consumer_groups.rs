use crate::client::ConsumerGroupClient;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use async_trait::async_trait;

#[async_trait]
impl ConsumerGroupClient for HttpClient {
    async fn get_consumer_group(
        &self,
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(
                    &command.stream_id.as_cow_str(),
                    &command.topic_id.as_cow_str()
                ),
                command.consumer_group_id
            ))
            .await?;
        let consumer_group = response.json().await?;
        Ok(consumer_group)
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        let response = self
            .get(&get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ))
            .await?;
        let consumer_groups = response.json().await?;
        Ok(consumer_groups)
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), IggyError> {
        self.post(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
        Ok(())
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), IggyError> {
        let path = format!(
            "{}/{}",
            get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str()
            ),
            command.consumer_group_id
        );
        self.delete(&path).await?;
        Ok(())
    }

    async fn join_consumer_group(&self, _command: &JoinConsumerGroup) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    async fn leave_consumer_group(&self, _command: &LeaveConsumerGroup) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-groups")
}
