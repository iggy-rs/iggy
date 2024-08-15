use crate::client::ConsumerGroupClient;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::error::IggyError;
use crate::http::client::HttpClient;
use crate::http::HttpTransport;
use crate::identifier::Identifier;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};
use async_trait::async_trait;

#[async_trait]
impl ConsumerGroupClient for HttpClient {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, IggyError> {
        let response = self
            .get(&format!(
                "{}/{}",
                get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                group_id
            ))
            .await?;
        if response.status() == 404 {
            return Ok(None);
        }

        let consumer_group = response.json().await?;
        Ok(Some(consumer_group))
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        let response = self
            .get(&get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()))
            .await?;
        let consumer_groups = response.json().await?;
        Ok(consumer_groups)
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        let response = self
            .post(
                &get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
                &CreateConsumerGroup {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    name: name.to_string(),
                    group_id,
                },
            )
            .await?;
        let consumer_group = response.json().await?;
        Ok(consumer_group)
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        let path = format!(
            "{}/{}",
            get_path(&stream_id.as_cow_str(), &topic_id.as_cow_str()),
            &group_id.as_cow_str()
        );
        self.delete(&path).await?;
        Ok(())
    }

    async fn join_consumer_group(
        &self,
        _: &Identifier,
        _: &Identifier,
        _: &Identifier,
    ) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }

    async fn leave_consumer_group(
        &self,
        _: &Identifier,
        _: &Identifier,
        _: &Identifier,
    ) -> Result<(), IggyError> {
        Err(IggyError::FeatureUnavailable)
    }
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-groups")
}
