use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::client::TopicClient;
use crate::compression::compression_algorithm::CompressionAlgorithm;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;
use crate::utils::expiry::IggyExpiry;
use crate::utils::topic_size::MaxTopicSize;

#[async_trait::async_trait]
impl<B: BinaryClient> TopicClient for B {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_topic(response).map(Some)
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetTopics {
                stream_id: stream_id.clone(),
            })
            .await?;
        mapper::map_topics(response)
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateTopic {
                stream_id: stream_id.clone(),
                name: name.to_string(),
                partitions_count,
                compression_algorithm,
                replication_factor,
                topic_id,
                message_expiry,
                max_topic_size,
            })
            .await?;
        mapper::map_topic(response)
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            name: name.to_string(),
            compression_algorithm,
            replication_factor,
            message_expiry,
            max_topic_size,
        })
        .await?;
        Ok(())
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&PurgeTopic {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
        })
        .await?;
        Ok(())
    }
}
