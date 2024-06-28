use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::client::ConsumerGroupClient;
use crate::command::{
    CREATE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE,
    GET_CONSUMER_GROUP_CODE, JOIN_CONSUMER_GROUP_CODE, LEAVE_CONSUMER_GROUP_CODE,
};
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerGroupClient for B {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(
                GET_CONSUMER_GROUP_CODE,
                GetConsumerGroup {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    group_id: group_id.clone(),
                }
                .to_bytes(),
            )
            .await?;
        mapper::map_consumer_group(response)
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(
                GET_CONSUMER_GROUPS_CODE,
                GetConsumerGroups {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                }
                .to_bytes(),
            )
            .await?;
        mapper::map_consumer_groups(response)
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(
                CREATE_CONSUMER_GROUP_CODE,
                CreateConsumerGroup {
                    stream_id: stream_id.clone(),
                    topic_id: topic_id.clone(),
                    name: name.to_string(),
                    group_id,
                }
                .to_bytes(),
            )
            .await?;
        mapper::map_consumer_group(response)
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            DELETE_CONSUMER_GROUP_CODE,
            DeleteConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                group_id: group_id.clone(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            JOIN_CONSUMER_GROUP_CODE,
            JoinConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                group_id: group_id.clone(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(
            LEAVE_CONSUMER_GROUP_CODE,
            LeaveConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                group_id: group_id.clone(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }
}
