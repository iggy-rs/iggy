use crate::binary::binary_client::{BinaryClient, BinaryClientV2};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::ConsumerGroupClient;
use crate::client_v2::ConsumerGroupClientV2;
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
        command: &GetConsumerGroup,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        get_consumer_group(self, command).await
    }

    async fn get_consumer_groups(
        &self,
        command: &GetConsumerGroups,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        get_consumer_groups(self, command).await
    }

    async fn create_consumer_group(&self, command: &CreateConsumerGroup) -> Result<(), IggyError> {
        create_consumer_group(self, command).await
    }

    async fn delete_consumer_group(&self, command: &DeleteConsumerGroup) -> Result<(), IggyError> {
        delete_consumer_group(self, command).await
    }

    async fn join_consumer_group(&self, command: &JoinConsumerGroup) -> Result<(), IggyError> {
        join_consumer_group(self, command).await
    }

    async fn leave_consumer_group(&self, command: &LeaveConsumerGroup) -> Result<(), IggyError> {
        leave_consumer_group(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientV2> ConsumerGroupClientV2 for B {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<ConsumerGroupDetails, IggyError> {
        get_consumer_group(
            self,
            &GetConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                consumer_group_id: group_id.clone(),
            },
        )
        .await
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, IggyError> {
        get_consumer_groups(
            self,
            &GetConsumerGroups {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            },
        )
        .await
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<(), IggyError> {
        create_consumer_group(
            self,
            &CreateConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: name.to_string(),
                group_id,
            },
        )
        .await
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        delete_consumer_group(
            self,
            &DeleteConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                consumer_group_id: group_id.clone(),
            },
        )
        .await
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        join_consumer_group(
            self,
            &JoinConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                consumer_group_id: group_id.clone(),
            },
        )
        .await
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        leave_consumer_group(
            self,
            &LeaveConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                consumer_group_id: group_id.clone(),
            },
        )
        .await
    }
}

async fn get_consumer_group<T: BinaryTransport>(
    transport: &T,
    command: &GetConsumerGroup,
) -> Result<ConsumerGroupDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_CONSUMER_GROUP_CODE, command.as_bytes())
        .await?;
    mapper::map_consumer_group(response)
}

async fn get_consumer_groups<T: BinaryTransport>(
    transport: &T,
    command: &GetConsumerGroups,
) -> Result<Vec<ConsumerGroup>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_CONSUMER_GROUPS_CODE, command.as_bytes())
        .await?;
    mapper::map_consumer_groups(response)
}

async fn create_consumer_group<T: BinaryTransport>(
    transport: &T,
    command: &CreateConsumerGroup,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CREATE_CONSUMER_GROUP_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn delete_consumer_group<T: BinaryTransport>(
    transport: &T,
    command: &DeleteConsumerGroup,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_CONSUMER_GROUP_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn join_consumer_group<T: BinaryTransport>(
    transport: &T,
    command: &JoinConsumerGroup,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(JOIN_CONSUMER_GROUP_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn leave_consumer_group<T: BinaryTransport>(
    transport: &T,
    command: &LeaveConsumerGroup,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(LEAVE_CONSUMER_GROUP_CODE, command.as_bytes())
        .await?;
    Ok(())
}
