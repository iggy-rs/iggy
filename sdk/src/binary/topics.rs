use crate::binary::binary_client::{BinaryClient, BinaryClientV2};
use crate::binary::{fail_if_not_authenticated, mapper, BinaryTransport};
use crate::bytes_serializable::BytesSerializable;
use crate::client::TopicClient;
use crate::client_v2::TopicClientV2;
use crate::command::{
    CREATE_TOPIC_CODE, DELETE_TOPIC_CODE, GET_TOPICS_CODE, GET_TOPIC_CODE, PURGE_TOPIC_CODE,
    UPDATE_TOPIC_CODE,
};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::expiry::IggyExpiry;

#[async_trait::async_trait]
impl<B: BinaryClient> TopicClient for B {
    async fn get_topic(&self, command: &GetTopic) -> Result<TopicDetails, IggyError> {
        get_topic(self, command).await
    }

    async fn get_topics(&self, command: &GetTopics) -> Result<Vec<Topic>, IggyError> {
        get_topics(self, command).await
    }

    async fn create_topic(&self, command: &CreateTopic) -> Result<(), IggyError> {
        create_topic(self, command).await
    }

    async fn update_topic(&self, command: &UpdateTopic) -> Result<(), IggyError> {
        update_topic(self, command).await
    }

    async fn delete_topic(&self, command: &DeleteTopic) -> Result<(), IggyError> {
        delete_topic(self, command).await
    }

    async fn purge_topic(&self, command: &PurgeTopic) -> Result<(), IggyError> {
        purge_topic(self, command).await
    }
}

#[async_trait::async_trait]
impl<B: BinaryClientV2> TopicClientV2 for B {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicDetails, IggyError> {
        get_topic(
            self,
            &GetTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            },
        )
        .await
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, IggyError> {
        get_topics(
            self,
            &GetTopics {
                stream_id: stream_id.clone(),
            },
        )
        .await
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError> {
        create_topic(
            self,
            &CreateTopic {
                stream_id: stream_id.clone(),
                name: name.to_string(),
                partitions_count,
                replication_factor: replication_factor.unwrap_or(1),
                topic_id,
                message_expiry: message_expiry.into(),
                max_topic_size,
            },
        )
        .await
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        replication_factor: Option<u8>,
        message_expiry: IggyExpiry,
        max_topic_size: Option<IggyByteSize>,
    ) -> Result<(), IggyError> {
        update_topic(
            self,
            &UpdateTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: name.to_string(),
                replication_factor: replication_factor.unwrap_or(1),
                message_expiry: message_expiry.into(),
                max_topic_size,
            },
        )
        .await
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        delete_topic(
            self,
            &DeleteTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            },
        )
        .await
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        purge_topic(
            self,
            &PurgeTopic {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            },
        )
        .await
    }
}

async fn get_topic<T: BinaryTransport>(
    transport: &T,
    command: &GetTopic,
) -> Result<TopicDetails, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_TOPIC_CODE, command.as_bytes())
        .await?;
    mapper::map_topic(response)
}

async fn get_topics<T: BinaryTransport>(
    transport: &T,
    command: &GetTopics,
) -> Result<Vec<Topic>, IggyError> {
    fail_if_not_authenticated(transport).await?;
    let response = transport
        .send_with_response(GET_TOPICS_CODE, command.as_bytes())
        .await?;
    mapper::map_topics(response)
}

async fn create_topic<T: BinaryTransport>(
    transport: &T,
    command: &CreateTopic,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(CREATE_TOPIC_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn update_topic<T: BinaryTransport>(
    transport: &T,
    command: &UpdateTopic,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(UPDATE_TOPIC_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn delete_topic<T: BinaryTransport>(
    transport: &T,
    command: &DeleteTopic,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(DELETE_TOPIC_CODE, command.as_bytes())
        .await?;
    Ok(())
}

async fn purge_topic<T: BinaryTransport>(
    transport: &T,
    command: &PurgeTopic,
) -> Result<(), IggyError> {
    fail_if_not_authenticated(transport).await?;
    transport
        .send_with_response(PURGE_TOPIC_CODE, command.as_bytes())
        .await?;
    Ok(())
}
