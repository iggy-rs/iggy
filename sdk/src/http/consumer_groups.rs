use crate::client::ConsumerGroupClient;
use crate::client_v2::ConsumerGroupClientV2;
use crate::consumer_groups::create_consumer_group::CreateConsumerGroup;
use crate::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use crate::consumer_groups::get_consumer_group::GetConsumerGroup;
use crate::consumer_groups::get_consumer_groups::GetConsumerGroups;
use crate::consumer_groups::join_consumer_group::JoinConsumerGroup;
use crate::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
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

#[async_trait]
impl ConsumerGroupClientV2 for HttpClient {
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
        group_id: u32,
        name: &str,
    ) -> Result<(), IggyError> {
        create_consumer_group(
            self,
            &CreateConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                consumer_group_id: group_id,
                name: name.to_string(),
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

async fn get_consumer_group<T: HttpTransport>(
    transport: &T,
    command: &GetConsumerGroup,
) -> Result<ConsumerGroupDetails, IggyError> {
    let response = transport
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

async fn get_consumer_groups<T: HttpTransport>(
    transport: &T,
    command: &GetConsumerGroups,
) -> Result<Vec<ConsumerGroup>, IggyError> {
    let response = transport
        .get(&get_path(
            &command.stream_id.as_cow_str(),
            &command.topic_id.as_cow_str(),
        ))
        .await?;
    let consumer_groups = response.json().await?;
    Ok(consumer_groups)
}

async fn create_consumer_group<T: HttpTransport>(
    transport: &T,
    command: &CreateConsumerGroup,
) -> Result<(), IggyError> {
    transport
        .post(
            &get_path(
                &command.stream_id.as_cow_str(),
                &command.topic_id.as_cow_str(),
            ),
            &command,
        )
        .await?;
    Ok(())
}

async fn delete_consumer_group<T: HttpTransport>(
    transport: &T,
    command: &DeleteConsumerGroup,
) -> Result<(), IggyError> {
    let path = format!(
        "{}/{}",
        get_path(
            &command.stream_id.as_cow_str(),
            &command.topic_id.as_cow_str()
        ),
        command.consumer_group_id
    );
    transport.delete(&path).await?;
    Ok(())
}

async fn join_consumer_group<T: HttpTransport>(
    _: &T,
    _command: &JoinConsumerGroup,
) -> Result<(), IggyError> {
    Err(IggyError::FeatureUnavailable)
}

async fn leave_consumer_group<T: HttpTransport>(
    _: &T,
    _command: &LeaveConsumerGroup,
) -> Result<(), IggyError> {
    Err(IggyError::FeatureUnavailable)
}

fn get_path(stream_id: &str, topic_id: &str) -> String {
    format!("streams/{stream_id}/topics/{topic_id}/consumer-groups")
}
