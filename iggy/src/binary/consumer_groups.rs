use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
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
use crate::error::Error;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};

pub async fn create_group(
    client: &dyn BinaryClient,
    command: &CreateConsumerGroup,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(CREATE_CONSUMER_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_group(
    client: &dyn BinaryClient,
    command: &DeleteConsumerGroup,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(DELETE_CONSUMER_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn get_group(
    client: &dyn BinaryClient,
    command: &GetConsumerGroup,
) -> Result<ConsumerGroupDetails, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_CONSUMER_GROUP_CODE, &command.as_bytes())
        .await?;
    mapper::map_consumer_group(&response)
}

pub async fn get_groups(
    client: &dyn BinaryClient,
    command: &GetConsumerGroups,
) -> Result<Vec<ConsumerGroup>, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_CONSUMER_GROUPS_CODE, &command.as_bytes())
        .await?;
    mapper::map_consumer_groups(&response)
}

pub async fn join_group(
    client: &dyn BinaryClient,
    command: &JoinConsumerGroup,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(JOIN_CONSUMER_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn leave_group(
    client: &dyn BinaryClient,
    command: &LeaveConsumerGroup,
) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(LEAVE_CONSUMER_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
