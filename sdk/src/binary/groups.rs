use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{
    CREATE_GROUP_CODE, DELETE_GROUP_CODE, GET_GROUPS_CODE, GET_GROUP_CODE, JOIN_GROUP_CODE,
    LEAVE_GROUP_CODE,
};
use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::groups::get_group::GetGroup;
use crate::groups::get_groups::GetGroups;
use crate::groups::join_group::JoinGroup;
use crate::groups::leave_group::LeaveGroup;
use crate::models::consumer_group::{ConsumerGroup, ConsumerGroupDetails};

pub async fn create_group(client: &dyn BinaryClient, command: &CreateGroup) -> Result<(), Error> {
    client
        .send_with_response(CREATE_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_group(client: &dyn BinaryClient, command: &DeleteGroup) -> Result<(), Error> {
    client
        .send_with_response(DELETE_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn get_group(
    client: &dyn BinaryClient,
    command: &GetGroup,
) -> Result<ConsumerGroupDetails, Error> {
    let response = client
        .send_with_response(GET_GROUP_CODE, &command.as_bytes())
        .await?;
    mapper::map_consumer_group(&response)
}

pub async fn get_groups(
    client: &dyn BinaryClient,
    command: &GetGroups,
) -> Result<Vec<ConsumerGroup>, Error> {
    let response = client
        .send_with_response(GET_GROUPS_CODE, &command.as_bytes())
        .await?;
    mapper::map_consumer_groups(&response)
}

pub async fn join_group(client: &dyn BinaryClient, command: &JoinGroup) -> Result<(), Error> {
    client
        .send_with_response(JOIN_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn leave_group(client: &dyn BinaryClient, command: &LeaveGroup) -> Result<(), Error> {
    client
        .send_with_response(LEAVE_GROUP_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
