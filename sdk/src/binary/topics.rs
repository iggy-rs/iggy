use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{
    CREATE_GROUP_CODE, CREATE_TOPIC_CODE, DELETE_GROUP_CODE, DELETE_TOPIC_CODE, GET_TOPICS_CODE,
    GET_TOPIC_CODE,
};
use crate::error::Error;
use crate::groups::create_group::CreateGroup;
use crate::groups::delete_group::DeleteGroup;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;

pub async fn get_topic(
    client: &dyn BinaryClient,
    command: &GetTopic,
) -> Result<TopicDetails, Error> {
    let response = client
        .send_with_response(GET_TOPIC_CODE, &command.as_bytes())
        .await?;
    mapper::map_topic(&response)
}

pub async fn get_topics(
    client: &dyn BinaryClient,
    command: &GetTopics,
) -> Result<Vec<Topic>, Error> {
    let response = client
        .send_with_response(GET_TOPICS_CODE, &command.as_bytes())
        .await?;
    mapper::map_topics(&response)
}

pub async fn create_topic(client: &dyn BinaryClient, command: &CreateTopic) -> Result<(), Error> {
    client
        .send_with_response(CREATE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_topic(client: &dyn BinaryClient, command: &DeleteTopic) -> Result<(), Error> {
    client
        .send_with_response(DELETE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

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
