use crate::binary::binary_client::BinaryClient;
use crate::binary::{fail_if_not_authenticated, mapper};
use crate::bytes_serializable::BytesSerializable;
use crate::command::{
    CREATE_TOPIC_CODE, DELETE_TOPIC_CODE, GET_TOPICS_CODE, GET_TOPIC_CODE, PURGE_TOPIC_CODE,
    UPDATE_TOPIC_CODE,
};
use crate::error::Error;
use crate::models::topic::{Topic, TopicDetails};
use crate::topics::create_topic::CreateTopic;
use crate::topics::delete_topic::DeleteTopic;
use crate::topics::get_topic::GetTopic;
use crate::topics::get_topics::GetTopics;
use crate::topics::purge_topic::PurgeTopic;
use crate::topics::update_topic::UpdateTopic;

pub async fn get_topic(
    client: &dyn BinaryClient,
    command: &GetTopic,
) -> Result<TopicDetails, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_TOPIC_CODE, &command.as_bytes())
        .await?;
    mapper::map_topic(&response)
}

pub async fn get_topics(
    client: &dyn BinaryClient,
    command: &GetTopics,
) -> Result<Vec<Topic>, Error> {
    fail_if_not_authenticated(client).await?;
    let response = client
        .send_with_response(GET_TOPICS_CODE, &command.as_bytes())
        .await?;
    mapper::map_topics(&response)
}

pub async fn create_topic(client: &dyn BinaryClient, command: &CreateTopic) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(CREATE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_topic(client: &dyn BinaryClient, command: &DeleteTopic) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(DELETE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_topic(client: &dyn BinaryClient, command: &UpdateTopic) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(UPDATE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn purge_topic(client: &dyn BinaryClient, command: &PurgeTopic) -> Result<(), Error> {
    fail_if_not_authenticated(client).await?;
    client
        .send_with_response(PURGE_TOPIC_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
