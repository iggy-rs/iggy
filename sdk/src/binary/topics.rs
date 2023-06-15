use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::error::Error;
use crate::topic::{Topic, TopicDetails};
use shared::command::Command;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topic::GetTopic;
use shared::topics::get_topics::GetTopics;

pub async fn get_topic(
    client: &dyn BinaryClient,
    command: GetTopic,
) -> Result<TopicDetails, Error> {
    let response = client
        .send_with_response(Command::GetTopic(command))
        .await?;

    let topic = mapper::map_topic(&response)?;
    Ok(topic)
}

pub async fn get_topics(
    client: &dyn BinaryClient,
    command: GetTopics,
) -> Result<Vec<Topic>, Error> {
    let response = client
        .send_with_response(Command::GetTopics(command))
        .await?;

    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut topics = mapper::map_topics(&response)?;
    topics.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(topics)
}

pub async fn create_topic(client: &dyn BinaryClient, command: CreateTopic) -> Result<(), Error> {
    client
        .send_with_response(Command::CreateTopic(command))
        .await?;
    Ok(())
}

pub async fn delete_topic(client: &dyn BinaryClient, command: DeleteTopic) -> Result<(), Error> {
    client
        .send_with_response(Command::DeleteTopic(command))
        .await?;
    Ok(())
}
