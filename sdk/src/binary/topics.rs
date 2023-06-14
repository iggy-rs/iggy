use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use crate::partition::Partition;
use crate::topic::{Topic, TopicDetails};
use shared::command::Command;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topic::GetTopic;
use shared::topics::get_topics::GetTopics;
use std::str::from_utf8;

pub async fn get_topic(
    client: &dyn BinaryClient,
    command: GetTopic,
) -> Result<TopicDetails, Error> {
    let response = client
        .send_with_response(Command::GetTopic(command))
        .await?;
    handle_get_topic_response(&response)
}

pub async fn get_topics(
    client: &dyn BinaryClient,
    command: GetTopics,
) -> Result<Vec<Topic>, Error> {
    let response = client
        .send_with_response(Command::GetTopics(command))
        .await?;
    handle_get_topics_response(&response)
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

fn handle_get_topic_response(response: &[u8]) -> Result<TopicDetails, Error> {
    let id = u32::from_le_bytes(response[..4].try_into()?);
    let partitions_count = u32::from_le_bytes(response[4..8].try_into()?);
    let name_length = u32::from_le_bytes(response[8..12].try_into()?) as usize;
    let name = from_utf8(&response[12..12 + name_length])?.to_string();
    let mut partitions = Vec::new();
    let length = response.len();
    let mut position = 12 + name_length;
    while position < length {
        let id = u32::from_le_bytes(response[position..position + 4].try_into()?);
        let segments_count = u32::from_le_bytes(response[position + 4..position + 8].try_into()?);
        let current_offset = u64::from_le_bytes(response[position + 8..position + 16].try_into()?);
        let size_bytes = u64::from_le_bytes(response[position + 16..position + 24].try_into()?);
        partitions.push(Partition {
            id,
            segments_count,
            current_offset,
            size_bytes,
        });
        position += 4 + 4 + 8 + 8;

        if position >= length {
            break;
        }
    }

    partitions.sort_by(|x, y| x.id.cmp(&y.id));
    let topic = TopicDetails {
        id,
        partitions_count,
        name,
        partitions,
    };

    Ok(topic)
}

fn handle_get_topics_response(response: &[u8]) -> Result<Vec<Topic>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut topics = Vec::new();
    let length = response.len();
    let mut position = 0;
    while position < length {
        let id = u32::from_le_bytes(response[position..position + 4].try_into()?);
        let partitions_count = u32::from_le_bytes(response[position + 4..position + 8].try_into()?);
        let name_length =
            u32::from_le_bytes(response[position + 8..position + 12].try_into()?) as usize;
        let name = from_utf8(&response[position + 12..position + 12 + name_length])?.to_string();
        topics.push(Topic {
            id,
            partitions_count,
            name,
        });
        position += 4 + 4 + 4 + name_length;

        if position >= length {
            break;
        }
    }

    topics.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(topics)
}
