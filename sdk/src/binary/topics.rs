use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use crate::topic::Topic;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::topics::create_topic::CreateTopic;
use shared::topics::delete_topic::DeleteTopic;
use shared::topics::get_topics::GetTopics;
use std::str::from_utf8;

pub async fn create_topic(client: &dyn BinaryClient, command: &CreateTopic) -> Result<(), Error> {
    client
        .send_with_response(
            [Command::CreateTopic.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
    Ok(())
}

pub async fn delete_topic(client: &dyn BinaryClient, command: &DeleteTopic) -> Result<(), Error> {
    client
        .send_with_response(
            [Command::DeleteTopic.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
    Ok(())
}

pub async fn get_topics(
    client: &dyn BinaryClient,
    command: &GetTopics,
) -> Result<Vec<Topic>, Error> {
    let response = client
        .send_with_response(
            [Command::GetTopics.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
    handle_response(&response)
}

fn handle_response(response: &[u8]) -> Result<Vec<Topic>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut topics = Vec::new();
    let length = response.len();
    let mut position = 0;
    while position < length {
        let id = u32::from_le_bytes(response[position..position + 4].try_into()?);
        let partitions = u32::from_le_bytes(response[position + 4..position + 8].try_into()?);
        let name_length =
            u32::from_le_bytes(response[position + 8..position + 12].try_into()?) as usize;
        let name = from_utf8(&response[position + 12..position + 12 + name_length]);
        topics.push(Topic {
            id,
            partitions,
            name: name.unwrap().to_string(),
        });
        position += 4 + 4 + 4 + name_length;

        if position >= length {
            break;
        }
    }

    topics.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(topics)
}
