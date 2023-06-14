use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use crate::stream::{Stream, StreamDetails};
use crate::topic::Topic;
use shared::command::Command;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_stream::GetStream;
use shared::streams::get_streams::GetStreams;
use std::str::from_utf8;

pub async fn get_stream(
    client: &dyn BinaryClient,
    command: GetStream,
) -> Result<StreamDetails, Error> {
    let response = client
        .send_with_response(Command::GetStream(command))
        .await?;
    handle_get_stream_response(&response)
}

pub async fn get_streams(
    client: &dyn BinaryClient,
    command: GetStreams,
) -> Result<Vec<Stream>, Error> {
    let response = client
        .send_with_response(Command::GetStreams(command))
        .await?;
    handle_get_streams_response(&response)
}

pub async fn create_stream(client: &dyn BinaryClient, command: CreateStream) -> Result<(), Error> {
    client
        .send_with_response(Command::CreateStream(command))
        .await?;
    Ok(())
}

pub async fn delete_stream(client: &dyn BinaryClient, command: DeleteStream) -> Result<(), Error> {
    client
        .send_with_response(Command::DeleteStream(command))
        .await?;
    Ok(())
}

fn handle_get_stream_response(response: &[u8]) -> Result<StreamDetails, Error> {
    let id = u32::from_le_bytes(response[0..4].try_into()?);
    let topics_count = u32::from_le_bytes(response[4..8].try_into()?);
    let name_length = u32::from_le_bytes(response[8..12].try_into()?) as usize;
    let name = from_utf8(&response[12..12 + name_length])?.to_string();
    let mut topics = Vec::new();
    let length = response.len();
    let mut position = 12 + name_length;
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
    let stream = StreamDetails {
        id,
        topics_count,
        name,
        topics,
    };

    Ok(stream)
}

fn handle_get_streams_response(response: &[u8]) -> Result<Vec<Stream>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut streams = Vec::new();
    let length = response.len();
    let mut position = 0;
    while position < length {
        let id = u32::from_le_bytes(response[position..position + 4].try_into()?);
        let topics_count = u32::from_le_bytes(response[position + 4..position + 8].try_into()?);
        let name_length =
            u32::from_le_bytes(response[position + 8..position + 12].try_into()?) as usize;
        let name = from_utf8(&response[position + 12..position + 12 + name_length])?.to_string();
        streams.push(Stream {
            id,
            topics_count,
            name,
        });
        position += 4 + 4 + 4 + name_length;

        if position >= length {
            break;
        }
    }

    streams.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(streams)
}
