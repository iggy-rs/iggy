use crate::binary::binary_client::BinaryClient;
use crate::error::Error;
use crate::stream::Stream;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_streams::GetStreams;
use std::str::from_utf8;

pub async fn get_streams(
    client: &dyn BinaryClient,
    command: &GetStreams,
) -> Result<Vec<Stream>, Error> {
    let response = client
        .send_with_response(Command::GetStreams, &command.as_bytes())
        .await?;
    handle_response(&response)
}

pub async fn create_stream(client: &dyn BinaryClient, command: &CreateStream) -> Result<(), Error> {
    client
        .send_with_response(Command::CreateStream, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_stream(client: &dyn BinaryClient, command: &DeleteStream) -> Result<(), Error> {
    client
        .send_with_response(Command::DeleteStream, &command.as_bytes())
        .await?;
    Ok(())
}

fn handle_response(response: &[u8]) -> Result<Vec<Stream>, Error> {
    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut streams = Vec::new();
    let length = response.len();
    let mut position = 0;
    while position < length {
        let id = u32::from_le_bytes(response[position..position + 4].try_into()?);
        let topics = u32::from_le_bytes(response[position + 4..position + 8].try_into()?);
        let name_length =
            u32::from_le_bytes(response[position + 8..position + 12].try_into()?) as usize;
        let name = from_utf8(&response[position + 12..position + 12 + name_length])?;
        streams.push(Stream {
            id,
            topics,
            name: name.to_string(),
        });
        position += 4 + 4 + 4 + name_length;

        if position >= length {
            break;
        }
    }

    streams.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(streams)
}
