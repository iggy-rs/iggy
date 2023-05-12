use crate::client::ConnectedClient;
use crate::error::Error;
use crate::stream::Stream;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::get_streams::GetStreams;
use std::str::from_utf8;

impl ConnectedClient {
    pub async fn get_streams(&mut self, command: &GetStreams) -> Result<Vec<Stream>, Error> {
        let response = self
            .send_with_response(
                [Command::GetStreams.as_bytes(), command.as_bytes()]
                    .concat()
                    .as_slice(),
            )
            .await?;
        handle_response(response)
    }
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
