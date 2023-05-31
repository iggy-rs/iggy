use crate::client::StreamClient;
use crate::error::Error;
use crate::quic::client::QuicClient;
use crate::stream::Stream;
use async_trait::async_trait;
use shared::bytes_serializable::BytesSerializable;
use shared::command::Command;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_streams::GetStreams;
use std::str::from_utf8;

#[async_trait]
impl StreamClient for QuicClient {
    async fn get_streams(&self, command: &GetStreams) -> Result<Vec<Stream>, Error> {
        let response = self
            .send_with_response(
                [Command::GetStreams.as_bytes(), command.as_bytes()]
                    .concat()
                    .as_slice(),
            )
            .await?;
        handle_response(&response)
    }

    async fn create_stream(&self, command: &CreateStream) -> Result<(), Error> {
        self.send_with_response(
            [Command::CreateStream.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, command: &DeleteStream) -> Result<(), Error> {
        self.send_with_response(
            [Command::DeleteStream.as_bytes(), command.as_bytes()]
                .concat()
                .as_slice(),
        )
        .await?;
        Ok(())
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
