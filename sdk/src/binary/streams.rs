use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::error::Error;
use crate::stream::{Stream, StreamDetails};
use shared::command::Command;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_stream::GetStream;
use shared::streams::get_streams::GetStreams;

pub async fn get_stream(
    client: &dyn BinaryClient,
    command: GetStream,
) -> Result<StreamDetails, Error> {
    let response = client
        .send_with_response(Command::GetStream(command))
        .await?;

    let stream = mapper::map_stream(&response)?;
    Ok(stream)
}

pub async fn get_streams(
    client: &dyn BinaryClient,
    command: GetStreams,
) -> Result<Vec<Stream>, Error> {
    let response = client
        .send_with_response(Command::GetStreams(command))
        .await?;

    if response.is_empty() {
        return Ok(Vec::new());
    }

    let mut streams = mapper::map_streams(&response)?;
    streams.sort_by(|x, y| x.id.cmp(&y.id));
    Ok(streams)
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
