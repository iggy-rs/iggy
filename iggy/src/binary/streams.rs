use crate::binary::binary_client::BinaryClient;
use crate::binary::mapper;
use crate::bytes_serializable::BytesSerializable;
use crate::command::{
    CREATE_STREAM_CODE, DELETE_STREAM_CODE, GET_STREAMS_CODE, GET_STREAM_CODE, UPDATE_STREAM_CODE,
};
use crate::error::Error;
use crate::models::stream::{Stream, StreamDetails};
use crate::streams::create_stream::CreateStream;
use crate::streams::delete_stream::DeleteStream;
use crate::streams::get_stream::GetStream;
use crate::streams::get_streams::GetStreams;
use crate::streams::update_stream::UpdateStream;

pub async fn get_stream(
    client: &dyn BinaryClient,
    command: &GetStream,
) -> Result<StreamDetails, Error> {
    let response = client
        .send_with_response(GET_STREAM_CODE, &command.as_bytes())
        .await?;
    mapper::map_stream(&response)
}

pub async fn get_streams(
    client: &dyn BinaryClient,
    command: &GetStreams,
) -> Result<Vec<Stream>, Error> {
    let response = client
        .send_with_response(GET_STREAMS_CODE, &command.as_bytes())
        .await?;
    mapper::map_streams(&response)
}

pub async fn create_stream(client: &dyn BinaryClient, command: &CreateStream) -> Result<(), Error> {
    client
        .send_with_response(CREATE_STREAM_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn delete_stream(client: &dyn BinaryClient, command: &DeleteStream) -> Result<(), Error> {
    client
        .send_with_response(DELETE_STREAM_CODE, &command.as_bytes())
        .await?;
    Ok(())
}

pub async fn update_stream(client: &dyn BinaryClient, command: &UpdateStream) -> Result<(), Error> {
    client
        .send_with_response(UPDATE_STREAM_CODE, &command.as_bytes())
        .await?;
    Ok(())
}
