use crate::client_error::ClientError;
use sdk::client::Client;
use shared::streams::create_stream::CreateStream;
use shared::streams::delete_stream::DeleteStream;
use shared::streams::get_stream::GetStream;
use shared::streams::get_streams::GetStreams;
use tracing::info;

pub async fn get_stream(command: &GetStream, client: &dyn Client) -> Result<(), ClientError> {
    let stream = client.get_stream(command).await?;
    info!("Stream: {:#?}", stream);
    Ok(())
}

pub async fn get_streams(command: &GetStreams, client: &dyn Client) -> Result<(), ClientError> {
    let streams = client.get_streams(command).await?;
    if streams.is_empty() {
        info!("No streams found");
        return Ok(());
    }

    info!("Streams: {:#?}", streams);
    Ok(())
}

pub async fn create_stream(command: &CreateStream, client: &dyn Client) -> Result<(), ClientError> {
    client.create_stream(command).await?;
    Ok(())
}

pub async fn delete_stream(command: &DeleteStream, client: &dyn Client) -> Result<(), ClientError> {
    client.delete_stream(command).await?;
    Ok(())
}
