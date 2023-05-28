use crate::client_error::ClientError;
use sdk::quic::client::ConnectedClient;
use shared::streams::create_stream::CreateStream;

pub async fn handle(command: CreateStream, client: &ConnectedClient) -> Result<(), ClientError> {
    client.create_stream(&command).await?;
    Ok(())
}
