use crate::client_error::ClientError;
use sdk::quic::client::ConnectedClient;
use shared::streams::delete_stream::DeleteStream;

pub async fn handle(command: DeleteStream, client: &ConnectedClient) -> Result<(), ClientError> {
    client.delete_stream(&command).await?;
    Ok(())
}
