use crate::client_error::ClientError;
use sdk::quic::client::ConnectedClient;
use shared::topics::delete_topic::DeleteTopic;

pub async fn handle(command: DeleteTopic, client: &ConnectedClient) -> Result<(), ClientError> {
    client.delete_topic(&command).await?;
    Ok(())
}
