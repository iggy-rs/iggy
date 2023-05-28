use crate::client_error::ClientError;
use sdk::quic::client::ConnectedClient;
use shared::messages::send_messages::SendMessages;

pub async fn handle(command: SendMessages, client: &ConnectedClient) -> Result<(), ClientError> {
    client.send_messages(&command).await?;
    Ok(())
}
