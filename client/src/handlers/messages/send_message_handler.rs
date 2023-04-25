use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::messages::send_message::SendMessage;

pub async fn handle(command: SendMessage, client: &mut ConnectedClient) -> Result<(), ClientError> {
    client.send_message(&command).await?;
    Ok(())
}
