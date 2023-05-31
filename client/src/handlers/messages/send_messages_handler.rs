use crate::client_error::ClientError;
use sdk::client::Client;
use shared::messages::send_messages::SendMessages;

pub async fn handle(command: SendMessages, client: &dyn Client) -> Result<(), ClientError> {
    client.send_messages(&command).await?;
    Ok(())
}
