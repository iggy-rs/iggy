use crate::client_error::ClientError;
use sdk::client::Client;
use shared::messages::send_message::SendMessage;

pub async fn handle(command: SendMessage, client: &mut Client) -> Result<(), ClientError> {
    client.send_message(&command).await?;
    Ok(())
}
