use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::system::ping::Ping;

pub async fn handle(command: Ping, client: &ConnectedClient) -> Result<(), ClientError> {
    client.ping(&command).await?;
    Ok(())
}
