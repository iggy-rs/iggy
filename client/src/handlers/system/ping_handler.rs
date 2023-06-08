use crate::client_error::ClientError;
use sdk::client::Client;
use shared::system::ping::Ping;

pub async fn handle(command: Ping, client: &dyn Client) -> Result<(), ClientError> {
    client.ping(command).await?;
    Ok(())
}
