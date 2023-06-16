use crate::client_error::ClientError;
use sdk::client::Client;
use shared::system::kill::Kill;
use shared::system::ping::Ping;

pub async fn kill(command: &Kill, client: &dyn Client) -> Result<(), ClientError> {
    client.kill(command).await?;
    Ok(())
}

pub async fn ping(command: &Ping, client: &dyn Client) -> Result<(), ClientError> {
    client.ping(command).await?;
    Ok(())
}
