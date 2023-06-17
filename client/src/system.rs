use sdk::client::Client;
use sdk::client_error::ClientError;
use sdk::system::kill::Kill;
use sdk::system::ping::Ping;

pub async fn kill(command: &Kill, client: &dyn Client) -> Result<(), ClientError> {
    client.kill(command).await?;
    Ok(())
}

pub async fn ping(command: &Ping, client: &dyn Client) -> Result<(), ClientError> {
    client.ping(command).await?;
    Ok(())
}
