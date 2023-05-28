use sdk::error::Error;
use sdk::quic::client::{Client, ConnectedClient};
use tracing::info;

pub async fn create_connected_client(
    client_address: &str,
    server_address: &str,
    server_name: &str,
) -> Result<ConnectedClient, Error> {
    info!("Creating the client...");
    let client = Client::new(client_address, server_address, server_name)?;
    let client = client.connect().await?;
    info!("Connected the client.");
    Ok(client)
}
