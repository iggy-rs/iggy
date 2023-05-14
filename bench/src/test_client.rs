use sdk::client::{Client, ConnectedClient};
use sdk::error::Error;
use tracing::info;

pub async fn create_connected_client(
    server_address: &str,
    server_name: &str,
) -> Result<ConnectedClient, Error> {
    info!("Creating the client...");
    let client_address = "127.0.0.1:0";
    let client = Client::new(client_address, server_address, server_name)?;
    let client = client.connect().await?;
    info!("Connected the client.");
    Ok(client)
}
