use sdk::client::Client;
use sdk::client_error::ClientError;
use sdk::system::get_clients::GetClients;
use sdk::system::kill::Kill;
use sdk::system::ping::Ping;
use tracing::info;

pub async fn kill(command: &Kill, client: &dyn Client) -> Result<(), ClientError> {
    client.kill(command).await?;
    Ok(())
}

pub async fn ping(command: &Ping, client: &dyn Client) -> Result<(), ClientError> {
    client.ping(command).await?;
    Ok(())
}

pub async fn get_clients(command: &GetClients, client: &dyn Client) -> Result<(), ClientError> {
    let clients = client.get_clients(command).await?;
    if clients.is_empty() {
        info!("No clients found");
        return Ok(());
    }

    info!("Clients: {:#?}", clients);
    Ok(())
}
