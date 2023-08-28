use iggy::client::Client;
use iggy::client_error::ClientError;
use iggy::system::get_client::GetClient;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
use tracing::info;

pub async fn ping(command: &Ping, client: &dyn Client) -> Result<(), ClientError> {
    client.ping(command).await?;
    Ok(())
}

pub async fn get_stats(command: &GetStats, client: &dyn Client) -> Result<(), ClientError> {
    let stats = client.get_stats(command).await?;
    info!("Stats: {:#?}", stats);
    Ok(())
}

pub async fn get_me(command: &GetMe, client: &dyn Client) -> Result<(), ClientError> {
    let me = client.get_me(command).await?;
    info!("Me: {:#?}", me);
    Ok(())
}

pub async fn get_client(command: &GetClient, client: &dyn Client) -> Result<(), ClientError> {
    let client_info = client.get_client(command).await?;
    info!("Client: {:#?}", client_info);
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
