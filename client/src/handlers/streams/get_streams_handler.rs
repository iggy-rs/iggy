use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use tracing::info;

pub async fn handle(client: &mut ConnectedClient) -> Result<(), ClientError> {
    let streams = client.get_streams().await?;
    if streams.is_empty() {
        info!("No streams found");
        return Ok(());
    }

    info!("Streams: {:#?}", streams);
    Ok(())
}
