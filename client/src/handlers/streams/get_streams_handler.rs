use crate::client_error::ClientError;
use sdk::quic::client::ConnectedClient;
use shared::streams::get_streams::GetStreams;
use tracing::info;

pub async fn handle(command: GetStreams, client: &ConnectedClient) -> Result<(), ClientError> {
    let streams = client.get_streams(&command).await?;
    if streams.is_empty() {
        info!("No streams found");
        return Ok(());
    }

    info!("Streams: {:#?}", streams);
    Ok(())
}
