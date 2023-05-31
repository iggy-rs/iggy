use crate::client_error::ClientError;
use sdk::client::Client;
use shared::streams::get_streams::GetStreams;
use tracing::info;

pub async fn handle(command: GetStreams, client: &dyn Client) -> Result<(), ClientError> {
    let streams = client.get_streams(&command).await?;
    if streams.is_empty() {
        info!("No streams found");
        return Ok(());
    }

    info!("Streams: {:#?}", streams);
    Ok(())
}
