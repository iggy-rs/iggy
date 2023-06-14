use crate::client_error::ClientError;
use sdk::client::Client;
use shared::streams::get_stream::GetStream;
use tracing::info;

pub async fn handle(command: GetStream, client: &dyn Client) -> Result<(), ClientError> {
    let stream = client.get_stream(command).await?;
    info!("Stream: {:#?}", stream);
    Ok(())
}
