use crate::client_error::ClientError;
use sdk::client::Client;
use shared::streams::create_stream::CreateStream;

pub async fn handle(command: CreateStream, client: &dyn Client) -> Result<(), ClientError> {
    client.create_stream(&command).await?;
    Ok(())
}
