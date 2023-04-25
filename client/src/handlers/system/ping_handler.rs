use crate::client_error::ClientError;
use sdk::client::ConnectedClient;

pub async fn handle(client: &mut ConnectedClient) -> Result<(), ClientError> {
    client.ping().await?;
    Ok(())
}
