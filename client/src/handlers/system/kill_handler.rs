use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::system::kill::Kill;

pub async fn handle(command: Kill, client: &ConnectedClient) -> Result<(), ClientError> {
    client.kill(&command).await?;
    Ok(())
}
