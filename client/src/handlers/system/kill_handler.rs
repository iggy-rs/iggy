use crate::client_error::ClientError;
use sdk::client::Client;
use shared::system::kill::Kill;

pub async fn handle(command: &Kill, client: &dyn Client) -> Result<(), ClientError> {
    client.kill(command).await?;
    Ok(())
}
