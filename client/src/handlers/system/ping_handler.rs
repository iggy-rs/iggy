use crate::client_error::ClientError;
use sdk::client::Client;

pub async fn handle(client: &mut Client) -> Result<(), ClientError> {
    client.ping().await?;
    Ok(())
}
