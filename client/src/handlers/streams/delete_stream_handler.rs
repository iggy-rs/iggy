use crate::client_error::ClientError;
use sdk::client::Client;
use shared::streams::delete_stream::DeleteStream;

pub async fn handle(command: DeleteStream, client: &mut Client) -> Result<(), ClientError> {
    client.delete_stream(&command).await?;
    Ok(())
}
