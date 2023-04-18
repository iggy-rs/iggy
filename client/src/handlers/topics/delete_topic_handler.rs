use crate::client_error::ClientError;
use sdk::client::Client;
use shared::topics::delete_topic::DeleteTopic;

pub async fn handle(command: DeleteTopic, client: &mut Client) -> Result<(), ClientError> {
    client.delete_topic(&command).await?;
    Ok(())
}
