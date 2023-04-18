use crate::client_error::ClientError;
use sdk::client::Client;
use shared::topics::create_topic::CreateTopic;

pub async fn handle(command: CreateTopic, client: &mut Client) -> Result<(), ClientError> {
    client.create_topic(&command).await?;
    Ok(())
}
