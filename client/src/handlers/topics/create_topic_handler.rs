use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::topics::create_topic::CreateTopic;

pub async fn handle(command: CreateTopic, client: &ConnectedClient) -> Result<(), ClientError> {
    client.create_topic(&command).await?;
    Ok(())
}
