use crate::client_error::ClientError;
use sdk::client::Client;
use shared::topics::get_topic::GetTopic;
use tracing::info;

pub async fn handle(command: GetTopic, client: &dyn Client) -> Result<(), ClientError> {
    let topic = client.get_topic(command).await?;
    info!("Topic: {:#?}", topic);
    Ok(())
}
