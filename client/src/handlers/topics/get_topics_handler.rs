use crate::client_error::ClientError;
use sdk::client::ConnectedClient;
use shared::topics::get_topics::GetTopics;
use tracing::info;

pub async fn handle(command: GetTopics, client: &ConnectedClient) -> Result<(), ClientError> {
    let topics = client.get_topics(&command).await?;
    if topics.is_empty() {
        info!("No topics found");
        return Ok(());
    }

    info!("Topics: {:#?}", topics);
    Ok(())
}
