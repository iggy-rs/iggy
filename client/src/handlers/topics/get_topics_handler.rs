use crate::client_error::ClientError;
use sdk::client::Client;
use shared::topics::get_topics::GetTopics;
use tracing::info;

pub async fn handle(command: GetTopics, client: &dyn Client) -> Result<(), ClientError> {
    let topics = client.get_topics(command).await?;
    if topics.is_empty() {
        info!("No topics found");
        return Ok(());
    }

    info!("Topics: {:#?}", topics);
    Ok(())
}
