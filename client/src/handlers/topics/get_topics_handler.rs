use crate::client_error::ClientError;
use sdk::client::Client;
use tracing::info;

const PARTS: usize = 1;

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() != PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream_id = input[0].parse::<u32>()?;
    let topics = client.get_topics(stream_id).await?;
    if topics.is_empty() {
        info!("No topics found");
        return Ok(());
    }

    info!("Topics: {:#?}", topics);
    Ok(())
}
