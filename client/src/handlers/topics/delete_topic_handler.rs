use crate::client_error::ClientError;
use sdk::client::Client;

const PARTS: usize = 2;

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() != PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream_id = input[0].parse::<u32>()?;
    let topic_id = input[1].parse::<u32>()?;
    client.delete_topic(stream_id, topic_id).await?;
    Ok(())
}
