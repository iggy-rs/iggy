use crate::client_error::ClientError;
use sdk::client::Client;

const PARTS: usize = 4;

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() != PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream_id = input[0].parse::<u32>()?;
    let topic_id = input[1].parse::<u32>()?;
    let partitions_count = input[2].parse::<u32>()?;
    let name = input[3];
    client
        .create_topic(stream_id, topic_id, partitions_count, name)
        .await?;
    Ok(())
}
