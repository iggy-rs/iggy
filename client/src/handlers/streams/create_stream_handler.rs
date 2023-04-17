use crate::client_error::ClientError;
use sdk::client::Client;

const PARTS: usize = 2;

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() != PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream_id = input[0].parse::<u32>()?;
    let name = input[1];
    client.create_stream(stream_id, name).await?;
    Ok(())
}
