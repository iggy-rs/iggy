use crate::client_error::ClientError;
use sdk::client::Client;

const PARTS: usize = 5;

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() != PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream = input[0].parse::<u32>()?;
    let topic = input[1].parse::<u32>()?;
    let key_kind = input[2].parse::<u8>()?;
    let key_value = input[3].parse::<u32>()?;
    let payload = input[4].as_bytes();

    client
        .send_message(stream, topic, key_kind, key_value, payload)
        .await?;
    Ok(())
}
