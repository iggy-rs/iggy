use crate::client_error::ClientError;
use sdk::client::Client;
use std::str::from_utf8;
use tracing::info;

const PARTS: usize = 6;

enum Format {
    Binary,
    String,
}

pub async fn handle(client: &mut Client, input: &[&str]) -> Result<(), ClientError> {
    if input.len() < PARTS {
        return Err(ClientError::InvalidCommandParts);
    }

    let stream_id = input[0].parse::<u32>()?;
    let topic_id = input[1].parse::<u32>()?;
    let partition_id = input[2].parse::<u32>()?;
    let kind = input[3].parse::<u8>()?;
    let value = input[4].parse::<u64>()?;
    let count = input[5].parse::<u32>()?;
    let format = match input.get(6) {
        Some(format) => match *format {
            "b" => Format::Binary,
            "s" => Format::String,
            _ => return Err(ClientError::InvalidFormat),
        },
        None => Format::Binary,
    };

    let messages = client
        .poll_messages(stream_id, topic_id, partition_id, kind, value, count)
        .await?;

    if messages.is_empty() {
        info!("No messages found");
        return Ok(());
    }

    let mut text = format!("Received {} messages:", messages.len());
    for message in messages {
        text += &format!(
            "\noffset: {}, timestamp: {}, length: {}, payload: ",
            message.offset, message.timestamp, message.length
        );
        match format {
            Format::Binary => text += &format!("{:?}", message.payload),
            Format::String => text += from_utf8(&message.payload).unwrap(),
        }
    }

    info!("{}", text);
    Ok(())
}
