use crate::message::Message;
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

pub async fn load(
    file: &mut File,
    start_offset: u32,
    end_offset: u32,
) -> Result<Vec<Message>, Error> {
    let mut messages = Vec::with_capacity(1 + (end_offset - start_offset) as usize);
    let mut reader = BufReader::new(file);
    loop {
        let offset = reader.read_u64_le().await;
        if offset.is_err() {
            break;
        }

        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            return Err(Error::CannotReadMessageTimestamp);
        }

        let length = reader.read_u32_le().await;
        if length.is_err() {
            return Err(Error::CannotReadMessageLength);
        }

        let mut payload = vec![0; length.unwrap() as usize];
        if reader.read_exact(&mut payload).await.is_err() {
            return Err(Error::CannotReadMessagePayload);
        }

        let offset = offset.unwrap();
        let message = Message::create(offset, timestamp.unwrap(), payload);
        messages.push(message);
    }

    Ok(messages)
}

pub async fn persist(file: &mut File, messages: &Vec<Arc<Message>>) -> Result<u32, Error> {
    let messages_size = messages
        .iter()
        .map(|message| message.get_size_bytes())
        .sum::<u32>();

    let mut bytes = Vec::with_capacity(messages_size as usize);
    for message in messages {
        message.extend(&mut bytes);
    }

    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveMessagesToSegment);
    }

    Ok(messages_size)
}
