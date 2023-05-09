use crate::message::Message;
use crate::segments::index::IndexRange;
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tracing::error;

pub async fn load(file: &mut File, range: IndexRange) -> Result<Vec<Arc<Message>>, Error> {
    let mut messages = Vec::with_capacity(1 + (range.end.offset - range.start.offset) as usize);
    let mut reader = BufReader::new(file);
    reader
        .seek(std::io::SeekFrom::Start(range.start.position as u64))
        .await?;

    let mut read_messages = 0;
    let messages_count = (1 + range.end.offset - range.start.offset) as usize;
    while read_messages < messages_count {
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
        let timestamp = timestamp.unwrap();
        messages.push(Arc::new(Message::create(offset, timestamp, payload)));
        read_messages += 1;
    }

    if messages.len() != messages_count {
        error!(
            "Loaded {} messages from disk, expected {}.",
            messages.len(),
            messages_count
        );
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
