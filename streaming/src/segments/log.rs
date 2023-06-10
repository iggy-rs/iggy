use crate::message::Message;
use crate::segments::index::IndexRange;
use shared::error::Error;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tracing::log::trace;

const EMPTY_MESSAGES: Vec<Arc<Message>> = vec![];

pub async fn load(file: &mut File, index_range: &IndexRange) -> Result<Vec<Arc<Message>>, Error> {
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(EMPTY_MESSAGES);
    }

    if index_range.end.position == 0 {
        return Ok(EMPTY_MESSAGES);
    }

    let mut messages = Vec::with_capacity(
        1 + (index_range.end.relative_offset - index_range.start.relative_offset) as usize,
    );
    let mut reader = BufReader::new(file);
    reader
        .seek(SeekFrom::Start(index_range.start.position as u64))
        .await?;

    let mut read_messages = 0;
    let messages_count =
        (1 + index_range.end.relative_offset - index_range.start.relative_offset) as usize;
    while read_messages < messages_count {
        let offset = reader.read_u64_le().await;
        if offset.is_err() {
            break;
        }

        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            return Err(Error::CannotReadMessageTimestamp);
        }

        let id = reader.read_u128_le().await;
        if id.is_err() {
            return Err(Error::CannotReadMessageId);
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
        let id = id.unwrap();
        messages.push(Arc::new(Message::create(offset, timestamp, id, payload)));
        read_messages += 1;
    }

    trace!("Loaded {} messages from disk.", messages.len());

    Ok(messages)
}

pub async fn load_message_ids(file: &mut File) -> Result<Vec<u128>, Error> {
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(Vec::new());
    }

    let mut message_ids = Vec::new();
    let mut reader = BufReader::new(file);
    loop {
        let offset = reader.read_u64_le().await;
        if offset.is_err() {
            break;
        }

        _ = reader.read_u64_le().await?;
        let id = reader.read_u128_le().await;
        if id.is_err() {
            return Err(Error::CannotReadMessageId);
        }

        let id = id.unwrap();
        message_ids.push(id);
        let length = reader.read_u32_le().await;
        // File seek() is way too slow, just read the message payload and ignore it for now.
        let mut payload = vec![0; length.unwrap() as usize];
        if reader.read_exact(&mut payload).await.is_err() {
            continue;
        }
    }

    trace!("Loaded {} message IDs from disk.", message_ids.len());

    Ok(message_ids)
}

pub async fn persist(
    file: &mut File,
    messages: &Vec<Arc<Message>>,
    enforce_sync: bool,
) -> Result<u32, Error> {
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

    if enforce_sync && file.sync_all().await.is_err() {
        return Err(Error::CannotSaveMessagesToSegment);
    }

    Ok(messages_size)
}
