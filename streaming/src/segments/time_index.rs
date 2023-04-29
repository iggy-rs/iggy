use crate::message::Message;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct TimeIndex {
    pub offset: u32,
    pub timestamp: u64,
}

pub async fn persist(file: &mut File, messages: &[&Message]) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(messages.len() * 8);
    for message in messages {
        bytes.extend(message.timestamp.to_le_bytes());
    }

    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveTimeIndexToSegment);
    }

    Ok(())
}
