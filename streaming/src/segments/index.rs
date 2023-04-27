use crate::message::Message;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct Index {
    pub offset: u32,
    pub position: u32,
}

pub async fn persist(
    file: &mut File,
    current_bytes: u32,
    messages: &[Message],
) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(messages.len() * 4);
    let mut current_position = current_bytes;

    for message in messages {
        current_position += message.get_size_bytes();
        bytes.extend(current_position.to_le_bytes());
    }

    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveIndexToSegment);
    }

    Ok(())
}
