use crate::message::Message;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct Index {
    pub offset: u64,
    pub position: u64,
}

pub async fn persist(
    file: &mut File,
    current_bytes: u64,
    messages: &[Message],
) -> Result<(), Error> {
    let mut current_position = current_bytes;
    let index_file_data = messages.iter().fold(vec![], |mut acc, message| {
        current_position += message.get_size_bytes();
        acc.extend_from_slice(&current_position.to_le_bytes());
        acc
    });

    if file.write_all(&index_file_data).await.is_err() {
        return Err(Error::CannotSaveIndexToSegment);
    }

    Ok(())
}
