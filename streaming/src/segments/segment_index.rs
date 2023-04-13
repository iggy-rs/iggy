use crate::message::Message;
use crate::stream_error::StreamError;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn save_indexes(
    file: &mut File,
    current_bytes: u64,
    messages: &[Message],
) -> Result<(), StreamError> {
    let mut current_position = current_bytes;
    let index_file_data = messages.iter().fold(vec![], |mut acc, message| {
        current_position += message.get_size_bytes();
        acc.extend_from_slice(&current_position.to_le_bytes());
        acc
    });

    if file.write_all(&index_file_data).await.is_err() {
        return Err(StreamError::CannotSaveIndexToSegment);
    }

    Ok(())
}
