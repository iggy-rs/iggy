use crate::message::Message;
use crate::stream_error::StreamError;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn save_time_indexes(file: &mut File, messages: &[Message]) -> Result<(), StreamError> {
    let time_index_file_data = messages
        .iter()
        .map(|message| message.timestamp.to_le_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>()
        .concat();

    if file.write_all(&time_index_file_data).await.is_err() {
        return Err(StreamError::CannotSaveTimeIndexToSegment);
    }

    Ok(())
}
