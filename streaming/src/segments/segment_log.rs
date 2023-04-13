use crate::message::Message;
use crate::stream_error::StreamError;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn save_log(file: &mut File, messages: &[Message]) -> Result<u64, StreamError> {
    let log_file_data = messages
        .iter()
        .map(|message| {
            let payload = message.payload.as_slice();
            let offset = &message.offset.to_le_bytes();
            let timestamp = &message.timestamp.to_le_bytes();
            let length = &message.length.to_le_bytes();
            [offset, timestamp, length, payload].concat()
        })
        .collect::<Vec<Vec<u8>>>()
        .concat();

    let saved_bytes = log_file_data.len() as u64;
    if file.write_all(&log_file_data).await.is_err() {
        return Err(StreamError::CannotSaveMessagesToSegment);
    }

    Ok(saved_bytes)
}
