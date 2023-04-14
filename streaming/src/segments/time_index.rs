use crate::error::Error;
use crate::message::Message;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct TimeIndex {
    pub offset: u64,
    pub timestamp: u64,
}

pub async fn persist(file: &mut File, messages: &[Message]) -> Result<(), Error> {
    let time_index_file_data = messages
        .iter()
        .map(|message| message.timestamp.to_le_bytes().to_vec())
        .collect::<Vec<Vec<u8>>>()
        .concat();

    if file.write_all(&time_index_file_data).await.is_err() {
        return Err(Error::CannotSaveTimeIndexToSegment);
    }

    Ok(())
}
