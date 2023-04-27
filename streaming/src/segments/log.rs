use crate::message::Message;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub async fn persist(file: &mut File, messages: &[Message]) -> Result<u32, Error> {
    let messages_size = messages
        .iter()
        .map(|message| message.get_size_bytes())
        .sum::<u32>();

    let mut bytes = Vec::with_capacity(messages_size as usize);
    for message in messages {
        message.extend(&mut bytes);
    }

    // let saved_bytes = log_file_data.len() as u64;
    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveMessagesToSegment);
    }

    Ok(messages_size)
}
