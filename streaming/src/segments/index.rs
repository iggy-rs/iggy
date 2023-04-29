use crate::message::Message;
use shared::error::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const INDEX_SIZE: u64 = 4;

pub struct Index {
    pub offset: u32,
    pub position: u32,
}

pub struct IndexRange {
    pub start_position: u32,
    pub end_position: u32,
}

pub async fn load_range(
    file: &mut File,
    segment_start_offset: u64,
    index_start_offset: u64,
    index_end_offset: u64,
) -> Result<IndexRange, Error> {
    let relative_start_offset = 1 + index_start_offset - segment_start_offset;
    let relative_end_offset = 1 + index_end_offset - segment_start_offset;
    let start_seek_position = relative_start_offset * INDEX_SIZE;
    let mut end_seek_position = relative_end_offset * INDEX_SIZE;

    let mut start_buffer = vec![0; INDEX_SIZE as usize];
    let mut end_buffer = vec![0; INDEX_SIZE as usize];

    let file_length = file.metadata().await?.len();
    if end_seek_position > file_length {
        end_seek_position = file_length - INDEX_SIZE;
    }

    file.seek(std::io::SeekFrom::Start(start_seek_position))
        .await?;
    file.read_exact(&mut start_buffer).await?;
    let start_position = u32::from_le_bytes(start_buffer.try_into().unwrap());
    file.seek(std::io::SeekFrom::Start(end_seek_position))
        .await?;
    file.read_exact(&mut end_buffer).await?;
    let end_position = u32::from_le_bytes(end_buffer.try_into().unwrap());

    Ok(IndexRange {
        start_position,
        end_position,
    })
}

pub async fn persist(
    file: &mut File,
    current_bytes: u32,
    messages: &[&Message],
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
