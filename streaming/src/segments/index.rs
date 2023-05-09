use crate::message::Message;
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::trace;

const INDEX_SIZE: u64 = 4;

#[derive(Debug)]
pub struct Index {
    pub relative_offset: u32,
    pub position: u32,
}

#[derive(Debug)]
pub struct IndexRange {
    pub start: OffsetPosition,
    pub end: OffsetPosition,
}

#[derive(Debug)]
pub struct OffsetPosition {
    pub offset: u32,
    pub position: u32,
}

pub async fn load_range(
    file: &mut File,
    segment_start_offset: u64,
    mut index_start_offset: u64,
    index_end_offset: u64,
) -> Result<IndexRange, Error> {
    trace!(
        "Loading index range for offsets: {} to {}, segment starts at: {}",
        index_start_offset,
        index_end_offset,
        segment_start_offset
    );

    if index_start_offset < segment_start_offset {
        index_start_offset = segment_start_offset - 1;
    }

    let relative_start_offset = index_start_offset - segment_start_offset;
    let relative_end_offset = index_end_offset - segment_start_offset;
    let start_seek_position = (1 + relative_start_offset) * INDEX_SIZE;
    let mut end_seek_position = (1 + relative_end_offset) * INDEX_SIZE;
    let file_length = file.metadata().await?.len();
    if end_seek_position > file_length {
        end_seek_position = file_length - INDEX_SIZE;
    }

    file.seek(std::io::SeekFrom::Start(start_seek_position))
        .await?;
    let start_position = file.read_u32_le().await?;
    file.seek(std::io::SeekFrom::Start(end_seek_position))
        .await?;
    let end_position = file.read_u32_le().await?;

    trace!(
        "Loaded index range: {}...{}, position range: {}...{}",
        relative_start_offset,
        relative_end_offset,
        start_position,
        end_position
    );

    Ok(IndexRange {
        start: OffsetPosition {
            offset: relative_start_offset as u32,
            position: start_position,
        },
        end: OffsetPosition {
            offset: relative_end_offset as u32,
            position: end_position,
        },
    })
}

pub async fn persist(
    file: &mut File,
    current_bytes: u32,
    messages: &Vec<Arc<Message>>,
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
