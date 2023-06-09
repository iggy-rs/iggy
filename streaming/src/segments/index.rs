use crate::message::Message;
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tracing::{error, trace, warn};

const EMPTY_INDEXES: Vec<Index> = vec![];

const INDEX_SIZE: u32 = 4;

#[derive(Debug)]
pub struct Index {
    pub relative_offset: u32,
    pub position: u32,
}

#[derive(Debug)]
pub struct IndexRange {
    pub start: Index,
    pub end: Index,
}

pub async fn load_all(file: &mut File) -> Result<Vec<Index>, Error> {
    trace!("Loading indexes from file...");
    let file_size = file.metadata().await?.len() as usize;
    if file_size == 0 {
        trace!("Index file is empty.");
        return Ok(EMPTY_INDEXES);
    }

    let indexes_count = file_size / 4;
    let mut indexes = Vec::with_capacity(indexes_count);
    let mut reader = BufReader::new(file);
    for offset in 0..indexes_count {
        let position = reader.read_u32_le().await;
        if position.is_err() {
            error!(
                "Cannot read position from index file for offset: {}.",
                offset
            );
            break;
        }

        indexes.push(Index {
            relative_offset: offset as u32,
            position: position.unwrap(),
        });
    }

    if indexes.len() != indexes_count {
        error!(
            "Loaded {} indexes from disk, expected {}.",
            indexes.len(),
            indexes_count
        );
    }

    trace!("Loaded {} indexes from file.", indexes_count);

    Ok(indexes)
}

pub async fn load_range(
    file: &mut File,
    segment_start_offset: u64,
    mut index_start_offset: u64,
    index_end_offset: u64,
) -> Result<Option<IndexRange>, Error> {
    trace!(
        "Loading index range for offsets: {} to {}, segment starts at: {}",
        index_start_offset,
        index_end_offset,
        segment_start_offset
    );

    if index_start_offset > index_end_offset {
        warn!(
            "Index start offset: {} is greater than index end offset: {}.",
            index_start_offset, index_end_offset
        );
        return Ok(None);
    }

    let file_length = file.metadata().await?.len() as u32;
    if file_length == 0 {
        trace!("Index file is empty.");
        return Ok(None);
    }

    trace!("Index file length: {}.", file_length);

    if index_start_offset < segment_start_offset {
        index_start_offset = segment_start_offset - 1;
    }

    let relative_start_offset = (index_start_offset - segment_start_offset) as u32;
    let relative_end_offset = (index_end_offset - segment_start_offset) as u32;
    let start_seek_position = relative_start_offset * INDEX_SIZE;
    let mut end_seek_position = relative_end_offset * INDEX_SIZE;
    if end_seek_position >= file_length {
        end_seek_position = file_length - INDEX_SIZE;
    }

    if start_seek_position >= end_seek_position {
        trace!(
            "Start seek position: {} is greater than or equal to end seek position: {}.",
            start_seek_position,
            end_seek_position
        );
        return Ok(None);
    }

    trace!(
        "Seeking to index range: {}...{}, position range: {}...{}",
        relative_start_offset,
        relative_end_offset,
        start_seek_position,
        end_seek_position
    );
    file.seek(std::io::SeekFrom::Start(start_seek_position as u64))
        .await?;
    let start_position = file.read_u32_le().await?;
    file.seek(std::io::SeekFrom::Start(end_seek_position as u64))
        .await?;
    let mut end_position = file.read_u32_le().await?;
    if end_position == 0 {
        end_position = file_length;
    }

    trace!(
        "Loaded index range: {}...{}, position range: {}...{}",
        relative_start_offset,
        relative_end_offset,
        start_position,
        end_position
    );

    Ok(Some(IndexRange {
        start: Index {
            relative_offset: relative_start_offset,
            position: start_position,
        },
        end: Index {
            relative_offset: relative_end_offset,
            position: end_position,
        },
    }))
}

pub async fn persist(
    file: &mut File,
    mut current_position: u32,
    messages: &Vec<Arc<Message>>,
    enforce_sync: bool,
) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(messages.len() * 4);

    for message in messages {
        trace!("Persisting index for position: {}", current_position);
        bytes.extend(current_position.to_le_bytes());
        current_position += message.get_size_bytes();
    }

    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveIndexToSegment);
    }

    if enforce_sync && file.sync_all().await.is_err() {
        return Err(Error::CannotSaveIndexToSegment);
    }

    Ok(())
}
