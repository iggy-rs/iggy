use crate::message::Message;
use shared::error::Error;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tracing::{error, trace};

#[derive(Debug)]
pub struct TimeIndex {
    pub relative_offset: u32,
    pub timestamp: u64,
}

pub async fn load(file: &mut File) -> Result<Vec<TimeIndex>, Error> {
    trace!("Loading time indexes from file...");

    let file_size = file.metadata().await?.len() as usize;
    let indexes_count = file_size / 8;
    let mut indexes = Vec::with_capacity(indexes_count);
    let mut reader = BufReader::new(file);
    for offset in 0..indexes_count {
        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            error!(
                "Cannot read timestamp from time index file for offset: {}.",
                offset
            );
            break;
        }

        indexes.push(TimeIndex {
            relative_offset: offset as u32,
            timestamp: timestamp.unwrap(),
        });
    }

    if indexes.len() != indexes_count {
        error!(
            "Loaded {} time indexes from disk, expected {}.",
            indexes.len(),
            indexes_count
        );
    }

    trace!("Loaded {} time indexes from file.", indexes_count);

    Ok(indexes)
}

pub async fn persist(file: &mut File, messages: &Vec<Arc<Message>>) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(messages.len() * 8);
    for message in messages {
        bytes.extend(message.timestamp.to_le_bytes());
    }

    if file.write_all(&bytes).await.is_err() {
        return Err(Error::CannotSaveTimeIndexToSegment);
    }

    Ok(())
}
