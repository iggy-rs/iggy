use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::storage::{SegmentStorage, Storage};
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, Bytes};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::Error;
use iggy::models::messages::{Message, MessageState};
use iggy::utils::checksum;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tracing::log::{trace, warn};
use tracing::{error, info};

const EMPTY_INDEXES: Vec<Index> = vec![];
const EMPTY_TIME_INDEXES: Vec<TimeIndex> = vec![];
const INDEX_SIZE: u32 = 4;
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1024;

#[derive(Debug)]
pub struct FileSegmentStorage {
    persister: Arc<dyn Persister>,
}

impl FileSegmentStorage {
    pub fn new(persister: Arc<dyn Persister>) -> Self {
        Self { persister }
    }
}

unsafe impl Send for FileSegmentStorage {}
unsafe impl Sync for FileSegmentStorage {}

// TODO: Split into smaller components.
#[async_trait]
impl Storage<Segment> for FileSegmentStorage {
    async fn load(&self, segment: &mut Segment) -> Result<(), Error> {
        info!(
            "Loading segment from disk for start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {} ...",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id
        );
        let log_file = file::open(&segment.log_path).await?;
        let file_size = log_file.metadata().await.unwrap().len() as u32;
        segment.current_size_bytes = file_size;

        info!(
            "Segment log file for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {} has {} bytes of size.",
            segment.start_offset, segment.current_offset, segment.partition_id, segment.topic_id, segment.stream_id, segment.current_size_bytes
        );

        if segment.config.segment.cache_indexes {
            segment.indexes = Some(segment.storage.segment.load_all_indexes(segment).await?);
            info!(
                "Loaded {} indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                segment.indexes.as_ref().unwrap().len(),
                segment.start_offset,
                segment.partition_id,
                segment.topic_id,
                segment.stream_id
            );
        }

        if segment.config.segment.cache_time_indexes {
            let time_indexes = self.load_all_time_indexes(segment).await?;
            if !time_indexes.is_empty() {
                let last_index = time_indexes.last().unwrap();
                segment.current_offset = segment.start_offset + last_index.relative_offset as u64;
                segment.time_indexes = Some(time_indexes);
            }

            info!(
                "Loaded {} time indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                segment.time_indexes.as_ref().unwrap().len(),
                segment.start_offset,
                segment.partition_id,
                segment.topic_id,
                segment.stream_id
            );
        } else {
            let last_time_index = self.load_last_time_index(segment).await?;
            if let Some(last_index) = last_time_index {
                segment.current_offset = segment.start_offset + last_index.relative_offset as u64;
                info!(
                "Loaded last time index for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                segment.start_offset,
                segment.partition_id,
                segment.topic_id,
                segment.stream_id
            );
            }
        }

        if segment.is_full().await {
            segment.is_closed = true;
        }

        Ok(())
    }

    async fn save(&self, segment: &Segment) -> Result<(), Error> {
        info!("Saving segment with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id);
        if !Path::new(&segment.log_path).exists()
            && self
                .persister
                .overwrite(&segment.log_path, &[])
                .await
                .is_err()
        {
            return Err(Error::CannotCreateSegmentLogFile(segment.log_path.clone()));
        }

        if !Path::new(&segment.time_index_path).exists()
            && self
                .persister
                .overwrite(&segment.time_index_path, &[])
                .await
                .is_err()
        {
            return Err(Error::CannotCreateSegmentTimeIndexFile(
                segment.time_index_path.clone(),
            ));
        }

        if !Path::new(&segment.index_path).exists()
            && self
                .persister
                .overwrite(&segment.index_path, &[])
                .await
                .is_err()
        {
            return Err(Error::CannotCreateSegmentIndexFile(
                segment.index_path.clone(),
            ));
        }

        info!("Saved segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id);

        Ok(())
    }

    async fn delete(&self, segment: &Segment) -> Result<(), Error> {
        info!(
            "Deleting segment with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            segment.start_offset, segment.partition_id, segment.stream_id, segment.topic_id,
        );
        self.persister.delete(&segment.log_path).await?;
        self.persister.delete(&segment.index_path).await?;
        self.persister.delete(&segment.time_index_path).await?;
        info!(
            "Deleted segment with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            segment.start_offset, segment.partition_id, segment.stream_id, segment.topic_id,
        );
        Ok(())
    }
}

#[async_trait]
impl SegmentStorage for FileSegmentStorage {
    async fn load_messages(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let mut messages = Vec::with_capacity(
            1 + (index_range.end.relative_offset - index_range.start.relative_offset) as usize,
        );
        load_messages_by_range(segment, index_range, |message: Message| {
            messages.push(Arc::new(message));
            Ok(())
        })
        .await?;
        trace!("Loaded {} messages from disk.", messages.len());
        Ok(messages)
    }

    async fn load_newest_messages_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<Arc<Message>>, Error> {
        let mut messages = Vec::new();
        let mut total_size_bytes = 0;
        load_messages_by_size(segment, size_bytes, |message: Message| {
            total_size_bytes += message.get_size_bytes() as u64;
            messages.push(Arc::new(message));
            Ok(())
        })
        .await?;
        trace!(
            "Loaded {} newest messages of total size {} bytes from disk.",
            messages.len(),
            total_size_bytes
        );
        Ok(messages)
    }

    async fn save_messages(
        &self,
        segment: &Segment,
        messages: &[Arc<Message>],
    ) -> Result<u32, Error> {
        let messages_size = messages
            .iter()
            .map(|message| message.get_size_bytes())
            .sum::<u32>();

        let mut bytes = Vec::with_capacity(messages_size as usize);
        for message in messages {
            message.extend(&mut bytes);
        }

        if let Err(err) = self
            .persister
            .append(&segment.log_path, &bytes)
            .await
            .with_context(|| format!("Failed to save messages to segment: {}", segment.log_path))
        {
            return Err(Error::CannotSaveMessagesToSegment(err));
        }

        Ok(messages_size)
    }

    async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, Error> {
        let mut message_ids = Vec::new();
        load_messages_by_range(segment, &IndexRange::max_range(), |message: Message| {
            message_ids.push(message.id);
            Ok(())
        })
        .await?;
        trace!("Loaded {} message IDs from disk.", message_ids.len());
        Ok(message_ids)
    }

    async fn load_checksums(&self, segment: &Segment) -> Result<(), Error> {
        load_messages_by_range(segment, &IndexRange::max_range(), |message: Message| {
            let calculated_checksum = checksum::calculate(&message.payload);
            trace!(
                "Loaded message for offset: {}, checksum: {}, expected: {}",
                message.offset,
                calculated_checksum,
                message.checksum
            );
            if calculated_checksum != message.checksum {
                return Err(Error::InvalidMessageChecksum(
                    calculated_checksum,
                    message.checksum,
                    message.offset,
                ));
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, Error> {
        trace!("Loading indexes from file...");
        let file = file::open(&segment.index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Index file is empty.");
            return Ok(EMPTY_INDEXES);
        }

        let indexes_count = file_size / 4;
        let mut indexes = Vec::with_capacity(indexes_count);
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        for offset in 0..indexes_count {
            match reader.read_u32_le().await {
                Ok(position) => {
                    indexes.push(Index {
                        relative_offset: offset as u32,
                        position,
                    });
                }
                Err(error) => {
                    error!(
                        "Cannot read position from index file for offset: {}. Error: {}",
                        offset, error
                    );
                    break;
                }
            }
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

    async fn load_index_range(
        &self,
        segment: &Segment,
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

        let mut file = file::open(&segment.index_path).await?;
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
        file.seek(SeekFrom::Start(start_seek_position as u64))
            .await?;
        let start_position = file.read_u32_le().await?;
        file.seek(SeekFrom::Start(end_seek_position as u64)).await?;
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

    async fn save_index(
        &self,
        segment: &Segment,
        mut current_position: u32,
        messages: &[Arc<Message>],
    ) -> Result<(), Error> {
        let mut bytes = Vec::with_capacity(messages.len() * 4);
        for message in messages {
            trace!("Persisting index for position: {}", current_position);
            bytes.put_u32_le(current_position);
            current_position += message.get_size_bytes();
        }

        if let Err(err) = self
            .persister
            .append(&segment.index_path, &bytes)
            .await
            .with_context(|| format!("Failed to save index to segment: {}", segment.index_path))
        {
            return Err(Error::CannotSaveIndexToSegment(err));
        }

        Ok(())
    }

    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, Error> {
        trace!("Loading time indexes from file...");
        let file = file::open(&segment.time_index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(EMPTY_TIME_INDEXES);
        }

        let indexes_count = file_size / 8;
        let mut indexes = Vec::with_capacity(indexes_count);
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        for offset in 0..indexes_count {
            match reader.read_u64_le().await {
                Ok(timestamp) => {
                    indexes.push(TimeIndex {
                        relative_offset: offset as u32,
                        timestamp,
                    });
                }
                Err(error) => {
                    error!(
                        "Cannot read timestamp from time index file for offset: {}. Error: {}",
                        offset, error
                    );
                    break;
                }
            }
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

    async fn load_last_time_index(&self, segment: &Segment) -> Result<Option<TimeIndex>, Error> {
        trace!("Loading last time index from file...");
        let mut file = file::open(&segment.time_index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(None);
        }

        let indexes_count = file_size / 8;
        let last_index_position = file_size - 8;
        file.seek(SeekFrom::Start(last_index_position as u64))
            .await?;
        let timestamp = file.read_u64_le().await?;
        let index = TimeIndex {
            relative_offset: indexes_count as u32 - 1,
            timestamp,
        };

        trace!("Loaded last time index from file: {:?}", index);
        Ok(Some(index))
    }

    async fn save_time_index(
        &self,
        segment: &Segment,
        messages: &[Arc<Message>],
    ) -> Result<(), Error> {
        let mut bytes = Vec::with_capacity(messages.len() * 8);
        for message in messages {
            bytes.put_u64_le(message.timestamp);
        }

        if let Err(err) = self
            .persister
            .append(&segment.time_index_path, &bytes)
            .await
            .with_context(|| {
                format!(
                    "Failed to save TimeIndex to segment: {}",
                    segment.time_index_path
                )
            })
        {
            return Err(Error::CannotSaveTimeIndexToSegment(err));
        }

        Ok(())
    }
}

async fn load_messages_by_range(
    segment: &Segment,
    index_range: &IndexRange,
    mut on_message: impl FnMut(Message) -> Result<(), Error>,
) -> Result<(), Error> {
    let file = file::open(&segment.log_path).await?;
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(());
    }

    if index_range.end.position == 0 {
        return Ok(());
    }

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    reader
        .seek(SeekFrom::Start(index_range.start.position as u64))
        .await?;

    let mut read_messages = 0;
    let messages_count =
        (1 + index_range.end.relative_offset - index_range.start.relative_offset) as usize;

    while read_messages < messages_count {
        let offset = reader.read_u64_le().await;
        if offset.is_err() {
            break;
        }

        let state = reader.read_u8().await;
        if state.is_err() {
            return Err(Error::CannotReadMessageState);
        }

        let state = MessageState::from_code(state.unwrap())?;
        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            return Err(Error::CannotReadMessageTimestamp);
        }

        let id = reader.read_u128_le().await;
        if id.is_err() {
            return Err(Error::CannotReadMessageId);
        }

        let checksum = reader.read_u32_le().await;
        if checksum.is_err() {
            return Err(Error::CannotReadMessageChecksum);
        }

        let headers_length = reader.read_u32_le().await;
        if headers_length.is_err() {
            return Err(Error::CannotReadHeadersLength);
        }

        let headers_length = headers_length.unwrap();
        let headers = match headers_length {
            0 => None,
            _ => {
                let mut headers_payload = vec![0; headers_length as usize];
                if reader.read_exact(&mut headers_payload).await.is_err() {
                    return Err(Error::CannotReadHeadersPayload);
                }

                let headers = HashMap::from_bytes(&headers_payload)?;
                Some(headers)
            }
        };

        let payload_length = reader.read_u32_le().await;
        if payload_length.is_err() {
            return Err(Error::CannotReadMessageLength);
        }

        let mut payload = vec![0; payload_length.unwrap() as usize];
        if reader.read_exact(&mut payload).await.is_err() {
            return Err(Error::CannotReadMessagePayload);
        }

        let offset = offset.unwrap();
        let timestamp = timestamp.unwrap();
        let id = id.unwrap();
        let checksum = checksum.unwrap();

        let message = Message::create(
            offset,
            state,
            timestamp,
            id,
            Bytes::from(payload),
            checksum,
            headers,
        );
        read_messages += 1;
        on_message(message)?;
    }
    Ok(())
}

async fn load_messages_by_size(
    segment: &Segment,
    size_bytes: u64,
    mut on_message: impl FnMut(Message) -> Result<(), Error>,
) -> Result<(), Error> {
    let file = file::open(&segment.log_path).await?;
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(());
    }
    let threshold = file_size.saturating_sub(size_bytes);

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    let mut accumulated_size: u64 = 0;

    loop {
        let offset = reader.read_u64_le().await;
        if offset.is_err() {
            break;
        }

        let state = reader.read_u8().await;
        if state.is_err() {
            return Err(Error::CannotReadMessageState);
        }

        let state = MessageState::from_code(state.unwrap())?;
        let timestamp = reader.read_u64_le().await;
        if timestamp.is_err() {
            return Err(Error::CannotReadMessageTimestamp);
        }

        let id = reader.read_u128_le().await;
        if id.is_err() {
            return Err(Error::CannotReadMessageId);
        }

        let checksum = reader.read_u32_le().await;
        if checksum.is_err() {
            return Err(Error::CannotReadMessageChecksum);
        }

        let headers_length = reader.read_u32_le().await;
        if headers_length.is_err() {
            return Err(Error::CannotReadHeadersLength);
        }

        let headers_length = headers_length.unwrap();
        let headers = match headers_length {
            0 => None,
            _ => {
                let mut headers_payload = vec![0; headers_length as usize];
                if reader.read_exact(&mut headers_payload).await.is_err() {
                    return Err(Error::CannotReadHeadersPayload);
                }

                let headers = HashMap::from_bytes(&headers_payload)?;
                Some(headers)
            }
        };

        let payload_length = reader.read_u32_le().await;
        if payload_length.is_err() {
            return Err(Error::CannotReadMessageLength);
        }

        let mut payload = vec![0; payload_length.unwrap() as usize];
        if reader.read_exact(&mut payload).await.is_err() {
            return Err(Error::CannotReadMessagePayload);
        }

        let offset = offset.unwrap();
        let timestamp = timestamp.unwrap();
        let id = id.unwrap();
        let checksum = checksum.unwrap();

        let message = Message::create(
            offset,
            state,
            timestamp,
            id,
            Bytes::from(payload),
            checksum,
            headers,
        );
        let message_size = message.get_size_bytes() as u64;

        if accumulated_size >= threshold {
            on_message(message)?;
        }

        accumulated_size += message_size;

        if accumulated_size >= file_size {
            break;
        }
    }

    Ok(())
}
