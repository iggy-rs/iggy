use super::persistence::*;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::{Message, MessageState};
use iggy::utils::checksum;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tracing::log::{trace, warn};
use tracing::{error, info};

const EMPTY_INDEXES: Vec<Index> = vec![];
const EMPTY_TIME_INDEXES: Vec<TimeIndex> = vec![];
const INDEX_SIZE: u32 = 4;
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[derive(Debug)]
pub enum SegmentStorageVariant {
    WithSync(StoragePersister<WithSync>),
    WithoutSync(StoragePersister<WithoutSync>),
}

impl SegmentStorageVariant {
    pub fn new(fsync: bool, path: String) -> Self {
        if !fsync {
            let persister_without_sync = WithoutSync::new(path.clone());
            SegmentStorageVariant::WithoutSync(StoragePersister::new(persister_without_sync))
        } else {
            SegmentStorageVariant::WithSync(StoragePersister::new(WithSync {
                path: path.to_string(),
            }))
        }
    }
}

#[async_trait]
impl SegmentPersister for SegmentStorageVariant {
    async fn append(
        &self,
        messages: Vec<Arc<Message>>,
        current_offset: u64,
    ) -> Result<(), IggyError> {
        match self {
            SegmentStorageVariant::WithSync(persister) => {
                persister.append(messages, current_offset).await
            }
            SegmentStorageVariant::WithoutSync(persister) => {
                persister.append(messages, current_offset).await
            }
        }
    }

    async fn create(&self, path: &str) -> Result<(), IggyError> {
        match self {
            SegmentStorageVariant::WithSync(persister) => persister.create(path).await,
            SegmentStorageVariant::WithoutSync(persister) => persister.create(path).await,
        }
    }

    async fn delete(&self) -> Result<(), IggyError> {
        match self {
            SegmentStorageVariant::WithSync(persister) => persister.delete().await,
            SegmentStorageVariant::WithoutSync(persister) => persister.delete().await,
        }
    }
}

unsafe impl Send for SegmentStorageVariant {}
unsafe impl Sync for SegmentStorageVariant {}

impl Segment {
    pub async fn load(&mut self) -> Result<(), IggyError> {
        let log_file = file::open(&self.log_path).await?;
        let file_size = log_file.metadata().await.unwrap().len() as u64;
        self.size_bytes = file_size as u32;
        let messages_count = self.get_messages_count();

        info!(
            "Loaded segment log file of size {} for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
            self.size_bytes, self.start_offset, self.current_offset, self.partition_id, self.topic_id, self.stream_id
        );

        if self.config.segment.cache_indexes {
            self.indexes = Some(self.load_all_indexes().await?);
            info!(
                "Loaded {} indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.indexes.as_ref().unwrap().len(),
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
        }

        if self.config.segment.cache_time_indexes {
            let time_indexes = self.load_all_time_indexes().await?;
            if !time_indexes.is_empty() {
                let last_index = time_indexes.last().unwrap();
                self.current_offset = self.start_offset + last_index.relative_offset as u64;
                self.time_indexes = Some(time_indexes);
            }

            info!(
                "Loaded {} time indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.time_indexes.as_ref().unwrap().len(),
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
        } else {
            let last_time_index = self.load_last_time_index().await?;
            if let Some(last_index) = last_time_index {
                self.current_offset = self.start_offset + last_index.relative_offset as u64;
                info!(
                "Loaded last time index for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                self.start_offset,
                self.partition_id,
                self.topic_id,
                self.stream_id
            );
            }
        }

        if self.is_full().await {
            self.is_closed = true;
        }

        self.size_of_parent_stream
            .fetch_add(file_size, Ordering::SeqCst);
        self.size_of_parent_topic
            .fetch_add(file_size, Ordering::SeqCst);
        self.size_of_parent_partition
            .fetch_add(file_size, Ordering::SeqCst);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count, Ordering::SeqCst);

        Ok(())
    }

    pub async fn create(&self) -> Result<(), IggyError> {
        self.storage.create(&self.path).await?;

        info!("Created segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
        self.start_offset, self.partition_id, self.topic_id, self.stream_id);

        Ok(())
    }

    pub async fn delete(&self) -> Result<(), IggyError> {
        let segment_size = self.size_bytes;
        let segment_count_of_messages = self.get_messages_count();

        self.storage.delete().await?;

        self.size_of_parent_stream
            .fetch_sub(self.size_bytes as u64, Ordering::SeqCst);
        self.size_of_parent_topic
            .fetch_sub(self.size_bytes as u64, Ordering::SeqCst);
        self.size_of_parent_partition
            .fetch_sub(self.size_bytes as u64, Ordering::SeqCst);
        self.messages_count_of_parent_stream
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        info!(
            "Deleted segment of size {segment_size} with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            self.start_offset, self.partition_id, self.stream_id, self.topic_id,
        );
        Ok(())
    }

    pub async fn load_messages(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<Message>>, IggyError> {
        let mut messages = Vec::with_capacity(
            1 + (index_range.end.relative_offset - index_range.start.relative_offset) as usize,
        );
        self.load_messages_by_range(index_range, |message: Message| {
            messages.push(Arc::new(message));
            Ok(())
        })
        .await?;
        trace!("Loaded {} messages from disk.", messages.len());
        Ok(messages)
    }

    pub async fn load_newest_messages_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<Arc<Message>>, IggyError> {
        let mut messages = Vec::new();
        let mut total_size_bytes = 0;
        self.load_messages_by_size(size_bytes, |message: Message| {
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

    pub async fn save_messages(&self, messages: Vec<Arc<Message>>) -> Result<u32, IggyError> {
        let messages_size = 10450000;

        if let Err(err) = self
            .append(messages)
            .await
            .with_context(|| format!("Failed to save messages to segment: {}", self.log_path))
        {
            return Err(IggyError::CannotSaveMessagesToSegment(err));
        }

        Ok(messages_size)
    }

    pub async fn load_message_ids(&self) -> Result<Vec<u128>, IggyError> {
        let mut message_ids = Vec::new();
        self.load_messages_by_range(&IndexRange::max_range(), |message: Message| {
            message_ids.push(message.id);
            Ok(())
        })
        .await?;
        trace!("Loaded {} message IDs from disk.", message_ids.len());
        Ok(message_ids)
    }

    pub async fn load_checksums(&self) -> Result<(), IggyError> {
        self.load_messages_by_range(&IndexRange::max_range(), |message: Message| {
            let calculated_checksum = checksum::calculate(&message.payload);
            trace!(
                "Loaded message for offset: {}, checksum: {}, expected: {}",
                message.offset,
                calculated_checksum,
                message.checksum
            );
            if calculated_checksum != message.checksum {
                return Err(IggyError::InvalidMessageChecksum(
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

    pub async fn load_all_indexes(&self) -> Result<Vec<Index>, IggyError> {
        trace!("Loading indexes from file...");
        let file = file::open(&self.index_path).await?;
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

    pub async fn load_index_range(
        &self,
        segment_start_offset: u64,
        mut index_start_offset: u64,
        index_end_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError> {
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

        let mut file = file::open(&self.index_path).await?;
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

    // pub async fn save_index(
    //     &self,
    //     mut current_position: u32,
    //     messages: &[Arc<Message>],
    // ) -> Result<(), IggyError> {
    //     let mut bytes = Vec::with_capacity(messages.len() * 4);
    //     for message in messages {
    //         trace!("Persisting index for position: {}", current_position);
    //         bytes.put_u32_le(current_position);
    //         current_position += message.get_size_bytes();
    //     }

    //     if let Err(err) = self
    //         .append(&bytes)
    //         .await
    //         .with_context(|| format!("Failed to save index to segment: {}", self.index_path))
    //     {
    //         return Err(IggyError::CannotSaveIndexToSegment(err));
    //     }

    //     Ok(())
    // }

    pub async fn load_all_time_indexes(&self) -> Result<Vec<TimeIndex>, IggyError> {
        trace!("Loading time indexes from file...");
        let file = file::open(&self.index_path).await?;
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

    async fn load_last_time_index(&self) -> Result<Option<TimeIndex>, IggyError> {
        trace!("Loading last time index from file...");
        let mut file = file::open(&self.index_path).await?;
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

    // pub async fn save_time_index(&self, messages: &[Arc<Message>]) -> Result<(), IggyError> {
    //     let mut bytes = Vec::with_capacity(messages.len() * 8);
    //     for message in messages {
    //         bytes.put_u64_le(message.timestamp);
    //     }

    //     if let Err(err) = self.append(&bytes).await.with_context(|| {
    //         format!(
    //             "Failed to save TimeIndex to segment: {}",
    //             self.time_index_path
    //         )
    //     }) {
    //         return Err(IggyError::CannotSaveTimeIndexToSegment(err));
    //     }

    //     Ok(())
    // }

    async fn load_messages_by_range(
        &self,
        index_range: &IndexRange,
        mut on_message: impl FnMut(Message) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        let file = file::open(&self.log_path).await?;
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
                return Err(IggyError::CannotReadMessageState);
            }

            let state = MessageState::from_code(state.unwrap())?;
            let timestamp = reader.read_u64_le().await;
            if timestamp.is_err() {
                return Err(IggyError::CannotReadMessageTimestamp);
            }

            let id = reader.read_u128_le().await;
            if id.is_err() {
                return Err(IggyError::CannotReadMessageId);
            }

            let checksum = reader.read_u32_le().await;
            if checksum.is_err() {
                return Err(IggyError::CannotReadMessageChecksum);
            }

            let headers_length = reader.read_u32_le().await;
            if headers_length.is_err() {
                return Err(IggyError::CannotReadHeadersLength);
            }

            let headers_length = headers_length.unwrap();
            let headers = match headers_length {
                0 => None,
                _ => {
                    let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                    headers_payload.put_bytes(0, headers_length as usize);
                    if reader.read_exact(&mut headers_payload).await.is_err() {
                        return Err(IggyError::CannotReadHeadersPayload);
                    }

                    let headers = HashMap::from_bytes(headers_payload.freeze())?;
                    Some(headers)
                }
            };

            let payload_length = reader.read_u32_le().await?;

            let mut payload = BytesMut::with_capacity(payload_length as usize);
            payload.put_bytes(0, payload_length as usize);
            if reader.read_exact(&mut payload).await.is_err() {
                return Err(IggyError::CannotReadMessagePayload);
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
                payload.freeze(),
                checksum,
                headers,
            );
            read_messages += 1;
            on_message(message)?;
        }
        Ok(())
    }

    async fn load_messages_by_size(
        &self,
        size_bytes: u64,
        mut on_message: impl FnMut(Message) -> Result<(), IggyError>,
    ) -> Result<(), IggyError> {
        let file = file::open(&self.log_path).await?;
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
                return Err(IggyError::CannotReadMessageState);
            }

            let state = MessageState::from_code(state.unwrap())?;
            let timestamp = reader.read_u64_le().await;
            if timestamp.is_err() {
                return Err(IggyError::CannotReadMessageTimestamp);
            }

            let id = reader.read_u128_le().await;
            if id.is_err() {
                return Err(IggyError::CannotReadMessageId);
            }

            let checksum = reader.read_u32_le().await;
            if checksum.is_err() {
                return Err(IggyError::CannotReadMessageChecksum);
            }

            let headers_length = reader.read_u32_le().await;
            if headers_length.is_err() {
                return Err(IggyError::CannotReadHeadersLength);
            }

            let headers_length = headers_length.unwrap();
            let headers = match headers_length {
                0 => None,
                _ => {
                    let mut headers_payload = BytesMut::with_capacity(headers_length as usize);
                    if reader.read_exact(&mut headers_payload).await.is_err() {
                        return Err(IggyError::CannotReadHeadersPayload);
                    }

                    let headers = HashMap::from_bytes(headers_payload.freeze())?;
                    Some(headers)
                }
            };

            let payload_length = reader.read_u32_le().await;
            if payload_length.is_err() {
                return Err(IggyError::CannotReadMessageLength);
            }

            let mut payload = vec![0; payload_length.unwrap() as usize];
            if reader.read_exact(&mut payload).await.is_err() {
                return Err(IggyError::CannotReadMessagePayload);
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
}
