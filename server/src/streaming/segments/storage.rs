use crate::streaming::batching::iterator::IntoBatchIterator;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::sizeable::Sizeable;
use crate::streaming::storage::{SegmentStorage, Storage};
use crate::streaming::utils::file;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy::error::IggyError;
use iggy::utils::checksum;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tracing::error;
use tracing::log::{info, trace, warn};

const EMPTY_INDEXES: Vec<Index> = vec![];
const EMPTY_TIME_INDEXES: Vec<TimeIndex> = vec![];
const INDEX_SIZE: u32 = 8;
const TIME_INDEX_SIZE: u32 = 12;
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

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
    async fn load(&self, segment: &mut Segment) -> Result<(), IggyError> {
        info!(
            "Loading segment from disk for start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {} ...",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id
        );
        let log_file = file::open(&segment.log_path).await?;
        let file_size = log_file.metadata().await.unwrap().len() as u64;
        segment.size_bytes = file_size as u32;
        let messages_count = segment.get_messages_count();

        info!(
            "Segment log file of size {} for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
            segment.size_bytes, segment.start_offset, segment.current_offset, segment.partition_id, segment.topic_id, segment.stream_id
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

        segment
            .size_of_parent_stream
            .fetch_add(file_size, Ordering::SeqCst);
        segment
            .size_of_parent_topic
            .fetch_add(file_size, Ordering::SeqCst);
        segment
            .size_of_parent_partition
            .fetch_add(file_size, Ordering::SeqCst);
        segment
            .messages_count_of_parent_stream
            .fetch_add(messages_count, Ordering::SeqCst);
        segment
            .messages_count_of_parent_topic
            .fetch_add(messages_count, Ordering::SeqCst);
        segment
            .messages_count_of_parent_partition
            .fetch_add(messages_count, Ordering::SeqCst);

        Ok(())
    }

    async fn save(&self, segment: &Segment) -> Result<(), IggyError> {
        info!("Saving segment with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id);
        if !Path::new(&segment.log_path).exists()
            && self
                .persister
                .overwrite(&segment.log_path, &[])
                .await
                .is_err()
        {
            return Err(IggyError::CannotCreateSegmentLogFile(
                segment.log_path.clone(),
            ));
        }

        if !Path::new(&segment.time_index_path).exists()
            && self
                .persister
                .overwrite(&segment.time_index_path, &[])
                .await
                .is_err()
        {
            return Err(IggyError::CannotCreateSegmentTimeIndexFile(
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
            return Err(IggyError::CannotCreateSegmentIndexFile(
                segment.index_path.clone(),
            ));
        }

        info!("Saved segment log file with start offset: {} for partition with ID: {} for topic with ID: {} and stream with ID: {}",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id);

        Ok(())
    }

    async fn delete(&self, segment: &Segment) -> Result<(), IggyError> {
        let segment_size = segment.size_bytes;
        let segment_count_of_messages = segment.get_messages_count();
        info!(
            "Deleting segment of size {segment_size} with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}...",
            segment.start_offset, segment.partition_id, segment.stream_id, segment.topic_id,
        );
        self.persister.delete(&segment.log_path).await?;
        self.persister.delete(&segment.index_path).await?;
        self.persister.delete(&segment.time_index_path).await?;
        segment
            .size_of_parent_stream
            .fetch_sub(segment.size_bytes as u64, Ordering::SeqCst);
        segment
            .size_of_parent_topic
            .fetch_sub(segment.size_bytes as u64, Ordering::SeqCst);
        segment
            .size_of_parent_partition
            .fetch_sub(segment.size_bytes as u64, Ordering::SeqCst);
        segment
            .messages_count_of_parent_stream
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        segment
            .messages_count_of_parent_topic
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        segment
            .messages_count_of_parent_partition
            .fetch_sub(segment_count_of_messages, Ordering::SeqCst);
        info!(
            "Deleted segment of size {segment_size} with start offset: {} for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            segment.start_offset, segment.partition_id, segment.stream_id, segment.topic_id,
        );
        Ok(())
    }
}

#[async_trait]
impl SegmentStorage for FileSegmentStorage {
    async fn load_message_batches(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<RetainedMessageBatch>>, IggyError> {
        let mut messages = Vec::with_capacity(
            1 + (index_range.end.relative_offset - index_range.start.relative_offset) as usize,
        );
        load_batches_by_range(segment, index_range, |batch| {
            messages.push(batch);
            Ok(())
        })
        .await?;
        trace!("Loaded {} messages from disk.", messages.len());
        Ok(messages)
    }

    async fn load_newest_message_batches_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<Arc<RetainedMessageBatch>>, IggyError> {
        todo!();
        /*
        let mut messages = Vec::new();
        let mut total_size_bytes = 0;
        load_messages_by_size(segment, size_bytes, |message: RetainedMessage| {
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
        */
    }

    async fn save_messages(
        &self,
        segment: &Segment,
        messages: &[Arc<RetainedMessageBatch>],
    ) -> Result<u32, IggyError> {
        let messages_size = messages
            .iter()
            .map(|message| message.get_size_bytes())
            .sum();

        let mut bytes = BytesMut::with_capacity(messages_size as usize);
        for message in messages {
            message.extend(&mut bytes);
        }

        if let Err(err) = self
            .persister
            .append(&segment.log_path, &bytes)
            .await
            .with_context(|| format!("Failed to save messages to segment: {}", segment.log_path))
        {
            return Err(IggyError::CannotSaveMessagesToSegment(err));
        }

        Ok(messages_size)
    }

    async fn load_message_ids(&self, segment: &Segment) -> Result<Vec<u128>, IggyError> {
        let mut message_ids = Vec::new();
        load_batches_by_range(segment, &IndexRange::max_range(), |batch| {
            message_ids.extend(
                batch
                    .into_messages_iter()
                    .map(|msg: RetainedMessage| msg.id),
            );
            Ok(())
        })
        .await?;
        trace!("Loaded {} message IDs from disk.", message_ids.len());
        Ok(message_ids)
    }

    async fn load_checksums(&self, segment: &Segment) -> Result<(), IggyError> {
        load_batches_by_range(segment, &IndexRange::max_range(), |batch| {
            for message in batch.into_messages_iter() {
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
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, IggyError> {
        trace!("Loading indexes from file...");
        let file = file::open(&segment.index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Index file is empty.");
            return Ok(EMPTY_INDEXES);
        }

        let indexes_count = file_size / INDEX_SIZE as usize;
        let mut indexes = Vec::with_capacity(indexes_count);
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        for idx_num in 0..indexes_count {
            let offset = reader.read_u32_le().await.map_err(|error| {
                error!(
                    "Cannot read offset from index file for index number: {}. Error: {}",
                    idx_num, &error
                );
                error
            })?;
            let position = reader.read_u32_le().await.map_err(|error| {
                error!(
                    "Cannot read position from index file for offset: {}. Error: {}",
                    offset, &error
                );
                error
            })?;
            indexes.push(Index {
                relative_offset: offset,
                position,
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

    async fn load_index_range(
        &self,
        segment: &Segment,
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

        let file = file::open(&segment.index_path).await?;
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
        let start_seek_position = file_length;

        trace!(
            "Seeking to index range: {}...{}",
            relative_start_offset,
            relative_end_offset,
        );

        let mut index_range = IndexRange::default();
        let mut start_position = 0;
        let mut end_position = 0;
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        reader
            .seek(SeekFrom::End(start_seek_position as i64))
            .await?;
        while reader.buffer().has_remaining() {
            let position = reader.read_u32_le().await?;
            let relative_offset = reader.read_u32_le().await?;
            if relative_offset >= relative_end_offset {
                end_position = position;
                index_range.end = Index {
                    relative_offset,
                    position,
                }
            }

            if relative_offset <= relative_start_offset {
                start_position = position;
                index_range.end = Index {
                    relative_offset,
                    position,
                };
                break;
            }
        }

        trace!(
            "Loaded index range: {}...{}, position range: {}...{}",
            relative_start_offset,
            relative_end_offset,
            start_position,
            end_position
        );
        Ok(Some(index_range))
    }

    async fn save_index(&self, segment: &Segment) -> Result<(), IggyError> {
        if let Err(err) = self
            .persister
            .append(&segment.index_path, &segment.unsaved_indexes)
            .await
            .with_context(|| format!("Failed to save index to segment: {}", segment.index_path))
        {
            return Err(IggyError::CannotSaveIndexToSegment(err));
        }

        Ok(())
    }

    async fn load_all_time_indexes(&self, segment: &Segment) -> Result<Vec<TimeIndex>, IggyError> {
        trace!("Loading time indexes from file...");
        let file = file::open(&segment.time_index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(EMPTY_TIME_INDEXES);
        }

        let indexes_count = file_size / TIME_INDEX_SIZE as usize;
        let mut indexes = Vec::with_capacity(indexes_count);
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        for idx_num in 0..indexes_count {
            let offset = reader.read_u32_le().await.map_err(|error| {
                error!(
                    "Cannot read offset from index file for offset: {}. Error: {}",
                    idx_num, &error
                );
                error
            })?;
            let timestamp = reader.read_u64().await.map_err(|error| {
                error!(
                    "Cannot read timestamp from index file for offset: {}. Error: {}",
                    offset, &error
                );
                error
            })?;
            indexes.push(TimeIndex {
                relative_offset: offset,
                timestamp,
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

    async fn load_last_time_index(
        &self,
        segment: &Segment,
    ) -> Result<Option<TimeIndex>, IggyError> {
        trace!("Loading last time index from file...");
        let mut file = file::open(&segment.time_index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(None);
        }

        let indexes_count = file_size / TIME_INDEX_SIZE as usize;
        let last_index_position = file_size - TIME_INDEX_SIZE as usize;
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

    async fn save_time_index(&self, segment: &Segment) -> Result<(), IggyError> {
        if let Err(err) = self
            .persister
            .append(&segment.time_index_path, &segment.unsaved_timestamps)
            .await
            .with_context(|| {
                format!(
                    "Failed to save TimeIndex to segment: {}",
                    segment.time_index_path
                )
            })
        {
            return Err(IggyError::CannotSaveTimeIndexToSegment(err));
        }

        Ok(())
    }
}

async fn load_batches_by_range(
    segment: &Segment,
    index_range: &IndexRange,
    mut on_batch: impl FnMut(Arc<RetainedMessageBatch>) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let file = file::open(&segment.log_path).await?;
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(());
    }

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    reader
        .seek(SeekFrom::Start(index_range.start.position as u64))
        .await?;

    let mut read_bytes: u64 = 0;
    let mut last_batch_to_read = false;
    while !last_batch_to_read {
        let batch_base_offset = reader
            .read_u64_le()
            .await
            .map_err(|_| IggyError::CannotReadBatchBaseOffset)?;
        let batch_length = reader
            .read_u32_le()
            .await
            .map_err(|_| IggyError::CannotReadBatchLength)?;
        let last_offset_delta = reader
            .read_u32_le()
            .await
            .map_err(|_| IggyError::CannotReadLastOffsetDelta)?;
        let max_timestamp = reader
            .read_u64_le()
            .await
            .map_err(|_| IggyError::CannotReadMaxTimestamp)?;

        let last_offset = batch_base_offset + (last_offset_delta as u64);
        let index_last_offset = index_range.end.relative_offset as u64 + segment.start_offset;

        let payload_len = batch_length as usize;
        let mut payload = vec![0; payload_len];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| IggyError::CannotReadBatchPayload)?;

        read_bytes += 8 + 4 + 4 + 8 + payload_len as u64;
        last_batch_to_read = read_bytes == file_size || last_offset == index_last_offset;

        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            batch_length,
            Bytes::from(payload),
        );
        on_batch(Arc::new(batch))?;
    }
    Ok(())
}

async fn load_messages_by_size(
    segment: &Segment,
    size_bytes: u64,
    mut on_batch: impl FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let file = file::open(&segment.log_path).await?;
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(());
    }

    let threshold = file_size.saturating_sub(size_bytes as u64);
    let mut accumulated_size: u64 = 0;

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    loop {
        let batch_base_offset = reader
            .read_u64_le()
            .await
            .map_err(|_| IggyError::CannotReadBatchBaseOffset)?;
        let batch_length = reader
            .read_u32_le()
            .await
            .map_err(|_| IggyError::CannotReadBatchLength)?;
        let last_offset_delta = reader
            .read_u32_le()
            .await
            .map_err(|_| IggyError::CannotReadLastOffsetDelta)?;
        let max_timestamp = reader
            .read_u64_le()
            .await
            .map_err(|_| IggyError::CannotReadMaxTimestamp)?;

        let payload_len = batch_length as usize;
        let mut payload = vec![0; payload_len];
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| IggyError::CannotReadBatchPayload)?;

        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            batch_length,
            Bytes::from(payload),
        );
        let message_size = batch.get_size_bytes() as u64;
        if accumulated_size >= threshold {
            on_batch(batch)?;
        }

        accumulated_size += message_size;
        if accumulated_size >= file_size {
            break;
        }
    }

    Ok(())
}
