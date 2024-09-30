use crate::streaming::batching::iterator::IntoMessagesIterator;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::persistence::persister::Persister;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::sizeable::Sizeable;
use crate::streaming::storage::SegmentStorage;
use crate::streaming::utils::file;
use crate::streaming::utils::head_tail_buf::HeadTailBuffer;
use anyhow::Context;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::checksum;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tracing::{error, info, trace, warn};

const EMPTY_INDEXES: Vec<Index> = vec![];
const EMPTY_TIME_INDEXES: Vec<TimeIndex> = vec![];
pub(crate) const INDEX_SIZE: u32 = 8;
pub(crate) const TIME_INDEX_SIZE: u32 = 12;
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

#[async_trait]
impl SegmentStorage for FileSegmentStorage {
    async fn load(&self, segment: &mut Segment) -> Result<(), IggyError> {
        info!(
            "Loading segment from disk for start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {} ...",
            segment.start_offset, segment.partition_id, segment.topic_id, segment.stream_id
        );
        let log_file = file::open(&segment.log_path).await?;
        let file_size = log_file.metadata().await.unwrap().len() as u64;
        segment.size_bytes = file_size as _;
        segment.last_index_position = file_size as _;

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

        let messages_count = segment.get_messages_count();

        info!(
            "Loaded segment log file of size {} for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
            IggyByteSize::from(file_size), segment.start_offset, segment.current_offset, segment.partition_id, segment.topic_id, segment.stream_id
        );

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

    async fn load_message_batches(
        &self,
        segment: &Segment,
        index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let mut batches = Vec::new();
        load_batches_by_range(segment, index_range, |batch| {
            batches.push(batch);
            Ok(())
        })
        .await?;
        trace!("Loaded {} message batches from disk.", batches.len());
        Ok(batches)
    }

    async fn load_newest_batches_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let mut batches = Vec::new();
        let mut total_size_bytes = 0;
        load_messages_by_size(segment, size_bytes, |batch| {
            total_size_bytes += batch.get_size_bytes() as u64;
            batches.push(batch);
            Ok(())
        })
        .await?;
        let messages_count = batches.len();
        trace!(
            "Loaded {} newest messages batches of total size {} bytes from disk.",
            messages_count,
            total_size_bytes
        );
        Ok(batches)
    }

    async fn save_batches(
        &self,
        segment: &Segment,
        batch: RetainedMessageBatch,
    ) -> Result<u32, IggyError> {
        let batch_size = batch.get_size_bytes();
        let mut bytes = BytesMut::with_capacity(batch_size as usize);
        batch.extend(&mut bytes);

        if let Err(err) = self
            .persister
            .append(&segment.log_path, &bytes)
            .await
            .with_context(|| format!("Failed to save messages to segment: {}", segment.log_path))
        {
            return Err(IggyError::CannotSaveMessagesToSegment(err));
        }

        Ok(batch_size)
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
            let offset = reader.read_u32_le().await.inspect_err(|error| {
                error!(
                    "Cannot read offset from index file for index number: {}. Error: {}",
                    idx_num, &error
                )
            })?;
            let position = reader.read_u32_le().await.inspect_err(|error| {
                error!(
                    "Cannot read position from index file for offset: {}. Error: {}",
                    offset, &error
                )
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
        index_start_offset: u64,
        index_end_offset: u64,
    ) -> Result<Option<IndexRange>, IggyError> {
        trace!(
            "Loading index range for offsets: {} to {}, segment starts at: {}",
            index_start_offset,
            index_end_offset,
            segment.start_offset
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
        let relative_start_offset = (index_start_offset - segment.start_offset) as u32;
        let relative_end_offset = (index_end_offset - segment.start_offset) as u32;

        trace!(
            "Seeking to index range: {}...{}",
            relative_start_offset,
            relative_end_offset,
        );
        let mut idx_pred = HeadTailBuffer::new();
        let mut index_range = IndexRange::default();
        let mut read_bytes = 0;

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        loop {
            let relative_offset = reader.read_u32_le().await?;
            let position = reader.read_u32_le().await?;
            read_bytes += INDEX_SIZE;
            let idx = Index {
                relative_offset,
                position,
            };
            idx_pred.push(idx);

            if relative_offset >= relative_start_offset {
                index_range.start = idx_pred.tail().unwrap_or_default();
            }
            if relative_offset >= relative_end_offset {
                index_range.end = idx;
                break;
            }

            if read_bytes == file_length {
                break;
            }
        }

        trace!(
            "Loaded index range: {}...{}, position range: {}...{}",
            relative_start_offset,
            relative_end_offset,
            index_range.start.position,
            index_range.end.position
        );
        Ok(Some(index_range))
    }

    async fn save_index(&self, index_path: &str, index: Index) -> Result<(), IggyError> {
        let mut bytes = BytesMut::with_capacity(INDEX_SIZE as usize);
        bytes.put_u32_le(index.relative_offset);
        bytes.put_u32_le(index.position);
        if let Err(err) = self
            .persister
            .append(index_path, &bytes)
            .await
            .with_context(|| format!("Failed to save index to segment: {}", index_path))
        {
            return Err(IggyError::CannotSaveIndexToSegment(err));
        }

        Ok(())
    }

    async fn try_load_time_index_for_timestamp(
        &self,
        segment: &Segment,
        timestamp: u64,
    ) -> Result<Option<TimeIndex>, IggyError> {
        trace!("Loading time indexes from file...");
        let file = file::open(&segment.time_index_path).await?;
        let file_size = file.metadata().await?.len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(Some(TimeIndex::default()));
        }

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut read_bytes = 0;
        let mut idx_pred = HeadTailBuffer::new();
        loop {
            let offset = reader.read_u32_le().await?;
            let time = reader.read_u64_le().await?;
            let idx = TimeIndex {
                relative_offset: offset,
                timestamp: time,
            };
            idx_pred.push(idx);
            if time >= timestamp {
                return Ok(idx_pred.tail());
            }
            read_bytes += TIME_INDEX_SIZE as usize;
            if read_bytes == file_size {
                return Ok(None);
            }
        }
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
            let offset = reader.read_u32_le().await.inspect_err(|error| {
                error!(
                    "Cannot read offset from index file for offset: {}. Error: {}",
                    idx_num, &error
                )
            })?;
            let timestamp = reader.read_u64().await.inspect_err(|error| {
                error!(
                    "Cannot read timestamp from index file for offset: {}. Error: {}",
                    offset, &error
                )
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

        let last_index_position = file_size - TIME_INDEX_SIZE as usize;
        file.seek(SeekFrom::Start(last_index_position as u64))
            .await?;
        let index_offset = file.read_u32_le().await?;
        let timestamp = file.read_u64_le().await?;
        let index = TimeIndex {
            relative_offset: index_offset,
            timestamp,
        };

        trace!("Loaded last time index from file: {:?}", index);
        Ok(Some(index))
    }

    async fn save_time_index(&self, index_path: &str, index: TimeIndex) -> Result<(), IggyError> {
        let mut bytes = BytesMut::with_capacity(TIME_INDEX_SIZE as usize);
        bytes.put_u32_le(index.relative_offset);
        bytes.put_u64_le(index.timestamp);
        if let Err(err) = self
            .persister
            .append(index_path, &bytes)
            .await
            .with_context(|| format!("Failed to save TimeIndex to segment: {}", index_path))
        {
            return Err(IggyError::CannotSaveTimeIndexToSegment(err));
        }

        Ok(())
    }
}

async fn load_batches_by_range(
    segment: &Segment,
    index_range: &IndexRange,
    mut on_batch: impl FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let file = file::open(&segment.log_path).await?;
    let file_size = file.metadata().await?.len();
    if file_size == 0 {
        return Ok(());
    }

    trace!(
        "Loading message batches by index range: {} [{}] - {} [{}], file size: {file_size}",
        index_range.start.position,
        index_range.start.relative_offset,
        index_range.end.position,
        index_range.end.relative_offset
    );

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    reader
        .seek(SeekFrom::Start(index_range.start.position as u64))
        .await?;

    let mut read_bytes = index_range.start.position as u64;
    let mut last_batch_to_read = false;
    while !last_batch_to_read {
        let Ok(batch_base_offset) = reader.read_u64_le().await else {
            break;
        };
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
        let mut payload = BytesMut::with_capacity(payload_len);
        payload.put_bytes(0, payload_len);
        if let Err(error) = reader.read_exact(&mut payload).await {
            warn!(
                "Cannot read batch payload for batch with base offset: {batch_base_offset}, last offset delta: {last_offset_delta}, max timestamp: {max_timestamp}, batch length: {batch_length} and payload length: {payload_len}.\nProbably OS hasn't flushed the data yet, try setting `enforce_fsync = true` for partition configuration if this issue occurs again.\n{error}",
            );
            break;
        }

        read_bytes += 8 + 4 + 4 + 8 + payload_len as u64;
        last_batch_to_read = read_bytes >= file_size || last_offset == index_last_offset;

        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            batch_length,
            payload.freeze(),
        );
        on_batch(batch)?;
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

    let threshold = file_size.saturating_sub(size_bytes);
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
        let mut payload = BytesMut::with_capacity(payload_len);
        payload.put_bytes(0, payload_len);
        reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| IggyError::CannotReadBatchPayload)?;

        let batch = RetainedMessageBatch::new(
            batch_base_offset,
            last_offset_delta,
            max_timestamp,
            batch_length,
            payload.freeze(),
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
