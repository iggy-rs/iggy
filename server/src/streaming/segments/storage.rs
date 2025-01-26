use crate::streaming::batching::batch_accumulator::BatchAccumulator;
use crate::streaming::batching::iterator::IntoMessagesIterator;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::persistence::persister::PersisterKind;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::COMPONENT;
use crate::streaming::storage::SegmentStorage;
use crate::streaming::utils::file;
use crate::streaming::utils::head_tail_buf::HeadTailBuffer;
use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::error::IggyError;
use iggy::models::batch::IGGY_BATCH_OVERHEAD;
use iggy::models::messages::ArchivedIggyMessage;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::checksum;
use iggy::utils::sizeable::Sizeable;
use rkyv::rend::unaligned::u32_ule;
use rkyv::util::AlignedVec;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tracing::{error, info, trace, warn};

const EMPTY_INDEXES: Vec<Index> = vec![];
pub const INDEX_SIZE: u32 = 16; // offset: 4 bytes, position: 4 bytes, timestamp: 8 bytes
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[derive(Debug)]
pub struct FileSegmentStorage {
    persister: Arc<PersisterKind>,
}
impl FileSegmentStorage {
    pub async fn persist_batches(path: &str, batch_accumulator: BatchAccumulator) -> Result<(), IggyError> {
        let mut file = file::append(path)
            .await.unwrap();
        let mut header = [0u8; IGGY_BATCH_OVERHEAD as usize];

        //TODO: create our own header writer, once we drop dependency on bytes crate.
        let mut header_writer = header.writer();
        let first_batch = batch_accumulator.batches.first().unwrap();
        let base_offset = first_batch.base_offset;
        let base_timestamp = first_batch.base_timestamp;
        first_batch.write_header(&mut header_writer);
        file.write_all(&header).await.unwrap();

        //TODO: Use write_vectored, instead of looping like this.
        for (iter, mut batch) in batch_accumulator.batches.into_iter().enumerate() {
            if iter > 0 {
                let base_offset_diff = batch.base_offset - base_offset;
                let base_timestamp_diff = batch.base_timestamp - base_timestamp;
                let base_offset_diff = base_offset_diff as u32;
                let base_timestamp_diff = base_timestamp_diff as u32;

                let mut position = 0;
                let batch_payload = &mut batch.messages;
                while position < batch_payload.len() {
                    let length = u64::from_le_bytes(
                        batch_payload[position..position + 8].try_into().unwrap(),
                    );
                    let length = length as usize;
                    position += 8;
                    let message = unsafe {
                        rkyv::access_unchecked_mut::<ArchivedIggyMessage>(
                            &mut batch_payload[position..length + position],
                        )
                    };
                    position += length;
                    let message = unsafe { message.unseal_unchecked() };
                    let offset_delta = message.offset_delta;
                    let timestamp_delta = message.timestamp_delta;

                    message.offset_delta = (offset_delta + base_offset_diff).into();
                    message.timestamp_delta = (timestamp_delta + base_timestamp_diff).into();
                }
            }
            batch.write_messages(&mut file).await;
        }

        Ok(())
    }
}

impl FileSegmentStorage {
    pub fn new(persister: Arc<PersisterKind>) -> Self {
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
        let log_file = file::open(&segment.log_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to open log_file, path: {}",
                    segment.log_path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let file_size = log_file.metadata().await.unwrap().len() as u64;
        segment.size_bytes = IggyByteSize::from(file_size);
        segment.last_index_position = file_size as _;

        if segment.config.segment.cache_indexes {
            segment.indexes = Some(
                segment
                    .storage
                    .segment
                    .load_all_indexes(segment)
                    .await
                    .with_error_context(|_| {
                        format!("{COMPONENT} - failed to load indexes, segment: {}", segment)
                    })?,
            );
            let last_index_offset = if segment.indexes.as_ref().unwrap().is_empty() {
                0_u64
            } else {
                segment.indexes.as_ref().unwrap().last().unwrap().offset as u64
            };

            segment.current_offset = segment.start_offset + last_index_offset;
            info!(
                "Loaded {} indexes for segment with start offset: {} and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
                segment.indexes.as_ref().unwrap().len(),
                segment.start_offset,
                segment.partition_id,
                segment.topic_id,
                segment.stream_id
            );
        }

        if segment.is_full().await {
            segment.is_closed = true;
        }

        let messages_count = segment.get_messages_count();

        info!(
            "Loaded segment with log file of size {} ({messages_count} messages) for start offset {}, current offset: {}, and partition with ID: {} for topic with ID: {} and stream with ID: {}.",
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
        self.persister
            .delete(&segment.log_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to delete log file, path: {}",
                    segment.log_path
                )
            })?;
        self.persister
            .delete(&segment.index_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to delete index file, path: {}",
                    segment.index_path
                )
            })?;
        let segment_size_bytes = segment.size_bytes.as_bytes_u64();
        segment
            .size_of_parent_stream
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
        segment
            .size_of_parent_topic
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
        segment
            .size_of_parent_partition
            .fetch_sub(segment_size_bytes, Ordering::SeqCst);
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
        .await
        .with_error_context(|_| {
            format!(
                "{COMPONENT} - failed to load batches by range, segment: {}, index range: {:?}",
                segment, index_range
            )
        })?;
        trace!("Loaded {} message batches from disk.", batches.len());
        Ok(batches)
    }

    async fn load_newest_batches_by_size(
        &self,
        segment: &Segment,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let mut batches = Vec::new();
        let mut total_size_bytes = IggyByteSize::default();
        load_messages_by_size(segment, size_bytes, |batch| {
            total_size_bytes += batch.get_size_bytes();
            batches.push(batch);
            Ok(())
        })
        .await
        .with_error_context(|_| {
            format!(
                "{COMPONENT} - failed to load messages by size, segment: {}, size bytes: {}",
                segment, size_bytes
            )
        })?;
        let messages_count = batches.len();
        trace!(
            "Loaded {} newest messages batches of total size {} from disk.",
            messages_count,
            total_size_bytes.as_human_string(),
        );
        Ok(batches)
    }

    async fn save_batches_raw(
        &self,
        segment: &Segment,
        batch_accumulator: BatchAccumulator,
        confirmation: Confirmation,
    ) -> Result<IggyByteSize, IggyError> {
        let save_result = match confirmation {
            Confirmation::Wait => FileSegmentStorage::persist_batches(&segment.log_path, batch_accumulator).await,
            Confirmation::NoWait => segment.persister_task.send(batch_accumulator).await,
        };

        save_result
            .with_error_context(|err| {
                format!(
                    "Failed to save messages to segment: {}, err: {err}",
                    segment.log_path
                )
            })
            .map_err(|_| IggyError::CannotSaveMessagesToSegment)?;

        Ok(0.into())
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
        .await
        .with_error_context(|_| {
            format!("{COMPONENT} - failed to load batches by max range, segment: {segment}")
        })?;
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
        .await
        .with_error_context(|_| {
            format!("{COMPONENT} - failed to load batches by max range, segment: {segment}")
        })?;
        Ok(())
    }

    async fn load_all_indexes(&self, segment: &Segment) -> Result<Vec<Index>, IggyError> {
        trace!("Loading indexes from file...");
        let file = file::open(&segment.index_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to open index file, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let file_size = file
            .metadata()
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to load index file metadata, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as usize;
        if file_size == 0 {
            trace!("Index file is empty.");
            return Ok(EMPTY_INDEXES);
        }

        let indexes_count = file_size / INDEX_SIZE as usize;
        let mut indexes = Vec::with_capacity(indexes_count);
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        for idx_num in 0..indexes_count {
            let offset = reader
                .read_u32_le()
                .await
                .inspect_err(|error| {
                    error!(
                        "Cannot read offset from index file for index number: {}. Error: {}",
                        idx_num, &error
                    )
                })
                .map_err(|_| IggyError::CannotReadIndexOffset)?;
            let position = reader
                .read_u32_le()
                .await
                .inspect_err(|error| {
                    error!(
                        "Cannot read position from index file for offset: {}. Error: {}",
                        offset, &error
                    )
                })
                .map_err(|_| IggyError::CannotReadIndexPosition)?;
            let timestamp = reader
                .read_u64_le()
                .await
                .inspect_err(|error| {
                    error!(
                        "Cannot read timestamp from index file for offset: {}. Error: {}",
                        offset, &error
                    )
                })
                .map_err(|_| IggyError::CannotReadIndexTimestamp)?;
            indexes.push(Index {
                offset,
                position,
                timestamp,
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

        let file = file::open(&segment.index_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to open segment's index path, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let file_length = file
            .metadata()
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to load index's metadata, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as u32;
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
            let offset = reader
                .read_u32_le()
                .await
                .with_error_context(|_| format!("{COMPONENT} - failed to load index's offset"))
                .map_err(|_| IggyError::CannotReadIndexOffset)?;
            let position = reader
                .read_u32_le()
                .await
                .with_error_context(|_| format!("{COMPONENT} - failed to load index's position"))
                .map_err(|_| IggyError::CannotReadIndexPosition)?;
            let timestamp = reader
                .read_u64_le()
                .await
                .with_error_context(|_| format!("{COMPONENT} - failed to load index's timestamp"))
                .map_err(|_| IggyError::CannotReadIndexTimestamp)?;
            read_bytes += INDEX_SIZE;
            let idx = Index {
                offset,
                position,
                timestamp,
            };
            idx_pred.push(idx);

            if offset >= relative_start_offset {
                index_range.start = idx_pred.tail().unwrap_or_default();
            }
            if offset >= relative_end_offset {
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
        bytes.put_u32_le(index.offset);
        bytes.put_u32_le(index.position);
        bytes.put_u64_le(index.timestamp);
        self.persister
            .append(index_path, &bytes)
            .await
            .map_err(|error| {
                error!(
                    "Failed to save index to segment: {}, error: {}",
                    index_path, error
                );
                IggyError::CannotSaveIndexToSegment
            })
    }

    async fn try_load_index_for_timestamp(
        &self,
        segment: &Segment,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        trace!("Loading time indexes from file...");
        let file = file::open(&segment.index_path)
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to open segment's index file, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let file_size = file
            .metadata()
            .await
            .with_error_context(|_| {
                format!(
                    "{COMPONENT} - failed to load index's metadata, path: {}",
                    segment.index_path
                )
            })
            .map_err(|_| IggyError::CannotReadFileMetadata)?
            .len() as usize;
        if file_size == 0 {
            trace!("Time index file is empty.");
            return Ok(Some(Index::default()));
        }

        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut read_bytes = 0;
        let mut idx_pred = HeadTailBuffer::new();
        loop {
            let offset = reader
                .read_u32_le()
                .await
                .map_err(|_| IggyError::CannotReadIndexOffset)?;
            let position = reader
                .read_u32_le()
                .await
                .map_err(|_| IggyError::CannotReadIndexPosition)?;
            let time = reader
                .read_u64_le()
                .await
                .map_err(|_| IggyError::CannotReadIndexTimestamp)?;
            let idx = Index {
                offset,
                position,
                timestamp: time,
            };
            idx_pred.push(idx);
            if time >= timestamp {
                return Ok(idx_pred.tail());
            }
            read_bytes += INDEX_SIZE as usize;
            if read_bytes == file_size {
                return Ok(None);
            }
        }
    }

    fn persister(&self) -> Arc<PersisterKind> {
        self.persister.clone()
    }
}

async fn load_batches_by_range(
    segment: &Segment,
    index_range: &IndexRange,
    mut on_batch: impl FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let file = file::open(&segment.log_path).await.map_err(|error| {
        error!(
            "Cannot open segment's log file: {}, error: {}",
            segment.log_path, error
        );
        IggyError::CannotReadFile
    })?;
    let file_size = file
        .metadata()
        .await
        .map_err(|error| {
            error!(
                "Cannot read segment's log file metadata: {}, error: {}",
                segment.log_path, error
            );
            IggyError::CannotReadFileMetadata
        })?
        .len();
    if file_size == 0 {
        return Ok(());
    }

    trace!(
        "Loading message batches by index range: {} [{}] - {} [{}], file size: {file_size}",
        index_range.start.position,
        index_range.start.offset,
        index_range.end.position,
        index_range.end.offset
    );

    let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
    reader
        .seek(SeekFrom::Start(index_range.start.position as u64))
        .await
        .with_error_context(|_| {
            format!(
                "{COMPONENT} - failed to seek to position {}",
                index_range.start.position
            )
        })
        .map_err(|_| IggyError::CannotSeekFile)?;

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
        let index_last_offset = index_range.end.offset as u64 + segment.start_offset;

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
            IggyByteSize::from(batch_length as u64),
            payload.freeze(),
        );
        on_batch(batch).with_error_context(|_| format!(
            "{COMPONENT} - failed to process batch with base offset: {}, last offset delta: {}, max timestamp: {}, batch length: {}",
            batch_base_offset, last_offset_delta, max_timestamp, batch_length,
        ))?;
    }
    Ok(())
}

async fn load_messages_by_size(
    segment: &Segment,
    size_bytes: u64,
    mut on_batch: impl FnMut(RetainedMessageBatch) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let file = file::open(&segment.log_path).await.map_err(|error| {
        error!(
            "Cannot open segment's log file: {}, error: {}",
            segment.log_path, error
        );
        IggyError::CannotReadFile
    })?;
    let file_size = file
        .metadata()
        .await
        .map_err(|error| {
            error!(
                "Cannot read segment's log file metadata: {}, error: {}",
                segment.log_path, error
            );
            IggyError::CannotReadFileMetadata
        })?
        .len();
    if file_size == 0 {
        return Ok(());
    }

    let threshold = file_size.saturating_sub(size_bytes);
    let mut accumulated_size = IggyByteSize::default();

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
            IggyByteSize::from(batch_length as u64),
            payload.freeze(),
        );
        let message_size = batch.get_size_bytes();
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
