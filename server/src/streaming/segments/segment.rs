use crate::compat::binary_schema::BinarySchema;
use crate::compat::chunks_error::IntoTryChunksError;
use crate::compat::message_converter::MessageFormatConverterPersister;
use crate::compat::message_stream::MessageStream;
use crate::compat::snapshots::retained_batch_snapshot::RetainedMessageBatchSnapshot;
use crate::compat::streams::retained_message::RetainedMessageStream;
use crate::configs::system::SystemConfig;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::segments::index::Index;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::sizeable::Sizeable;
use crate::streaming::storage::SystemStorage;
use crate::streaming::utils::file;
use futures::{pin_mut, TryStreamExt};
use iggy::error::IggyError;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tracing::{error, info, trace};

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";
pub const MAX_SIZE_BYTES: u32 = 1000 * 1000 * 1000;
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;
const BUF_WRITER_CAPACITY_BYTES: usize = 512 * 1000;

#[derive(Debug)]
pub struct Segment {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub current_offset: u64,
    pub index_path: String,
    pub log_path: String,
    pub time_index_path: String,
    pub size_bytes: u32,
    pub size_of_parent_stream: Arc<AtomicU64>,
    pub size_of_parent_topic: Arc<AtomicU64>,
    pub size_of_parent_partition: Arc<AtomicU64>,
    pub messages_count_of_parent_stream: Arc<AtomicU64>,
    pub messages_count_of_parent_topic: Arc<AtomicU64>,
    pub messages_count_of_parent_partition: Arc<AtomicU64>,
    pub is_closed: bool,
    pub(crate) message_expiry: Option<u32>,
    pub(crate) unsaved_batches: Option<Vec<Arc<RetainedMessageBatch>>>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) indexes: Option<Vec<Index>>,
    pub(crate) time_indexes: Option<Vec<TimeIndex>>,
    pub(crate) unsaved_indexes: Vec<u8>,
    pub(crate) unsaved_timestamps: Vec<u8>,
    pub(crate) storage: Arc<SystemStorage>,
}

impl Segment {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        start_offset: u64,
        config: Arc<SystemConfig>,
        storage: Arc<SystemStorage>,
        message_expiry: Option<u32>,
        size_of_parent_stream: Arc<AtomicU64>,
        size_of_parent_topic: Arc<AtomicU64>,
        size_of_parent_partition: Arc<AtomicU64>,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_topic: Arc<AtomicU64>,
        messages_count_of_parent_partition: Arc<AtomicU64>,
    ) -> Segment {
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);

        Segment {
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            end_offset: 0,
            current_offset: start_offset,
            log_path: Self::get_log_path(&path),
            index_path: Self::get_index_path(&path),
            time_index_path: Self::get_time_index_path(&path),
            size_bytes: 0,
            message_expiry,
            indexes: match config.segment.cache_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            time_indexes: match config.segment.cache_time_indexes {
                true => Some(Vec::new()),
                false => None,
            },
            unsaved_indexes: Vec::new(),
            unsaved_timestamps: Vec::new(),
            unsaved_batches: None,
            is_closed: false,
            size_of_parent_stream,
            size_of_parent_partition,
            size_of_parent_topic,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
            config,
            storage,
        }
    }

    pub async fn is_full(&self) -> bool {
        if self.size_bytes >= self.config.segment.size.as_bytes_u64() as u32 {
            return true;
        }

        self.is_expired(IggyTimestamp::now().to_micros()).await
    }

    pub async fn is_expired(&self, now: u64) -> bool {
        if self.message_expiry.is_none() {
            return false;
        }

        let last_messages = self.get_messages(self.end_offset, 1).await;
        if last_messages.is_err() {
            return false;
        }

        let last_messages = last_messages.unwrap();
        if last_messages.is_empty() {
            return false;
        }

        let last_message = &last_messages[0];
        let message_expiry = (self.message_expiry.unwrap() * 1000) as u64;
        (last_message.timestamp + message_expiry) <= now
    }

    fn get_log_path(path: &str) -> String {
        format!("{}.{}", path, LOG_EXTENSION)
    }

    fn get_index_path(path: &str) -> String {
        format!("{}.{}", path, INDEX_EXTENSION)
    }

    fn get_time_index_path(path: &str) -> String {
        format!("{}.{}", path, TIME_INDEX_EXTENSION)
    }

    pub async fn convert_segment_from_schema(&self, schema: BinarySchema) -> Result<(), IggyError> {
        let log_path = self.log_path.as_str();
        let index_path = self.index_path.as_str();
        let time_index_path = self.time_index_path.as_str();
        let log_alt_path = format!("{}_temp.{}", log_path.split('.').next().unwrap(), "log");
        let index_alt_path = format!("{}_temp.{}", index_path.split('.').next().unwrap(), "index");
        let time_index_alt_path = format!(
            "{}_temp.{}",
            time_index_path.split('.').next().unwrap(),
            "timeindex"
        );

        match schema {
            BinarySchema::RetainedMessageSchema => {
                let file = file::open(&self.log_path).await?;
                let file_size = file.metadata().await?.len();
                if file_size == 0 {
                    return Ok(());
                }

                trace!(
                    "Creating temporary files for conversion, log: {}, index: {}, time_index: {}",
                    log_alt_path,
                    index_alt_path,
                    time_index_alt_path
                );
                //TODO (numinex) - this whole operation should be done in a single transaction
                tokio::fs::File::create(&log_alt_path).await?;
                tokio::fs::File::create(&index_alt_path).await?;
                tokio::fs::File::create(&time_index_alt_path).await?;
                let batch_writer = BufWriter::with_capacity(
                    BUF_WRITER_CAPACITY_BYTES,
                    file::append(&log_alt_path).await?,
                );
                let index_writer = BufWriter::with_capacity(
                    BUF_WRITER_CAPACITY_BYTES,
                    file::append(&index_alt_path).await?,
                );
                let time_index_writer = BufWriter::with_capacity(
                    BUF_WRITER_CAPACITY_BYTES,
                    file::append(&time_index_alt_path).await?,
                );
                let reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);

                let stream = RetainedMessageStream::new(reader, file_size).into_stream();
                pin_mut!(stream);
                let (_, mut batch_writer, mut index_writer, mut time_index_writer) = stream
                        .try_chunks(1000)
                        .try_fold((0u32, batch_writer, index_writer, time_index_writer), |(position, mut batch_writer, mut index_writer, mut time_index_writer), messages| async move {
                            let batch = RetainedMessageBatchSnapshot::try_from_messages(messages)
                                .map_err(|err| err.into_try_chunks_error())?;
                            let size = batch.get_size_bytes();
                            info!("Converted messages with start offset: {} and end offset: {}, with binary schema: {} to newest schema",
                            batch.base_offset, batch.get_last_offset(), schema);

                            batch
                                .persist(&mut batch_writer)
                                .await
                                .map_err(|err| err.into_try_chunks_error())?;
                            trace!("Persisted message batch with new format to log file, saved {} bytes", size);
                            let relative_offset = (batch.get_last_offset() - self.start_offset) as u32;
                            batch
                                .persist_index(position, relative_offset, &mut index_writer)
                                .await
                                .map_err(|err| err.into_try_chunks_error())?;
                            trace!("Persisted index with offset: {} and position: {} to index file", relative_offset, position);
                            batch
                                .persist_time_index(batch.max_timestamp, relative_offset, &mut time_index_writer)
                                .await
                                .map_err(|err| err.into_try_chunks_error())?;
                            trace!("Persisted time index with offset: {} to time index file", relative_offset);
                            let position = position + size;

                            Ok((position, batch_writer, index_writer, time_index_writer))
                        })
                        .await
                        .map_err(|err| err.1)?; // For now

                batch_writer.flush().await?;
                index_writer.flush().await?;
                time_index_writer.flush().await?;

                // Remove old files and rename the temp to original name
                file::remove(&log_path).await?;
                file::remove(&index_path).await?;
                file::remove(&time_index_path).await?;
                file::rename(&log_alt_path, &log_path).await?;
                file::rename(&index_alt_path, &index_path).await?;
                file::rename(&time_index_alt_path, &time_index_path).await?;
                Ok(())
            }
            BinarySchema::RetainedMessageBatchSchema => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SegmentConfig;
    use crate::streaming::storage::tests::get_test_system_storage;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig::default());
        let path = config.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        let log_path = Segment::get_log_path(&path);
        let index_path = Segment::get_index_path(&path);
        let time_index_path = Segment::get_time_index_path(&path);
        let message_expiry = Some(10);
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            storage,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );

        assert_eq!(segment.stream_id, stream_id);
        assert_eq!(segment.topic_id, topic_id);
        assert_eq!(segment.partition_id, partition_id);
        assert_eq!(segment.start_offset, start_offset);
        assert_eq!(segment.current_offset, 0);
        assert_eq!(segment.end_offset, 0);
        assert_eq!(segment.size_bytes, 0);
        assert_eq!(segment.log_path, log_path);
        assert_eq!(segment.index_path, index_path);
        assert_eq!(segment.time_index_path, time_index_path);
        assert_eq!(segment.message_expiry, message_expiry);
        assert!(segment.unsaved_batches.is_none());
        assert!(segment.indexes.is_some());
        assert!(segment.time_indexes.is_some());
        assert!(!segment.is_closed);
        assert!(!segment.is_full().await);
    }

    #[test]
    fn should_not_initialize_indexes_cache_when_disabled() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_indexes: false,
                ..Default::default()
            },
            ..Default::default()
        });
        let message_expiry = None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            storage,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );

        assert!(segment.indexes.is_none());
    }

    #[test]
    fn should_not_initialize_time_indexes_cache_when_disabled() {
        let storage = Arc::new(get_test_system_storage());
        let stream_id = 1;
        let topic_id = 2;
        let partition_id = 3;
        let start_offset = 0;
        let config = Arc::new(SystemConfig {
            segment: SegmentConfig {
                cache_time_indexes: false,
                ..Default::default()
            },
            ..Default::default()
        });
        let message_expiry = None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let size_of_parent_topic = Arc::new(AtomicU64::new(0));
        let size_of_parent_partition = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_topic = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_partition = Arc::new(AtomicU64::new(0));

        let segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            config,
            storage,
            message_expiry,
            size_of_parent_stream,
            size_of_parent_topic,
            size_of_parent_partition,
            messages_count_of_parent_stream,
            messages_count_of_parent_topic,
            messages_count_of_parent_partition,
        );
        assert!(segment.time_indexes.is_none());
    }
}
