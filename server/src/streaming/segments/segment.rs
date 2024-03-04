use crate::compat::schemas::message_snapshot::MessageSnapshot;
use crate::compat::{
    binary_schema::BinarySchema, schemas::retained_batch_snapshot::RetainedMessageBatchSnapshot,
};
use crate::configs::system::SystemConfig;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::segments::index::Index;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::storage::SystemStorage;
use crate::streaming::utils::file;
use bytes::{Buf, BufMut as _, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::models::messages::MessageState;
use iggy::utils::timestamp::IggyTimestamp;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};

pub const LOG_EXTENSION: &str = "log";
pub const INDEX_EXTENSION: &str = "index";
pub const TIME_INDEX_EXTENSION: &str = "timeindex";
pub const MAX_SIZE_BYTES: u32 = 1000 * 1000 * 1000;
const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

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

    pub async fn convert_segment_messages_from_schema(
        &self,
        schema: BinarySchema,
    ) -> Result<(), IggyError> {
        let log_path = self.log_path.as_str();
        let index_path = self.index_path.as_str();
        let timeindex_path = self.time_index_path.as_str();
        let segment_alternative_path = format!("{log_path}_temp");
        let index_alternative_path = format!("{index_path}_temp");
        let timeindex_alternative_path = format!("{timeindex_path}_temp");

        match schema {
            BinarySchema::RetainedMessageSchema => {
                let mut messages = Vec::new();
                let file = file::open(&self.log_path).await?;
                let file_size = file.metadata().await?.len();
                if file_size == 0 {
                    return Ok(());
                }

                let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
                while reader.buffer().has_remaining() {
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
                            let mut headers_payload =
                                BytesMut::with_capacity(headers_length as usize);
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

                    let mut payload = BytesMut::with_capacity(payload_length.unwrap() as usize);
                    if reader.read_exact(&mut payload).await.is_err() {
                        return Err(IggyError::CannotReadMessagePayload);
                    }

                    let payload = payload.freeze();
                    let offset = offset.unwrap();
                    let timestamp = timestamp.unwrap();
                    let id = id.unwrap();
                    let checksum = checksum.unwrap();

                    let message = MessageSnapshot::new(
                        offset, state, timestamp, id, payload, checksum, headers,
                    );
                    messages.push(message);
                }
                let message_batches = messages.as_slice().chunks(1000).collect::<Vec<_>>();
                let messages_size: u64 = message_batches
                    .iter()
                    .map(|batch| {
                        let size: u64 = batch.iter().map(|msg| msg.get_size_bytes() as u64).sum();
                        size + 24
                    })
                    .sum();
                let mut batch_bytes = BytesMut::with_capacity(messages_size as usize);
                let mut index_bytes = BytesMut::with_capacity(message_batches.len() * 8);
                let mut timeindex_bytes = BytesMut::with_capacity(message_batches.len() * 12);
                let mut size_in_bytes: u32 = 0;
                for batch in message_batches {
                    let retained_batch = RetainedMessageBatchSnapshot::try_from_messages(batch)?;
                    retained_batch.extend(&mut batch_bytes);

                    let relative_offset = retained_batch.get_last_offset() - self.start_offset;
                    index_bytes.put_u32_le(relative_offset as u32);
                    index_bytes.put_u32_le(size_in_bytes);

                    timeindex_bytes.put_u32_le(relative_offset as u32);
                    timeindex_bytes.put_u64_le(retained_batch.max_timestamp);

                    size_in_bytes += retained_batch.get_size_bytes() as u32;
                }

                // Now we have to persist those in the temp path, check if everything went correctly
                // If so then remove the old 
                //let mut file = file::
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
