use super::indexes::*;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::batching::{batch_filter::BatchItemizer, iterator::IntoMessagesIterator};
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::segments::segment::Segment;
use error_set::ErrContext;
use iggy::{
    error::IggyError,
    utils::{byte_size::IggyByteSize, checksum, sizeable::Sizeable},
};
use std::sync::Arc;
use tracing::{trace, warn};

const EMPTY_MESSAGES: Vec<RetainedMessage> = vec![];

impl Segment {
    pub fn get_messages_count(&self) -> u64 {
        if self.size_bytes == 0 {
            return 0;
        }

        self.current_offset - self.start_offset + 1
    }

    pub async fn get_messages(
        &self,
        mut offset: u64,
        count: u32,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        if count == 0 {
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let end_offset = offset + (count - 1) as u64;

        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_messages.is_none() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let batch_accumulator = self.unsaved_messages.as_ref().unwrap();
        if batch_accumulator.is_empty() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let first_buffer_offset = batch_accumulator.batch_base_offset();
        let last_buffer_offset = batch_accumulator.batch_max_offset();

        // Case 1: All messages are in messages_require_to_save buffer
        if offset >= first_buffer_offset && end_offset <= last_buffer_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        // Case 2: All messages are on disk
        if end_offset < first_buffer_offset {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        // Case 3: Messages span disk and messages_require_to_save buffer boundary
        let mut messages = Vec::new();

        // Load messages from disk up to the messages_require_to_save buffer boundary
        if offset < first_buffer_offset {
            let disk_messages = self
                .load_messages_from_disk(offset, first_buffer_offset - 1)
                .await.with_error_context(|e| format!(
            "STREAMING_SEGMENT - failed to load messages from disk, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {}, end offset :{}, error: {}",
            self.stream_id, self.topic_id, self.partition_id, offset, first_buffer_offset - 1, e
        ))?;
            messages.extend(disk_messages);
        }

        // Load remaining messages from messages_require_to_save buffer
        let buffer_start = std::cmp::max(offset, first_buffer_offset);
        let buffer_messages = self.load_messages_from_unsaved_buffer(buffer_start, end_offset);
        messages.extend(buffer_messages);

        Ok(messages)
    }

    pub async fn get_all_messages(&self) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_all_batches(&self) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        self.load_batches_by_range(&IndexRange::max_range()).await
    }

    pub async fn get_newest_batches_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let mut batches = Vec::new();
        let mut total_size_bytes = IggyByteSize::default();
        self.log_reader
            .as_ref()
            .unwrap()
            .load_batches_by_size_with_callback(size_bytes, |batch| {
                total_size_bytes += batch.get_size_bytes();
                batches.push(batch);
                Ok(())
            })
            .await
            .with_error_context(|e| {
                format!(
                    "Failed to load messages by size ({size_bytes} bytes) with callback, error: {e} for {}",
                    self
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

    fn load_messages_from_unsaved_buffer(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        let batch_accumulator = self.unsaved_messages.as_ref().unwrap();
        batch_accumulator.get_messages_by_offset(start_offset, end_offset)
    }

    /// Load message batches given an index range.
    pub async fn load_batches_by_range(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        trace!("Loading message batches for index range: {:?}", index_range);

        let batches = self
            .log_reader
            .as_ref()
            .unwrap()
            .load_batches_by_range_impl(index_range)
            .await
            .with_error_context(|e| {
                format!(
                    "Failed to load message batches by range {:?} from disk, error: {e} for {}",
                    index_range, self
                )
            })?;

        trace!("Loaded {} message batches.", batches.len());
        Ok(batches)
    }

    pub async fn load_index_for_timestamp(
        &self,
        timestamp: u64,
    ) -> Result<Option<Index>, IggyError> {
        trace!("Loading index for timestamp: {}", timestamp);
        let index = self
            .index_reader
            .as_ref()
            .unwrap()
            .load_index_for_timestamp_impl(timestamp)
            .await
            .with_error_context(|e| {
                format!(
                    "Failed to load index for timestamp: {timestamp}, error: {e} for {}",
                    self
                )
            })?;

        trace!("Loaded index: {:?}", index);
        Ok(index)
    }

    /// Loads and verifies message checksums from the log file.
    pub async fn load_message_checksums(&self) -> Result<(), IggyError> {
        self.log_reader
            .as_ref()
            .unwrap()
            .load_batches_by_range_with_callback(&IndexRange::max_range(), |batch| {
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
            .with_error_context(|e| {
                format!(
                    "Failed to load batches by max range, error: {e} for {}",
                    self
                )
            })?;
        Ok(())
    }

    /// Loads and returns all message IDs from the log file.
    pub async fn load_message_ids(&self) -> Result<Vec<u128>, IggyError> {
        trace!("Loading message IDs from log file: {}", self.log_path);
        let ids = self
            .log_reader
            .as_ref()
            .unwrap()
            .load_message_ids_impl()
            .await
            .with_error_context(|e| {
                format!("Failed to load message ids, error: {} for {}", e, self)
            })?;
        trace!("Loaded {} message IDs from log file.", ids.len());
        Ok(ids)
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Loading messages from disk, start offset: {}, end offset: {}, current offset: {}...",
            start_offset,
            end_offset,
            self.current_offset
        );

        if start_offset > end_offset {
            warn!(
                "Cannot load messages from disk, invalid offset range: {} - {}.",
                start_offset, end_offset
            );
            return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
        }

        if let Some(indices) = &self.indexes {
            let relative_start_offset = (start_offset - self.start_offset) as u32;
            let relative_end_offset = (end_offset - self.start_offset) as u32;
            let index_range = match self.load_highest_lower_bound_index(
                indices,
                relative_start_offset,
                relative_end_offset,
            ) {
                Ok(range) => range,
                Err(_) => {
                    trace!(
                        "Cannot load messages from disk, index range not found: {} - {}.",
                        start_offset,
                        end_offset
                    );
                    return Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect());
                }
            };

            return self
                .load_messages_from_segment_file(&index_range, start_offset, end_offset)
                .await;
        }

        match self
            .index_reader
            .as_ref()
            .unwrap()
            .load_index_range_impl(start_offset, end_offset, self.start_offset)
            .await
            .with_error_context(|e| {
                format!("Failed to load index range start offset: {start_offset}, end offset: {end_offset}, error: {e} for {}", self)
            })? {
            Some(index_range) => {
                self.load_messages_from_segment_file(&index_range, start_offset, end_offset)
                    .await
            }
            None => Ok(EMPTY_MESSAGES.into_iter().map(Arc::new).collect()),
        }
    }

    async fn load_messages_from_segment_file(
        &self,
        index_range: &IndexRange,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Loading messages from disk, index range: {:?}, start offset: {}, end offset: {}.",
            index_range,
            start_offset,
            end_offset
        );
        let messages_count = (start_offset + end_offset + 1) as usize;
        let messages = self
            .load_batches_by_range(index_range)
            .await
            .with_error_context(|_| format!(
                "STREAMING_SEGMENT - failed to load message batches, stream ID: {}, topic ID: {}, partition ID: {}, start offset: {}, end offset: {}",
                self.stream_id, self.topic_id, self.partition_id, start_offset, end_offset,
            ))?
            .iter()
            .to_messages_with_filter(messages_count, &|msg| {
                msg.offset >= start_offset && msg.offset <= end_offset
            });

        trace!(
            "Loaded {} messages from disk, segment start offset: {}, end offset: {}.",
            messages.len(),
            self.start_offset,
            self.current_offset
        );

        Ok(messages.into_iter().map(Arc::new).collect())
    }
}
