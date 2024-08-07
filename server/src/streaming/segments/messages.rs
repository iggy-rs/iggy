use crate::streaming::batching::batch_filter::BatchItemizer;
use crate::streaming::batching::message_batch::RetainedMessageBatch;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use crate::streaming::sizeable::Sizeable;
use bytes::BufMut;
use iggy::error::IggyError;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, trace, warn};

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
    ) -> Result<Vec<RetainedMessage>, IggyError> {
        if count == 0 {
            return Ok(EMPTY_MESSAGES);
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let end_offset = offset + (count - 1) as u64;
        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_batches.is_none() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let unsaved_batches = self.unsaved_batches.as_ref().unwrap();
        if unsaved_batches.is_empty() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let first_offset = unsaved_batches[0].base_offset;
        if end_offset < first_offset {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let last_offset = unsaved_batches[unsaved_batches.len() - 1].get_last_offset();
        if offset >= first_offset && end_offset <= last_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        // Can this be somehow improved? maybe with chain iterators
        let mut messages = self.load_messages_from_disk(offset, end_offset).await?;
        let mut buffered_messages = self.load_messages_from_unsaved_buffer(offset, last_offset);
        messages.append(&mut buffered_messages);

        Ok(messages)
    }

    pub async fn get_all_messages(&self) -> Result<Vec<RetainedMessage>, IggyError> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_all_batches(&self) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        self.storage
            .segment
            .load_message_batches(self, &IndexRange::max_range())
            .await
    }

    pub async fn get_newest_batches_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<RetainedMessageBatch>, IggyError> {
        let messages = self
            .storage
            .segment
            .load_newest_batches_by_size(self, size_bytes)
            .await?;

        Ok(messages)
    }

    fn load_messages_from_unsaved_buffer(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Vec<RetainedMessage> {
        let unsaved_batches = self.unsaved_batches.as_ref().unwrap();
        let slice_start = unsaved_batches
            .iter()
            .rposition(|batch| batch.base_offset <= start_offset)
            .unwrap_or(0);

        // Take only the batch when last_offset >= relative_end_offset and it's base_offset is <= relative_end_offset
        // otherwise take batches until the last_offset >= relative_end_offset and base_offset <= relative_start_offset
        let messages_count = (start_offset + end_offset) as usize;
        unsaved_batches[slice_start..]
            .iter()
            .filter(|batch| {
                batch.is_contained_or_overlapping_within_offset_range(start_offset, end_offset)
            })
            .to_messages_with_filter(messages_count, &|msg| {
                msg.offset >= start_offset && msg.offset <= end_offset
            })
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<RetainedMessage>, IggyError> {
        trace!(
            "Loading messages from disk, segment start offset: {}, end offset: {}, current offset: {}...",
            start_offset,
            end_offset,
            self.current_offset
        );

        if start_offset > end_offset {
            warn!(
                "Cannot load messages from disk, invalid offset range: {} - {}.",
                start_offset, end_offset
            );
            return Ok(EMPTY_MESSAGES);
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
                    return Ok(EMPTY_MESSAGES);
                }
            };

            return self
                .load_messages_from_segment_file(&index_range, start_offset, end_offset)
                .await;
        }

        match self
            .storage
            .segment
            .load_index_range(self, start_offset, end_offset)
            .await?
        {
            Some(index_range) => {
                self.load_messages_from_segment_file(&index_range, start_offset, end_offset)
                    .await
            }
            None => Ok(EMPTY_MESSAGES),
        }
    }

    async fn load_messages_from_segment_file(
        &self,
        index_range: &IndexRange,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<RetainedMessage>, IggyError> {
        let messages_count = (start_offset + end_offset) as usize;
        let messages = self
            .storage
            .segment
            .load_message_batches(self, index_range)
            .await?
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

        Ok(messages)
    }

    pub async fn append_batch(
        &mut self,
        batch: Arc<RetainedMessageBatch>,
    ) -> Result<(), IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }

        if let Some(indexes) = &mut self.indexes {
            indexes.reserve(1);
        }

        if let Some(time_indexes) = &mut self.time_indexes {
            time_indexes.reserve(1);
        }

        let last_offset = batch.base_offset + batch.last_offset_delta as u64;
        self.current_offset = last_offset;
        self.end_offset = last_offset;

        self.store_offset_and_timestamp_index_for_batch(last_offset, batch.max_timestamp);

        let messages_size = batch.get_size_bytes();
        let messages_count = batch.last_offset_delta + 1;

        let unsaved_batches = self.unsaved_batches.get_or_insert_with(Vec::new);
        unsaved_batches.push(batch);
        self.size_bytes += messages_size;

        self.size_of_parent_stream
            .fetch_add(messages_size as u64, Ordering::SeqCst);
        self.size_of_parent_topic
            .fetch_add(messages_size as u64, Ordering::SeqCst);
        self.size_of_parent_partition
            .fetch_add(messages_size as u64, Ordering::SeqCst);
        self.messages_count_of_parent_stream
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_topic
            .fetch_add(messages_count as u64, Ordering::SeqCst);
        self.messages_count_of_parent_partition
            .fetch_add(messages_count as u64, Ordering::SeqCst);

        Ok(())
    }
    fn store_offset_and_timestamp_index_for_batch(
        &mut self,
        batch_last_offset: u64,
        batch_max_timestamp: u64,
    ) {
        let relative_offset = (batch_last_offset - self.start_offset) as u32;
        match (&mut self.indexes, &mut self.time_indexes) {
            (Some(indexes), Some(time_indexes)) => {
                indexes.push(Index {
                    relative_offset,
                    position: self.size_bytes,
                });
                time_indexes.push(TimeIndex {
                    relative_offset,
                    timestamp: batch_max_timestamp,
                });
            }
            (Some(indexes), None) => {
                indexes.push(Index {
                    relative_offset,
                    position: self.size_bytes,
                });
            }
            (None, Some(time_indexes)) => {
                time_indexes.push(TimeIndex {
                    relative_offset,
                    timestamp: batch_max_timestamp,
                });
            }
            (None, None) => {}
        };

        // Regardless of whether caching of indexes and time_indexes is on
        // store them in the unsaved buffer
        self.unsaved_indexes.put_u32_le(relative_offset);
        self.unsaved_indexes.put_u32_le(self.size_bytes);
        self.unsaved_timestamps.put_u32_le(relative_offset);
        self.unsaved_timestamps.put_u64_le(batch_max_timestamp);
    }

    pub async fn persist_messages(&mut self) -> Result<usize, IggyError> {
        let storage = self.storage.segment.clone();
        if self.unsaved_batches.is_none() {
            return Ok(0);
        }

        let unsaved_batches = self.unsaved_batches.as_ref().unwrap();
        if unsaved_batches.is_empty() {
            return Ok(0);
        }

        let unsaved_messages_number = (unsaved_batches.last().unwrap().get_last_offset() + 1)
            - unsaved_batches.first().unwrap().base_offset;

        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id
        );

        let saved_bytes = storage.save_batches(self, unsaved_batches).await?;
        storage.save_index(self).await?;
        self.unsaved_indexes.clear();
        storage.save_time_index(self).await?;
        self.unsaved_timestamps.clear();

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages_number,
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_batches = None;
            info!(
                "Closed segment with start offset: {} for partition with ID: {}.",
                self.start_offset, self.partition_id
            );
        } else {
            self.unsaved_batches.as_mut().unwrap().clear();
        }

        Ok(unsaved_messages_number as usize)
    }
}
