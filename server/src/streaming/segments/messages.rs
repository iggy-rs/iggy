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
use tracing::trace;

const EMPTY_MESSAGES: Vec<Arc<RetainedMessage>> = vec![];

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
            return Ok(EMPTY_MESSAGES);
        }

        if offset < self.start_offset {
            offset = self.start_offset;
        }

        let mut end_offset = offset + (count - 1) as u64;
        if end_offset > self.current_offset {
            end_offset = self.current_offset;
        }

        // In case that the partition messages buffer is disabled, we need to check the unsaved messages buffer
        if self.unsaved_messages.is_none() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        if unsaved_messages.is_empty() {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let first_offset = unsaved_messages[0].base_offset;
        if end_offset < first_offset {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let last_offset = unsaved_messages[unsaved_messages.len() - 1].base_offset;
        if end_offset <= last_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        let mut messages = self.load_messages_from_disk(offset, end_offset).await?;
        let mut buffered_messages = self.load_messages_from_unsaved_buffer(offset, end_offset);
        messages.append(&mut buffered_messages);

        Ok(messages)
    }

    pub async fn get_all_batches(&self) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_newest_message_batches_by_size(
        &self,
        size_bytes: u64,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        let messages = self
            .storage
            .segment
            .load_newest_messages_by_size(self, size_bytes)
            .await?;

        Ok(messages)
    }

    fn load_messages_from_unsaved_buffer(
        &self,
        offset: u64,
        end_offset: u64,
    ) -> Vec<Arc<RetainedMessage>> {
        /*
        self.unsaved_messages
            .as_ref()
            .unwrap()
            .iter()
            .filter(|message| {
                let message_offset = message.base_offset;
                message_offset >= offset && message_offset <= end_offset
            })
            .cloned()
            .collect::<Vec<Arc<RetainedMessage>>>()
            */
        vec![]
    }

    async fn load_messages_from_disk(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        trace!(
            "Loading messages from disk, segment start offset: {}, end offset: {}, current offset: {}...",
            start_offset,
            end_offset,
            self.current_offset
        );

        if start_offset > end_offset || end_offset > self.current_offset {
            trace!(
                "Cannot load messages from disk, invalid offset range: {} - {}.",
                start_offset,
                end_offset
            );
            return Ok(EMPTY_MESSAGES);
        }

        if let Some(indexes) = &self.indexes {
            let relative_start_offset = start_offset - self.start_offset;
            let relative_end_offset = end_offset - self.start_offset;
            let start_index = indexes.get(relative_start_offset as usize);
            let end_index = indexes.get(1 + relative_end_offset as usize);
            if let Some(start_index) = start_index {
                let start_position = start_index.position;
                let end_position = match end_index {
                    Some(index) => index.position,
                    None => self.size_bytes,
                };

                let index_range = IndexRange {
                    start: Index {
                        relative_offset: relative_start_offset as u32,
                        position: start_position,
                    },
                    end: Index {
                        relative_offset: relative_end_offset as u32,
                        position: end_position,
                    },
                };

                return self.load_messages_from_segment_file(&index_range).await;
            }
        }

        let index_range = self
            .storage
            .segment
            .load_index_range(self, self.start_offset, start_offset, end_offset)
            .await?;
        if index_range.is_none() {
            trace!(
                "Cannot load messages from disk, index range not found: {} - {}.",
                start_offset,
                end_offset
            );

            return Ok(EMPTY_MESSAGES);
        }

        self.load_messages_from_segment_file(&index_range.unwrap())
            .await
    }

    async fn load_messages_from_segment_file(
        &self,
        index_range: &IndexRange,
    ) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        let messages = self
            .storage
            .segment
            .load_messages(self, index_range)
            .await?;
        trace!(
            "Loaded {} messages from disk, segment start offset: {}, end offset: {}.",
            messages.len(),
            self.start_offset,
            self.current_offset
        );

        Ok(messages)
    }

    pub async fn append_messages(
        &mut self,
        messages: Arc<RetainedMessageBatch>,
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

        let last_offset = messages.base_offset + messages.last_offset_delta as u64;
        self.current_offset = last_offset;
        self.end_offset = last_offset;

        self.store_offset_and_timestamp_index_for_batch(last_offset, messages.max_timestamp);

        let messages_size = messages.get_size_bytes();
        let messages_count = messages.last_offset_delta + 1;

        let unsaved_messages = self.unsaved_messages.get_or_insert_with(Vec::new);
        unsaved_messages.push(messages);
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
        self.unsaved_indexes.reserve(2);
        self.unsaved_timestamps.reserve(2);

        // Regardless of whether caching of indexes and time_indexes is on
        // store them in the unsaved buffer
        self.unsaved_indexes.put_u32_le(relative_offset);
        self.unsaved_indexes.put_u32_le(self.size_bytes);
        self.unsaved_timestamps.put_u32_le(relative_offset);
        self.unsaved_timestamps.put_u64_le(batch_max_timestamp);
    }

    pub async fn persist_messages(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.segment.clone();
        if self.unsaved_messages.is_none() {
            return Ok(());
        }

        let unsaved_messages = self.unsaved_messages.as_ref().unwrap();
        if unsaved_messages.is_empty() {
            return Ok(());
        }

        trace!(
            "Saving {} messages on disk in segment with start offset: {} for partition with ID: {}...",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id
        );

        let saved_bytes = storage.save_messages(self, unsaved_messages).await?;
        storage.save_index(self).await?;
        self.unsaved_indexes.clear();
        storage.save_time_index(self).await?;
        self.unsaved_timestamps.clear();

        trace!(
            "Saved {} messages on disk in segment with start offset: {} for partition with ID: {}, total bytes written: {}.",
            unsaved_messages.len(),
            self.start_offset,
            self.partition_id,
            saved_bytes
        );

        if self.is_full().await {
            self.end_offset = self.current_offset;
            self.is_closed = true;
            self.unsaved_messages = None;
        } else {
            self.unsaved_messages.as_mut().unwrap().clear();
        }

        Ok(())
    }
}
