use crate::streaming::segments::index::{Index, IndexRange};
use crate::streaming::segments::segment::Segment;
use crate::streaming::segments::time_index::TimeIndex;
use iggy::error::IggyError;
use iggy::models::messages::{Message, RetainedMessage};
use iggy::sizeable::Sizeable;
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

        let first_offset = unsaved_messages[0].get_offset();
        if end_offset < first_offset {
            return self.load_messages_from_disk(offset, end_offset).await;
        }

        let last_offset = unsaved_messages[unsaved_messages.len() - 1].get_offset();
        if end_offset <= last_offset {
            return Ok(self.load_messages_from_unsaved_buffer(offset, end_offset));
        }

        let mut messages = self.load_messages_from_disk(offset, end_offset).await?;
        let mut buffered_messages = self.load_messages_from_unsaved_buffer(offset, end_offset);
        messages.append(&mut buffered_messages);

        Ok(messages)
    }

    pub async fn get_all_messages(&self) -> Result<Vec<Arc<RetainedMessage>>, IggyError> {
        self.get_messages(self.start_offset, self.get_messages_count() as u32)
            .await
    }

    pub async fn get_newest_messages_by_size(
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
        self.unsaved_messages
            .as_ref()
            .unwrap()
            .iter()
            .filter(|message| message.get_offset() >= offset && message.get_offset() <= end_offset)
            .cloned()
            .collect::<Vec<Arc<RetainedMessage>>>()
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
        messages: &[Arc<RetainedMessage>],
    ) -> Result<(), IggyError> {
        if self.is_closed {
            return Err(IggyError::SegmentClosed(
                self.start_offset,
                self.partition_id,
            ));
        }

        let messages_count = messages.len();

        let unsaved_messages = self.unsaved_messages.get_or_insert_with(Vec::new);
        unsaved_messages.reserve(messages_count);

        if let Some(indexes) = &mut self.indexes {
            indexes.reserve(messages_count);
        }

        if let Some(time_indexes) = &mut self.time_indexes {
            time_indexes.reserve(messages_count);
        }

        // Not the prettiest code. It's done this way to avoid repeatably
        // checking if indexes and time_indexes are Some or None.
        let mut messages_size = 0;
        if self.indexes.is_some() && self.time_indexes.is_some() {
            for message in messages {
                let relative_offset = (message.get_offset() - self.start_offset) as u32;

                self.indexes.as_mut().unwrap().push(Index {
                    relative_offset,
                    position: self.size_bytes,
                });

                self.time_indexes.as_mut().unwrap().push(TimeIndex {
                    relative_offset,
                    timestamp: message.get_timestamp(),
                });
                let message_size = message.get_size_bytes();
                self.size_bytes += message_size;
                messages_size += message_size;
                self.current_offset = message.get_offset();
                unsaved_messages.push(message.clone());
            }
        } else if self.indexes.is_some() {
            for message in messages {
                let relative_offset = (message.get_offset() - self.start_offset) as u32;

                self.indexes.as_mut().unwrap().push(Index {
                    relative_offset,
                    position: self.size_bytes,
                });
                let message_size = message.get_size_bytes();
                self.size_bytes += message_size;
                messages_size += message_size;
                self.current_offset = message.get_offset();
                unsaved_messages.push(message.clone());
            }
        } else if self.time_indexes.is_some() {
            for message in messages {
                let relative_offset = (message.get_offset() - self.start_offset) as u32;

                self.time_indexes.as_mut().unwrap().push(TimeIndex {
                    relative_offset,
                    timestamp: message.get_timestamp(),
                });
                let message_size = message.get_size_bytes();
                self.size_bytes += message_size;
                messages_size += message_size;
                self.current_offset = message.get_offset();
                unsaved_messages.push(message.clone());
            }
        } else {
            for message in messages {
                let message_size = message.get_size_bytes();
                self.size_bytes += message_size;
                messages_size += message_size;
                self.current_offset = message.get_offset();
                unsaved_messages.push(message.clone());
            }
        }

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
        let current_position = self.size_bytes - saved_bytes;
        storage
            .save_index(self, current_position, unsaved_messages)
            .await?;
        storage.save_time_index(self, unsaved_messages).await?;

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
